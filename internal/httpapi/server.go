package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/Chiru9030/high-throughput-log-pipeline/internal/model"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/pipeline"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/storage"
)

const (
	maxSinglePayloadBytes = 256 * 1024
	maxBatchPayloadBytes  = 4 * 1024 * 1024
)

type Server struct {
	pipe   *pipeline.Pipeline
	store  storage.Store
	apiKey string
	mux    *http.ServeMux
}

func NewServer(pipe *pipeline.Pipeline, store storage.Store) *Server {
	return NewServerWithAuth(pipe, store, "")
}

func NewServerWithAuth(pipe *pipeline.Pipeline, store storage.Store, apiKey string) *Server {
	s := &Server{
		pipe:   pipe,
		store:  store,
		apiKey: apiKey,
		mux:    http.NewServeMux(),
	}
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	s.mux.HandleFunc("POST /v1/ingest", s.handleIngestSingle)
	s.mux.HandleFunc("POST /v1/ingest/batch", s.handleIngestBatch)
	s.mux.HandleFunc("GET /v1/logs", s.handleQueryLogs)
	s.mux.HandleFunc("GET /v1/anomalies", s.handleQueryAnomalies)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.apiKey != "" && r.URL.Path != "/health" {
		auth := r.Header.Get("Authorization")
		key := r.Header.Get("X-API-Key")
		if key != s.apiKey && auth != "Bearer "+s.apiKey {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	stored, anomalies := s.store.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"pipeline":        s.pipe.Stats(),
		"stored_events":   stored,
		"stored_anomalies": anomalies,
	})
}

func (s *Server) handleIngestSingle(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxSinglePayloadBytes)
	var ev model.LogEvent
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "payload exceeds 256 KiB limit"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return
	}
	if ev.Service == "" || ev.Message == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "service and message are required"})
		return
	}
	if ev.Level == "" {
		ev.Level = model.LevelInfo
	}

	if err := s.pipe.Enqueue(ev); err != nil {
		if errors.Is(err, pipeline.ErrQueueFull) {
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func (s *Server) handleIngestBatch(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBatchPayloadBytes)
	var events []model.LogEvent
	if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "batch payload exceeds 4 MiB limit"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON array"})
		return
	}

	accepted := 0
	dropped := 0
	for _, ev := range events {
		if ev.Service == "" || ev.Message == "" {
			continue
		}
		if ev.Level == "" {
			ev.Level = model.LevelInfo
		}
		if err := s.pipe.Enqueue(ev); err != nil {
			dropped++
		} else {
			accepted++
		}
	}

	status := http.StatusAccepted
	if dropped > 0 && accepted == 0 {
		w.Header().Set("Retry-After", "1")
		status = http.StatusTooManyRequests
	}
	writeJSON(w, status, map[string]int{"accepted": accepted, "dropped": dropped})
}

func (s *Server) handleQueryLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	filter := model.QueryFilter{
		Service:     q.Get("service"),
		Level:       model.LogLevel(q.Get("level")),
		OnlyAnomaly: q.Get("anomaly") == "true",
		Limit:       limit,
	}
	results := s.store.Query(filter)
	writeJSON(w, http.StatusOK, map[string]any{"count": len(results), "logs": results})
}

func (s *Server) handleQueryAnomalies(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	results := s.store.Query(model.QueryFilter{
		Service:     r.URL.Query().Get("service"),
		OnlyAnomaly: true,
		Limit:       limit,
	})
	writeJSON(w, http.StatusOK, map[string]any{"count": len(results), "anomalies": results})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

