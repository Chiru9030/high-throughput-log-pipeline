package httpapi

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/Chiru9030/high-throughput-log-pipeline/internal/model"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/parser"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/pipeline"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/storage"
)

const (
	maxSinglePayloadBytes = 256 * 1024
	maxBatchPayloadBytes  = 4 * 1024 * 1024
	maxQueryLimit         = 1000
)

//go:embed ui/dashboard.html
var dashboardHTML []byte

//go:embed ui/app.js
var dashboardJS []byte

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
	s.mux.HandleFunc("GET /", s.handleDashboard)
	s.mux.HandleFunc("GET /dashboard", s.handleDashboard)
	s.mux.HandleFunc("GET /dashboard/app.js", s.handleDashboardJS)
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /readyz", s.handleReady)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /metrics/prometheus", s.handlePrometheusMetrics)
	s.mux.HandleFunc("GET /v1/metrics/latency", s.handleMetricsLatency)
	s.mux.HandleFunc("POST /v1/ingest", s.handleIngestSingle)
	s.mux.HandleFunc("POST /v1/ingest/batch", s.handleIngestBatch)
	s.mux.HandleFunc("GET /v1/logs", s.handleQueryLogs)
	s.mux.HandleFunc("GET /v1/anomalies", s.handleQueryAnomalies)
	return s
}

func constantTimeMatches(provided, expected string) bool {
	if provided == "" || expected == "" {
		return false
	}
	pHash := sha256.Sum256([]byte(provided))
	eHash := sha256.Sum256([]byte(expected))
	return subtle.ConstantTimeCompare(pHash[:], eHash[:]) == 1
}

func extractBearer(header string) string {
	header = strings.TrimSpace(header)
	if len(header) > 7 && strings.EqualFold(header[:7], "bearer ") {
		return strings.TrimSpace(header[7:])
	}
	return ""
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Referrer-Policy", "no-referrer")

	isPublicRoute := r.URL.Path == "/" ||
		r.URL.Path == "/dashboard" ||
		r.URL.Path == "/dashboard/app.js" ||
		r.URL.Path == "/health" ||
		r.URL.Path == "/healthz" ||
		r.URL.Path == "/readyz"

	if s.apiKey != "" && !isPublicRoute {
		bearer := extractBearer(r.Header.Get("Authorization"))
		key := strings.TrimSpace(r.Header.Get("X-API-Key"))
		if !constantTimeMatches(key, s.apiKey) && !constantTimeMatches(bearer, s.apiKey) {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
	}
	s.mux.ServeHTTP(w, r)
}

func normalizeAndValidateLevel(lvl model.LogLevel) (model.LogLevel, bool) {
	norm := model.LogLevel(strings.ToUpper(strings.TrimSpace(string(lvl))))
	if norm == "" {
		return model.LevelInfo, true
	}
	switch norm {
	case model.LevelDebug, model.LevelInfo, model.LevelWarn, model.LevelError, model.LevelFatal:
		return norm, true
	default:
		return "", false
	}
}

func sanitizeAndValidateInbound(ev *model.LogEvent) string {
	// Clear server-owned anomaly/identity fields so callers cannot spoof flags.
	ev.ID = ""
	ev.IsAnomaly = false
	ev.AnomalyReason = ""

	ev.Service = strings.TrimSpace(ev.Service)
	ev.Message = strings.TrimSpace(ev.Message)
	if ev.Service == "" || ev.Message == "" {
		return "service and message are required"
	}
	if math.IsNaN(ev.LatencyMs) || math.IsInf(ev.LatencyMs, 0) || ev.LatencyMs < 0 {
		return "latency_ms must be finite and non-negative"
	}
	if ev.StatusCode < 0 || ev.StatusCode > 599 {
		return "status_code must be between 0 and 599"
	}
	lvl, ok := normalizeAndValidateLevel(ev.Level)
	if !ok {
		return "level must be DEBUG, INFO, WARN, ERROR, or FATAL"
	}
	ev.Level = lvl
	return ""
}

func decodeStrictJSON(body []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("unexpected trailing data after JSON payload")
	}
	return nil
}

func (s *Server) handleDashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' https://code.jquery.com; style-src 'self' 'unsafe-inline'; connect-src 'self'")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(dashboardHTML)
}

func (s *Server) handleDashboardJS(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(dashboardJS)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	stored, anomalies := s.store.Stats()
	stats := s.pipe.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":           "ready",
		"queue_depth":      stats.QueueDepth,
		"stored_events":    stored,
		"stored_anomalies": anomalies,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	stored, anomalies := s.store.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"pipeline":         s.pipe.Stats(),
		"stored_events":    stored,
		"stored_anomalies": anomalies,
	})
}

func (s *Server) handlePrometheusMetrics(w http.ResponseWriter, _ *http.Request) {
	st := s.pipe.Stats()
	stored, storedAnom := s.store.Stats()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(
		w,
		"# TYPE log_pipeline_enqueued_total counter\nlog_pipeline_enqueued_total %d\n"+
			"# TYPE log_pipeline_dropped_total counter\nlog_pipeline_dropped_total %d\n"+
			"# TYPE log_pipeline_processed_total counter\nlog_pipeline_processed_total %d\n"+
			"# TYPE log_pipeline_anomalies_total counter\nlog_pipeline_anomalies_total %d\n"+
			"# TYPE log_pipeline_persist_errors_total counter\nlog_pipeline_persist_errors_total %d\n"+
			"# TYPE log_pipeline_queue_depth gauge\nlog_pipeline_queue_depth %d\n"+
			"# TYPE log_pipeline_stored_events gauge\nlog_pipeline_stored_events %d\n"+
			"# TYPE log_pipeline_stored_anomalies gauge\nlog_pipeline_stored_anomalies %d\n",
		st.Enqueued, st.Dropped, st.Processed, st.Anomalies, st.PersistErrors, st.QueueDepth, stored, storedAnom,
	)
}

func parseTextLine(raw string) (model.LogEvent, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return model.LogEvent{}, false
	}
	if strings.HasPrefix(trimmed, "<") {
		return parser.ParseSyslogRFC5424(trimmed)
	}
	return parser.ParseLogfmt(trimmed)
}

func (s *Server) handleIngestSingle(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxSinglePayloadBytes)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "payload exceeds 256 KiB limit"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unable to read body"})
		return
	}

	contentType := strings.ToLower(r.Header.Get("Content-Type"))
	if strings.Contains(contentType, "text/plain") {
		lines := strings.Split(strings.ReplaceAll(string(bodyBytes), "\r\n", "\n"), "\n")
		var parsed []model.LogEvent
		for _, line := range lines {
			if strings.TrimSpace(line) == "" {
				continue
			}
			ev, ok := parseTextLine(line)
			if !ok {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid syslog or logfmt format"})
				return
			}
			if valErr := sanitizeAndValidateInbound(&ev); valErr != "" {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": valErr})
				return
			}
			parsed = append(parsed, ev)
		}
		if len(parsed) == 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "empty text/plain payload"})
			return
		}
		for _, ev := range parsed {
			if err := s.pipe.Enqueue(ev); err != nil {
				if errors.Is(err, pipeline.ErrQueueFull) {
					w.Header().Set("Retry-After", "1")
					writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": err.Error()})
					return
				}
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
				return
			}
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"status": "accepted", "count": len(parsed)})
		return
	}

	var ev model.LogEvent
	if err := decodeStrictJSON(bodyBytes, &ev); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return
	}
	if valErr := sanitizeAndValidateInbound(&ev); valErr != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": valErr})
		return
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
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "batch payload exceeds 4 MiB limit"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unable to read body"})
		return
	}
	var events []model.LogEvent
	if err := decodeStrictJSON(bodyBytes, &events); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON array"})
		return
	}
	if len(events) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "batch array must contain at least one event"})
		return
	}

	accepted := 0
	dropped := 0
	failed := 0
	rejected := 0
	for i := range events {
		ev := events[i]
		if sanitizeAndValidateInbound(&ev) != "" {
			rejected++
			continue
		}
		if err := s.pipe.Enqueue(ev); err != nil {
			if errors.Is(err, pipeline.ErrQueueFull) {
				dropped++
			} else {
				failed++
			}
		} else {
			accepted++
		}
	}

	if accepted == 0 && dropped == 0 && failed == 0 && rejected > 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "no valid events in batch", "rejected": rejected})
		return
	}

	status := http.StatusAccepted
	if accepted == 0 && failed > 0 {
		status = http.StatusServiceUnavailable
	} else if accepted == 0 && dropped > 0 {
		w.Header().Set("Retry-After", "1")
		status = http.StatusTooManyRequests
	}
	writeJSON(w, status, map[string]int{"accepted": accepted, "dropped": dropped, "failed": failed, "rejected": rejected})
}

func (s *Server) handleMetricsLatency(w http.ResponseWriter, r *http.Request) {
	service := strings.TrimSpace(r.URL.Query().Get("service"))
	summary := s.pipe.LatencyMetrics().Summary(service)
	writeJSON(w, http.StatusOK, map[string]any{
		"service": service,
		"latency": summary,
	})
}

func clampLimit(raw string) int {
	limit, _ := strconv.Atoi(raw)
	if limit <= 0 {
		return 100
	}
	if limit > maxQueryLimit {
		return maxQueryLimit
	}
	return limit
}

func (s *Server) handleQueryLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	rawLvl := strings.TrimSpace(q.Get("level"))
	var lvl model.LogLevel
	if rawLvl != "" {
		norm, ok := normalizeAndValidateLevel(model.LogLevel(rawLvl))
		if !ok {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid level query parameter"})
			return
		}
		lvl = norm
	}

	rawAnom := strings.TrimSpace(q.Get("anomaly"))
	onlyAnom := false
	if rawAnom != "" {
		parsedBool, err := strconv.ParseBool(rawAnom)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "anomaly query parameter must be true or false"})
			return
		}
		onlyAnom = parsedBool
	}

	filter := model.QueryFilter{
		Service:     strings.TrimSpace(q.Get("service")),
		Level:       lvl,
		OnlyAnomaly: onlyAnom,
		Limit:       clampLimit(q.Get("limit")),
	}
	results := s.store.Query(filter)
	writeJSON(w, http.StatusOK, map[string]any{"count": len(results), "logs": results})
}

func (s *Server) handleQueryAnomalies(w http.ResponseWriter, r *http.Request) {
	results := s.store.Query(model.QueryFilter{
		Service:     strings.TrimSpace(r.URL.Query().Get("service")),
		OnlyAnomaly: true,
		Limit:       clampLimit(r.URL.Query().Get("limit")),
	})
	writeJSON(w, http.StatusOK, map[string]any{"count": len(results), "anomalies": results})
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
