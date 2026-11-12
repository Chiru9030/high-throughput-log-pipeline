package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Chiru9030/high-throughput-log-pipeline/internal/anomaly"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/httpapi"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/model"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/pipeline"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/storage"
)

func TestPipelineBatchingRedactionAndAnomalyDetection(t *testing.T) {
	store, err := storage.NewIndexedStore(10_000, "")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	detector := anomaly.NewDetector(3.0, 20)
	pipe := pipeline.New(pipeline.Config{
		QueueCapacity: 1024,
		WorkerCount:   2,
		BatchSize:     32,
		FlushInterval: 15 * time.Millisecond,
	}, detector, store)

	// Feed 30 normal baseline events (latency ~10-12ms)
	for i := 0; i < 30; i++ {
		lat := 10.0 + float64(i%3)
		if err := pipe.Enqueue(model.LogEvent{
			Service:   "checkout-api",
			Level:     model.LevelInfo,
			Message:   "processed request with key sk_live_abcd1234efgh",
			LatencyMs: lat,
		}); err != nil {
			t.Fatalf("unexpected enqueue error: %v", err)
		}
	}

	// Feed a statistical latency spike (500ms)
	_ = pipe.Enqueue(model.LogEvent{
		Service:   "checkout-api",
		Level:     model.LevelWarn,
		Message:   "slow downstream call",
		LatencyMs: 500.0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pipe.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}

	total, anomalies := store.Stats()
	if total != 31 {
		t.Fatalf("expected 31 stored events, got %d", total)
	}
	if anomalies != 1 {
		t.Fatalf("expected 1 anomaly (latency spike), got %d", anomalies)
	}

	// Verify PII/secret redaction
	logs := store.Query(model.QueryFilter{Service: "checkout-api", Limit: 50})
	for _, l := range logs {
		if strings.Contains(l.Message, "sk_live_") {
			t.Fatalf("expected secret to be redacted, got %q", l.Message)
		}
	}

	anomLogs := store.Query(model.QueryFilter{OnlyAnomaly: true, Limit: 10})
	if len(anomLogs) != 1 || anomLogs[0].AnomalyReason != "latency_zscore_spike" {
		t.Fatalf("unexpected anomaly query result: %+v", anomLogs)
	}
}

func TestHTTPIngestAndBackpressure(t *testing.T) {
	store, _ := storage.NewIndexedStore(1000, "")
	defer store.Close()

	// Tiny queue + slow flush to test HTTP 202 + query
	pipe := pipeline.New(pipeline.Config{
		QueueCapacity: 64,
		WorkerCount:   2,
		BatchSize:     8,
		FlushInterval: 10 * time.Millisecond,
	}, anomaly.NewDetector(3.0, 10), store)

	srv := httpapi.NewServer(pipe, store)

	payload, _ := json.Marshal([]model.LogEvent{
		{Service: "payments", Level: model.LevelInfo, Message: "charge ok", LatencyMs: 12},
		{Service: "payments", Level: model.LevelFatal, Message: "panic in worker", StatusCode: 500},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/ingest/batch", bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", rec.Code)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = pipe.Shutdown(ctx)

	qReq := httptest.NewRequest(http.MethodGet, "/v1/anomalies?service=payments", nil)
	qRec := httptest.NewRecorder()
	srv.ServeHTTP(qRec, qReq)
	if qRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", qRec.Code)
	}

	var resp struct {
		Count int `json:"count"`
	}
	_ = json.Unmarshal(qRec.Body.Bytes(), &resp)
	if resp.Count != 1 {
		t.Fatalf("expected 1 anomaly from /v1/anomalies, got %d", resp.Count)
	}

	// Oversized single event (>256 KiB) -> 413 Request Entity Too Large
	bigMsg := strings.Repeat("x", 300*1024)
	bigBody, _ := json.Marshal(model.LogEvent{Service: "payments", Message: bigMsg})
	bigReq := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(bigBody))
	bigRec := httptest.NewRecorder()
	srv.ServeHTTP(bigRec, bigReq)
	if bigRec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 RequestEntityTooLarge, got %d", bigRec.Code)
	}
}

func BenchmarkPipelineIngest(b *testing.B) {
	store, _ := storage.NewIndexedStore(50_000, "")
	defer store.Close()

	pipe := pipeline.New(pipeline.Config{
		QueueCapacity: 65536,
		WorkerCount:   4,
		BatchSize:     512,
		FlushInterval: 10 * time.Millisecond,
	}, anomaly.NewDetector(3.0, 50), store)

	ev := model.LogEvent{
		Timestamp: time.Now(),
		Service:   "ledger-service",
		Level:     model.LevelInfo,
		Message:   "transfer posted",
		LatencyMs: 4.2,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pipe.Enqueue(ev)
	}
	b.StopTimer()
	_ = pipe.Shutdown(context.Background())
}

