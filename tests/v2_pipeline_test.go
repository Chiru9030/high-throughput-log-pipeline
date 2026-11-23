package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Chiru9030/high-throughput-log-pipeline/internal/anomaly"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/httpapi"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/metrics"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/model"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/pipeline"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/storage"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/wal"
)

func TestSyslogAndLogfmtParsing(t *testing.T) {
	store, _ := storage.NewIndexedStore(1000, "")
	defer store.Close()

	pipe := pipeline.New(pipeline.Config{
		QueueCapacity: 64,
		WorkerCount:   2,
		BatchSize:     1,
		FlushInterval: 10 * time.Millisecond,
	}, anomaly.NewDetector(3.0, 10), store)

	srv := httpapi.NewServer(pipe, store)

	// 1. Ingest Logfmt via text/plain
	logfmtData := `service=auth-svc level=warn latency_ms=45.2 msg="rate limit reached"`
	req1 := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewBufferString(logfmtData))
	req1.Header.Set("Content-Type", "text/plain")
	rec1 := httptest.NewRecorder()
	srv.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted for logfmt, got %d", rec1.Code)
	}

	// 2. Ingest Syslog RFC 5424 via text/plain
	syslogData := `<34>1 2026-11-23T11:00:00Z myhost payment-svc 1234 ID47 - transaction settled successfully`
	req2 := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewBufferString(syslogData))
	req2.Header.Set("Content-Type", "text/plain")
	rec2 := httptest.NewRecorder()
	srv.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted for syslog, got %d", rec2.Code)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = pipe.Shutdown(ctx)

	// Verify both were stored
	logs := store.Query(model.QueryFilter{Limit: 10})
	if len(logs) != 2 {
		t.Fatalf("expected 2 logs stored, got %d", len(logs))
	}
}

func TestWriteAheadLogCrashRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	// Create and write 10 events
	walLog, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		_, err := walLog.Append(model.LogEvent{
			Service:   "payment-api",
			Level:     model.LevelInfo,
			Message:   "charge created",
			LatencyMs: float64(10 + i),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = walLog.Close()

	// Simulate crash & restart: open new WAL and recover
	walRecovery, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer walRecovery.Close()

	recovered, err := walRecovery.Recover()
	if err != nil {
		t.Fatalf("recovery error: %v", err)
	}
	if len(recovered) != 10 {
		t.Fatalf("expected 10 recovered events, got %d", len(recovered))
	}
	if recovered[0].Service != "payment-api" || recovered[9].LatencyMs != 19.0 {
		t.Fatalf("recovered event mismatch: %+v", recovered[0])
	}
}

func TestPercentileAggregatorP50P90P99(t *testing.T) {
	agg := metrics.NewPercentileAggregator(time.Minute)

	// Feed 100 sample latencies from 1ms to 100ms
	for i := 1; i <= 100; i++ {
		agg.Record("checkout", float64(i))
	}

	summary := agg.Summary("checkout")
	if summary.Count != 100 {
		t.Fatalf("expected count 100, got %v", summary.Count)
	}
	// P50 should be ~50ms
	if summary.P50 < 49 || summary.P50 > 51 {
		t.Fatalf("expected P50 around 50, got %v", summary.P50)
	}
	// P90 should be ~90ms
	if summary.P90 < 89 || summary.P90 > 91 {
		t.Fatalf("expected P90 around 90, got %v", summary.P90)
	}
	// P99 should be ~99ms
	if summary.P99 < 98 || summary.P99 > 100 {
		t.Fatalf("expected P99 around 99, got %v", summary.P99)
	}
}

func TestMetricsLatencyEndpoint(t *testing.T) {
	store, _ := storage.NewIndexedStore(1000, "")
	defer store.Close()

	pipe := pipeline.New(pipeline.Config{
		QueueCapacity: 64,
		WorkerCount:   1,
		BatchSize:     1,
		FlushInterval: 5 * time.Millisecond,
	}, anomaly.NewDetector(3.0, 10), store)

	srv := httpapi.NewServer(pipe, store)

	// Ingest event with latency
	_ = pipe.Enqueue(model.LogEvent{
		Service:   "orders",
		Message:   "order created",
		LatencyMs: 25.0,
	})

	time.Sleep(50 * time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/v1/metrics/latency?service=orders", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp struct {
		Service string                 `json:"service"`
		Latency metrics.LatencySummary `json:"latency"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Service != "orders" || resp.Latency.Count != 1 || resp.Latency.P50 != 25.0 {
		t.Fatalf("unexpected latency response: %+v", resp)
	}

	_ = pipe.Shutdown(context.Background())
}
