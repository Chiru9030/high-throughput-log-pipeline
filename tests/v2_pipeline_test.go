package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Chiru9030/high-throughput-log-pipeline/internal/anomaly"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/httpapi"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/metrics"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/model"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/parser"
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

func TestAuthSecurityHeadersDashboardAndPrometheus(t *testing.T) {
	store, _ := storage.NewIndexedStore(1000, "")
	defer store.Close()

	pipe := pipeline.New(pipeline.Config{
		QueueCapacity: 32,
		WorkerCount:   2,
		BatchSize:     4,
		FlushInterval: 5 * time.Millisecond,
	}, anomaly.NewDetector(3.0, 10), store)
	defer pipe.Shutdown(context.Background())

	srv := httpapi.NewServerWithAuth(pipe, store, "test_secret_token_v1")

	// Public probes & dashboard shell must succeed without key and include security headers
	for _, route := range []string{"/", "/dashboard", "/healthz", "/readyz"} {
		req := httptest.NewRequest(http.MethodGet, route, nil)
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 for public route %s, got %d", route, rec.Code)
		}
		if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("missing nosniff header on %s", route)
		}
		if rec.Header().Get("X-Frame-Options") != "DENY" {
			t.Fatalf("missing X-Frame-Options header on %s", route)
		}
	}

	// Protected endpoints must reject missing/wrong credentials
	unauthReq := httptest.NewRequest(http.MethodGet, "/v1/logs", nil)
	unauthRec := httptest.NewRecorder()
	srv.ServeHTTP(unauthRec, unauthReq)
	if unauthRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", unauthRec.Code)
	}

	// Authorized ingest via Bearer token succeeds
	body := `{"service":"auth-svc","level":"ERROR","message":"token expired","status_code":500,"latency_ms":22}`
	authReq := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewBufferString(body))
	authReq.Header.Set("Content-Type", "application/json")
	authReq.Header.Set("Authorization", "Bearer test_secret_token_v1")
	authRec := httptest.NewRecorder()
	srv.ServeHTTP(authRec, authReq)
	if authRec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted with valid auth, got %d", authRec.Code)
	}

	time.Sleep(40 * time.Millisecond)

	// Prometheus metrics endpoint works with X-API-Key
	promReq := httptest.NewRequest(http.MethodGet, "/metrics/prometheus", nil)
	promReq.Header.Set("X-API-Key", "test_secret_token_v1")
	promRec := httptest.NewRecorder()
	srv.ServeHTTP(promRec, promReq)
	if promRec.Code != http.StatusOK || !bytes.Contains(promRec.Body.Bytes(), []byte("log_pipeline_enqueued_total")) {
		t.Fatalf("unexpected prometheus output: code=%d body=%s", promRec.Code, promRec.Body.String())
	}
}

func TestWALReplayRecoveredOnBoot(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "boot.wal")
	walLog, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	store1, _ := storage.NewIndexedStore(500, "")
	pipe1 := pipeline.NewWithWAL(pipeline.DefaultConfig(), anomaly.NewDetector(3.0, 20), store1, walLog)
	if err := pipe1.Enqueue(model.LogEvent{Service: "billing", Level: model.LevelFatal, Message: "db down", StatusCode: 503}); err != nil {
		t.Fatal(err)
	}
	_ = pipe1.Shutdown(context.Background())
	_ = walLog.Close()
	_ = store1.Close()

	// Simulated reboot into fresh store + WAL recovery
	walBoot, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer walBoot.Close()
	recovered, err := walBoot.Recover()
	if err != nil {
		t.Fatal(err)
	}
	store2, _ := storage.NewIndexedStore(500, "")
	defer store2.Close()
	pipe2 := pipeline.NewWithWAL(pipeline.DefaultConfig(), anomaly.NewDetector(3.0, 20), store2, walBoot)
	defer pipe2.Shutdown(context.Background())

	if err := pipe2.ReplayRecovered(recovered); err != nil {
		t.Fatal(err)
	}
	total, anom := store2.Stats()
	if total != 1 || anom != 1 {
		t.Fatalf("expected 1 replayed event & anomaly, got total=%d anom=%d", total, anom)
	}
}

func TestConcurrentEnqueueAndShutdownNoPanic(t *testing.T) {
	store, _ := storage.NewIndexedStore(2000, "")
	defer store.Close()

	pipe := pipeline.New(pipeline.Config{
		QueueCapacity: 256,
		WorkerCount:   4,
		BatchSize:     16,
		FlushInterval: 5 * time.Millisecond,
	}, anomaly.NewDetector(3.0, 20), store)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_ = pipe.Enqueue(model.LogEvent{
					Service:   "concurrent-svc",
					Level:     model.LevelInfo,
					Message:   "ok",
					LatencyMs: 10.0,
				})
			}
		}(g)
	}

	time.Sleep(3 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pipe.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}
	wg.Wait()
}

func TestWALCorruptHeaderTruncatesSafelyAndRedactsSecret(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "corrupt.wal")
	ndjsonPath := filepath.Join(tmpDir, "events.ndjson")

	walLog, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	store, err := storage.NewIndexedStore(500, ndjsonPath)
	if err != nil {
		t.Fatal(err)
	}
	pipe := pipeline.NewWithWAL(pipeline.DefaultConfig(), anomaly.NewDetector(3.0, 20), store, walLog)

	// Enqueue sensitive secret — WAL must never store raw token
	if err := pipe.Enqueue(model.LogEvent{
		Service: "vault",
		Level:   model.LevelInfo,
		Message: "token=Bearer abcdef1234567890",
	}); err != nil {
		t.Fatal(err)
	}
	_ = pipe.Shutdown(context.Background())
	_ = walLog.Close()
	_ = store.Close()

	rawWAL, _ := os.ReadFile(walPath)
	if bytes.Contains(rawWAL, []byte("abcdef1234567890")) {
		t.Fatalf("WAL persisted unredacted Bearer token")
	}

	// Inject garbage corrupt frame length (0xFFFFFFFF -> would OOM if unchecked)
	f, _ := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o600)
	var badHeader [16]byte
	binary.BigEndian.PutUint64(badHeader[0:8], 99)
	binary.BigEndian.PutUint32(badHeader[8:12], 0xFFFFFFFF)
	binary.BigEndian.PutUint32(badHeader[12:16], 0xDEADBEEF)
	_, _ = f.Write(badHeader[:])
	_ = f.Close()

	// Recovery must recover the 1 valid frame cleanly and truncate trailing garbage
	wal2, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()
	recovered, err := wal2.Recover()
	if err != nil || len(recovered) != 1 {
		t.Fatalf("expected 1 recovered record after corrupt header, got len=%d err=%v", len(recovered), err)
	}

	// Replaying into store must NOT duplicate NDJSON archival file lines
	store2, err := storage.NewIndexedStore(500, ndjsonPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	pipe2 := pipeline.NewWithWAL(pipeline.DefaultConfig(), anomaly.NewDetector(3.0, 20), store2, wal2)
	defer pipe2.Shutdown(context.Background())
	if err := pipe2.ReplayRecovered(recovered); err != nil {
		t.Fatal(err)
	}

	ndjsonBytes, _ := os.ReadFile(ndjsonPath)
	lines := strings.Split(strings.TrimSpace(string(ndjsonBytes)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected exactly 1 line in NDJSON without reboot duplication, got %d", len(lines))
	}
}

func TestEdgeCasesZeroStddevGlobalLatencyValidationAndSyslogPRI(t *testing.T) {
	store, _ := storage.NewIndexedStore(500, "")
	defer store.Close()

	pipe := pipeline.New(pipeline.Config{
		QueueCapacity: 64,
		WorkerCount:   2,
		BatchSize:     8,
		FlushInterval: 5 * time.Millisecond,
	}, anomaly.NewDetector(3.0, 20), store)

	// Feed 25 uniform latencies (stddev == 0), then a huge 500ms jump
	for i := 0; i < 25; i++ {
		_ = pipe.Enqueue(model.LogEvent{Service: "steady-svc", Level: model.LevelInfo, Message: "ok", LatencyMs: 10.0})
	}
	_ = pipe.Enqueue(model.LogEvent{Service: "steady-svc", Level: model.LevelInfo, Message: "spike", LatencyMs: 500.0})
	_ = pipe.Shutdown(context.Background())

	anoms := store.Query(model.QueryFilter{Service: "steady-svc", OnlyAnomaly: true, Limit: 10})
	if len(anoms) != 1 {
		t.Fatalf("expected 1 spike anomaly even over zero-variance baseline, got %d", len(anoms))
	}

	// Global latency summary (empty ?service=) must aggregate across services
	global := pipe.LatencyMetrics().Summary("")
	if global.Count != 26 || global.Max != 500.0 {
		t.Fatalf("expected global latency summary with 26 samples and max 500, got %+v", global)
	}

	// Check parser PRI & structured data handling + lowercase JSON level validation + text/plain 413 + 400 empty batch
	pipe2 := pipeline.New(pipeline.Config{QueueCapacity: 4, WorkerCount: 1, BatchSize: 4, FlushInterval: 5 * time.Millisecond}, anomaly.NewDetector(3.0, 10), store)
	defer pipe2.Shutdown(context.Background())
	srv := httpapi.NewServer(pipe2, store)

	// Empty batch -> 400
	bReq := httptest.NewRequest(http.MethodPost, "/v1/ingest/batch", bytes.NewBufferString(`[]`))
	bRec := httptest.NewRecorder()
	srv.ServeHTTP(bRec, bReq)
	if bRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on empty batch, got %d", bRec.Code)
	}

	// Invalid level -> 400
	lvlReq := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewBufferString(`{"service":"x","level":"BOGUS","message":"hi"}`))
	lvlRec := httptest.NewRecorder()
	srv.ServeHTTP(lvlRec, lvlReq)
	if lvlRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid log level, got %d", lvlRec.Code)
	}

	// Oversized text/plain (>256 KiB) -> 413
	hugeTxt := strings.Repeat("a", 300*1024)
	tReq := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewBufferString(hugeTxt))
	tReq.Header.Set("Content-Type", "text/plain")
	tRec := httptest.NewRecorder()
	srv.ServeHTTP(tRec, tReq)
	if tRec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 on oversized text/plain, got %d", tRec.Code)
	}

	// Syslog <11> (severity 3 -> ERROR) + structured data stripped from msg
	ev, ok := parser.ParseSyslogRFC5424(`<11>1 2026-11-23T11:00:00Z host pay-svc 42 ID1 [meta k="v"] gateway timeout`)
	if !ok || ev.Level != model.LevelError || ev.Message != "gateway timeout" {
		t.Fatalf("unexpected RFC5424 parse result: ok=%v ev=%+v", ok, ev)
	}
}

func TestV1ZeroCompromiseAuditRegressions(t *testing.T) {
	// 1. Redaction across Attributes/TraceID/Service & Luhn-preserving timestamps
	attrEv := model.LogEvent{
		Service:    "svc token=rawsecret123",
		TraceID:    "Bearer abcdef1234567890",
		Message:    "ts=1700000000000 card=4111111111111111",
		Attributes: map[string]string{"auth": "sk_live_abcdefgh123456"},
	}
	anomaly.RedactEvent(&attrEv)
	if strings.Contains(attrEv.Service, "rawsecret123") ||
		strings.Contains(attrEv.TraceID, "abcdef1234567890") ||
		strings.Contains(attrEv.Attributes["auth"], "sk_live_") ||
		strings.Contains(attrEv.Message, "4111111111111111") ||
		!strings.Contains(attrEv.Message, "1700000000000") {
		t.Fatalf("unexpected redaction/Luhn result: %+v", attrEv)
	}

	// 2. Client-spoofed anomaly, trailing JSON garbage, negative latency, invalid status/query, case-insensitive bearer, multi-line text/plain
	store, _ := storage.NewIndexedStore(500, "")
	defer store.Close()
	pipe := pipeline.New(pipeline.Config{QueueCapacity: 32, WorkerCount: 2, BatchSize: 4, FlushInterval: 5 * time.Millisecond}, anomaly.NewDetector(3.0, 20), store)
	defer pipe.Shutdown(context.Background())
	srv := httpapi.NewServerWithAuth(pipe, store, "audit_secret_v1")

	// Spoofed anomaly on benign INFO log must be cleared
	spoofReq := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewBufferString(`{"service":"pay","level":"info","message":"ok","is_anomaly":true,"anomaly_reason":"fake"}`))
	spoofReq.Header.Set("Authorization", "bearer audit_secret_v1")
	spoofRec := httptest.NewRecorder()
	srv.ServeHTTP(spoofRec, spoofReq)
	if spoofRec.Code != http.StatusAccepted {
		t.Fatalf("expected case-insensitive bearer 202, got %d", spoofRec.Code)
	}

	// Trailing garbage & negative latency must return 400
	for _, bad := range []string{
		`{"service":"x","level":"info","message":"ok"}{"extra":1}`,
		`{"service":"x","level":"info","message":"ok","latency_ms":-5}`,
		`{"service":"x","level":"info","message":"ok","status_code":999}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewBufferString(bad))
		req.Header.Set("X-API-Key", "audit_secret_v1")
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for %s, got %d", bad, rec.Code)
		}
	}

	// Invalid query parameters rejected with 400
	for _, q := range []string{"/v1/logs?level=NOPE", "/v1/logs?anomaly=maybe"} {
		req := httptest.NewRequest(http.MethodGet, q, nil)
		req.Header.Set("X-API-Key", "audit_secret_v1")
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for query %s, got %d", q, rec.Code)
		}
	}

	// Multi-line text/plain splits into multiple events
	multiBody := "service=a level=debug msg=\"one\"\nservice=b level=warn msg=\"two \\\"quoted\\\"\""
	mReq := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewBufferString(multiBody))
	mReq.Header.Set("Content-Type", "text/plain")
	mReq.Header.Set("X-API-Key", "audit_secret_v1")
	mRec := httptest.NewRecorder()
	srv.ServeHTTP(mRec, mReq)
	if mRec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for multi-line text/plain, got %d body=%s", mRec.Code, mRec.Body.String())
	}

	time.Sleep(40 * time.Millisecond)
	anomLogs := store.Query(model.QueryFilter{Service: "pay", OnlyAnomaly: true})
	if len(anomLogs) != 0 {
		t.Fatalf("client-spoofed is_anomaly leaked into anomalies: %+v", anomLogs)
	}
	bLogs := store.Query(model.QueryFilter{Service: "b"})
	if len(bLogs) != 1 || bLogs[0].Message != `two "quoted"` {
		t.Fatalf("unexpected logfmt escaped-quote multi-line parse: %+v", bLogs)
	}

	// 3. RFC5424 severity 7 -> DEBUG, invalid PRI/version rejected, escaped bracket SD skipped
	if evDbg, ok := parser.ParseSyslogRFC5424(`<15>1 2026-11-23T11:00:00Z host dbg 1 ID1 [meta k="a [b\] c"] clean msg`); !ok || evDbg.Level != model.LevelDebug || evDbg.Message != "clean msg" {
		t.Fatalf("RFC5424 severity 7 / SD parse failed: ok=%v ev=%+v", ok, evDbg)
	}
	if _, ok := parser.ParseSyslogRFC5424(`<999>1 2026-11-23T11:00:00Z host dbg 1 ID1 - bad pri`); ok {
		t.Fatal("expected out-of-range PRI to fail")
	}
	if _, ok := parser.ParseSyslogRFC5424(`<11>2 2026-11-23T11:00:00Z host dbg 1 ID1 - bad ver`); ok {
		t.Fatal("expected invalid RFC5424 version 2 to fail")
	}

	// 4. Percentile future timestamp clamping, out-of-order prune, nearest-rank 2-sample math
	pa := metrics.NewPercentileAggregator(time.Second)
	pa.RecordAt("lat", 10, time.Now().Add(time.Hour))
	pa.RecordAt("lat", 1000, time.Now())
	sum := pa.Summary("lat")
	if sum.P99 != 1000 || sum.Count != 2 {
		t.Fatalf("unexpected nearest-rank percentile summary: %+v", sum)
	}

	// 5. Cold NDJSON tail recovery + WAL unflushed crash recovery + compaction without duplicating NDJSON lines
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "crash.wal")
	ndjsonPath := filepath.Join(tmpDir, "cold.ndjson")
	w1, _ := wal.Open(walPath)
	seq1, _ := w1.Append(model.LogEvent{Service: "cold", Level: model.LevelInfo, Message: "flushed"})
	seq2, _ := w1.Append(model.LogEvent{Service: "cold", Level: model.LevelInfo, Message: "unflushed-crash"})
	_ = w1.MarkCommitted(seq1)
	_ = w1.Close()

	s1, _ := storage.NewIndexedStore(100, ndjsonPath)
	_ = s1.InsertBatch([]model.LogEvent{{Service: "cold", Level: model.LevelInfo, Message: "flushed"}})
	_ = s1.Close()

	// Reboot: cold file hydrates seq1; WAL replays ONLY unflushed seq2 into cold file
	s2, _ := storage.NewIndexedStore(100, ndjsonPath)
	defer s2.Close()
	w2, _ := wal.Open(walPath)
	defer w2.Close()
	p2 := pipeline.NewWithWAL(pipeline.DefaultConfig(), anomaly.NewDetector(3.0, 20), s2, w2)
	defer p2.Shutdown(context.Background())
	recs, _ := w2.RecoverRecords()
	if err := p2.ReplayRecoveredRecords(recs); err != nil {
		t.Fatal(err)
	}
	if err := w2.CompactIfNeeded(true); err != nil {
		t.Fatal(err)
	}
	if total, _ := s2.Stats(); total != 2 || seq2 <= seq1 {
		t.Fatalf("expected 2 events restored (flushed + crash-unflushed), got %d", total)
	}
	rawCold, _ := os.ReadFile(ndjsonPath)
	coldLines := strings.Split(strings.TrimSpace(string(rawCold)), "\n")
	if len(coldLines) != 2 {
		t.Fatalf("expected exactly 2 NDJSON lines without duplication or loss, got %d", len(coldLines))
	}

	// 6. Strict CSP and external script route on dashboard
	dReq := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	dRec := httptest.NewRecorder()
	srv.ServeHTTP(dRec, dReq)
	if dRec.Code != http.StatusOK || !strings.Contains(dRec.Header().Get("Content-Security-Policy"), "script-src 'self' https://code.jquery.com") {
		t.Fatalf("missing strict CSP on /dashboard: %q", dRec.Header().Get("Content-Security-Policy"))
	}
	jsReq := httptest.NewRequest(http.MethodGet, "/dashboard/app.js", nil)
	jsRec := httptest.NewRecorder()
	srv.ServeHTTP(jsRec, jsReq)
	if jsRec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /dashboard/app.js, got %d", jsRec.Code)
	}

	// 7. Reject NaN / +Inf latency_ms without poisoning metrics or JSON encoders
	for _, badLat := range []string{`service=pay msg=ok latency_ms=+Inf`, `service=pay msg=ok latency_ms=NaN`} {
		badReq := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewBufferString(badLat))
		badReq.Header.Set("Content-Type", "text/plain")
		badReq.Header.Set("X-API-Key", "audit_secret_v1")
		badRec := httptest.NewRecorder()
		srv.ServeHTTP(badRec, badReq)
		if badRec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 on non-finite latency (%s), got %d", badLat, badRec.Code)
		}
	}

	// 8. O(N) circular cold-tail hydration > capacity + dedup by unique ID when pre-ckpt crash occurs
	ringDir := t.TempDir()
	ringCold := filepath.Join(ringDir, "ring.ndjson")
	ringWAL := filepath.Join(ringDir, "ring.wal")
	preStore, err := storage.NewIndexedStore(5, ringCold)
	if err != nil {
		t.Fatal(err)
	}
	var lastEv model.LogEvent
	for i := 1; i <= 12; i++ {
		ev := model.LogEvent{ID: "id-" + string(rune('a'+i)), Service: "ring", Level: model.LevelInfo, Message: "msg", LatencyMs: float64(i)}
		if err := preStore.InsertBatch([]model.LogEvent{ev}); err != nil {
			t.Fatal(err)
		}
		lastEv = ev
	}
	_ = preStore.Close()

	// Append the already-flushed lastEv into WAL without advancing ckpt (simulating post-flush/pre-ckpt crash)
	rw, err := wal.Open(ringWAL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rw.Append(lastEv); err != nil {
		t.Fatal(err)
	}
	_ = rw.Close()

	hydStore, err := storage.NewIndexedStore(5, ringCold)
	if err != nil {
		t.Fatal(err)
	}
	defer hydStore.Close()
	hydWAL, _ := wal.Open(ringWAL)
	defer hydWAL.Close()
	hydPipe := pipeline.NewWithWAL(pipeline.DefaultConfig(), anomaly.NewDetector(3.0, 20), hydStore, hydWAL)
	defer hydPipe.Shutdown(context.Background())
	hydPipe.WarmDetectorFromStore()
	hRecs, _ := hydWAL.RecoverRecords()
	if err := hydPipe.ReplayRecoveredRecords(hRecs); err != nil {
		t.Fatal(err)
	}
	rawRing, _ := os.ReadFile(ringCold)
	ringLines := strings.Split(strings.TrimSpace(string(rawRing)), "\n")
	if len(ringLines) != 12 {
		t.Fatalf("expected zero duplicate NDJSON lines after post-flush/pre-ckpt crash, got %d", len(ringLines))
	}
	queried := hydStore.Query(model.QueryFilter{Service: "ring", Limit: 10})
	if len(queried) != 5 || queried[0].LatencyMs != 12 {
		t.Fatalf("expected circular ring tail [8..12], got len=%d latest=%+v", len(queried), queried)
	}
}


