# High-Throughput Log Aggregation & Event Ingestion Pipeline

A high-throughput, low-allocation Go log ingestion and anomaly detection pipeline built for real-time telemetry processing.

## Key Engineering Highlights

1. **Non-Blocking Ingestion & Backpressure**:
   - `POST /v1/ingest` and `POST /v1/ingest/batch` enqueue into a bounded lock-free Go channel (`202 Accepted`), returning `429 Too Many Requests` (`Retry-After: 1`) under saturation instead of exhausting memory.
   - Multi-worker pool drains into configurable size-based (`BatchSize`) and time-based (`FlushInterval`) batches with graceful shutdown drain.
2. **Streaming Anomaly Detection & Secret Redaction**:
   - **Welford's Online Algorithm** (`internal/anomaly/detector.go`): Computes streaming mean and variance per service in O(1) memory and flags statistical latency spikes (`Z-score > 3.0`) as well as `FATAL` / `5xx` bursts.
   - **Automatic Redaction**: Masks API keys (`sk_live_*`, `sk_test_*`), Bearer tokens, and credit card PANs before storage.
3. **Memory & Query Optimization**:
   - `sync.Pool` batch slice reuse minimizes GC pressure under high ingestion rates.
   - Bounded ring-buffer `IndexedStore` maintains secondary indexes by `service` and `is_anomaly` for fast sub-millisecond queries (`GET /v1/logs`, `GET /v1/anomalies`), with optional buffered NDJSON persistence.

## Quick Start

```bash
# Run unit/integration tests with race detector and benchmarks
go test -v -race -bench=. ./...

# Run service locally
go run ./cmd/pipeline
```
