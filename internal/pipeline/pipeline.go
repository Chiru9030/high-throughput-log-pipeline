package pipeline

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chiru9030/high-throughput-log-pipeline/internal/anomaly"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/metrics"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/model"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/storage"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/wal"
)

var (
	ErrQueueFull      = errors.New("ingestion queue saturated: apply backpressure")
	ErrPipelineClosed = errors.New("pipeline is shutting down")
)

type Config struct {
	QueueCapacity int
	WorkerCount   int
	BatchSize     int
	FlushInterval time.Duration
}

func DefaultConfig() Config {
	return Config{
		QueueCapacity: 16384,
		WorkerCount:   4,
		BatchSize:     256,
		FlushInterval: 50 * time.Millisecond,
	}
}

type Stats struct {
	Enqueued      uint64 `json:"enqueued"`
	Dropped       uint64 `json:"dropped"`
	Processed     uint64 `json:"processed"`
	Batches       uint64 `json:"batches_flushed"`
	Anomalies     uint64 `json:"anomalies_detected"`
	PersistErrors uint64 `json:"persist_errors"`
	QueueDepth    int    `json:"queue_depth"`
}

type queuedEvent struct {
	seq uint64
	ev  model.LogEvent
}

type Pipeline struct {
	cfg        Config
	shards     []chan queuedEvent
	shardDepth []atomic.Int64
	detector   *anomaly.Detector
	store      storage.Store
	batchPool  sync.Pool
	wal        *wal.WAL
	metrics    *metrics.PercentileAggregator

	closeMu sync.RWMutex
	closed  bool
	wg      sync.WaitGroup

	ckptMu     sync.Mutex
	inflight   map[uint64]struct{}
	highestSeq uint64
	eventSeq   atomic.Uint64

	enqueued      atomic.Uint64
	dropped       atomic.Uint64
	processed     atomic.Uint64
	batches       atomic.Uint64
	anomalies     atomic.Uint64
	persistErrors atomic.Uint64
}

func New(cfg Config, detector *anomaly.Detector, store storage.Store) *Pipeline {
	return NewWithWAL(cfg, detector, store, nil)
}

func NewWithWAL(cfg Config, detector *anomaly.Detector, store storage.Store, walLog *wal.WAL) *Pipeline {
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 16384
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 4
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 50 * time.Millisecond
	}

	shardCap := (cfg.QueueCapacity + cfg.WorkerCount - 1) / cfg.WorkerCount
	if shardCap < 1 {
		shardCap = 1
	}
	shards := make([]chan queuedEvent, cfg.WorkerCount)
	for i := range shards {
		shards[i] = make(chan queuedEvent, shardCap)
	}

	p := &Pipeline{
		cfg:        cfg,
		shards:     shards,
		shardDepth: make([]atomic.Int64, cfg.WorkerCount),
		detector:   detector,
		store:      store,
		wal:        walLog,
		metrics:    metrics.NewPercentileAggregator(time.Minute),
		inflight:   make(map[uint64]struct{}),
		batchPool: sync.Pool{
			New: func() any {
				s := make([]model.LogEvent, 0, cfg.BatchSize)
				return &s
			},
		},
	}

	for i := 0; i < cfg.WorkerCount; i++ {
		p.wg.Add(1)
		go p.workerLoop(i, p.shards[i])
	}

	return p
}

func (p *Pipeline) shardIndex(service string) int {
	if len(p.shards) == 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(service))
	return int(h.Sum32() % uint32(len(p.shards)))
}

func (p *Pipeline) trackInFlight(seq uint64) {
	if seq == 0 {
		return
	}
	p.ckptMu.Lock()
	p.inflight[seq] = struct{}{}
	if seq > p.highestSeq {
		p.highestSeq = seq
	}
	p.ckptMu.Unlock()
}

func (p *Pipeline) untrackInFlight(seq uint64) {
	if seq == 0 {
		return
	}
	p.ckptMu.Lock()
	delete(p.inflight, seq)
	p.ckptMu.Unlock()
}

func (p *Pipeline) markFlushed(seqs []uint64) {
	if p.wal == nil || !p.store.HasColdStorage() || len(seqs) == 0 {
		return
	}
	p.ckptMu.Lock()
	for _, s := range seqs {
		if s > 0 {
			delete(p.inflight, s)
		}
	}
	safeSeq := p.highestSeq
	if len(p.inflight) > 0 {
		var minInflight uint64
		for s := range p.inflight {
			if minInflight == 0 || s < minInflight {
				minInflight = s
			}
		}
		if minInflight > 0 {
			safeSeq = minInflight - 1
		}
	}
	p.ckptMu.Unlock()

	if safeSeq > 0 {
		_ = p.wal.MarkCommitted(safeSeq)
	}
}

// WarmDetectorFromStore restores Welford latency baselines and recent window samples from cold-hydrated store records.
func (p *Pipeline) WarmDetectorFromStore() {
	hydrated := p.store.SnapshotEvents()
	for i := range hydrated {
		ev := hydrated[i]
		p.detector.Inspect(&ev)
		if ev.LatencyMs > 0 {
			p.metrics.RecordAt(ev.Service, ev.LatencyMs, ev.Timestamp)
		}
	}
}

// ReplayRecovered folds WAL-recovered historical events back into memory without duplicating cold-storage NDJSON lines.
func (p *Pipeline) ReplayRecovered(events []model.LogEvent) error {
	recs := make([]wal.Record, len(events))
	for i := range events {
		recs[i] = wal.Record{Seq: 0, Event: events[i]}
	}
	return p.ReplayRecoveredRecords(recs)
}

// ReplayRecoveredRecords restores WAL frames: deduplicates cold-hydrated records by ID/sequence and persists only unflushed crash records.
func (p *Pipeline) ReplayRecoveredRecords(recs []wal.Record) error {
	if len(recs) == 0 {
		return nil
	}
	capLimit := p.store.Capacity()
	hasCold := p.store.HasColdStorage()
	storedBefore, _ := p.store.Stats()
	var committed uint64
	if p.wal != nil {
		committed = p.wal.CommittedSeq()
	}

	var alreadyFlushed []model.LogEvent
	var pendingColdFlush []model.LogEvent
	var maxPendingSeq uint64
	var skippedCold int

	for i := range recs {
		ev := recs[i].Event
		// Skip events already hydrated from cold NDJSON file (either pre-checkpoint or deduped via unique event ID).
		if hasCold && storedBefore > 0 && ((recs[i].Seq > 0 && recs[i].Seq <= committed) || (ev.ID != "" && p.store.HasHydratedID(ev.ID))) {
			skippedCold++
			continue
		}
		p.detector.Inspect(&ev)
		if ev.LatencyMs > 0 {
			p.metrics.RecordAt(ev.Service, ev.LatencyMs, ev.Timestamp)
		}
		if ev.IsAnomaly {
			p.anomalies.Add(1)
		}
		if hasCold && recs[i].Seq > committed {
			pendingColdFlush = append(pendingColdFlush, ev)
			if recs[i].Seq > maxPendingSeq {
				maxPendingSeq = recs[i].Seq
			}
		} else {
			alreadyFlushed = append(alreadyFlushed, ev)
		}
	}

	if capLimit > 0 && len(alreadyFlushed) > capLimit {
		alreadyFlushed = alreadyFlushed[len(alreadyFlushed)-capLimit:]
	}

	if len(alreadyFlushed) > 0 {
		if err := p.store.InsertRecoveredBatch(alreadyFlushed); err != nil {
			p.persistErrors.Add(1)
			return err
		}
	}
	if len(pendingColdFlush) > 0 {
		if err := p.store.InsertBatch(pendingColdFlush); err != nil {
			p.persistErrors.Add(1)
			return err
		}
		if p.wal != nil && maxPendingSeq > 0 {
			_ = p.wal.MarkCommitted(maxPendingSeq)
		}
	}
	actuallyReplayed := len(alreadyFlushed) + len(pendingColdFlush)
	if actuallyReplayed > 0 {
		p.processed.Add(uint64(actuallyReplayed))
		p.batches.Add(1)
	}
	_ = skippedCold
	return nil
}

func (p *Pipeline) LatencyMetrics() *metrics.PercentileAggregator {
	return p.metrics
}

// Enqueue assigns a server event ID, redacts all fields, persists synchronously to WAL when enabled, and routes to FIFO worker.
func (p *Pipeline) Enqueue(ev model.LogEvent) error {
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}
	if ev.ID == "" {
		ev.ID = fmt.Sprintf("ev-%d-%d", time.Now().UnixNano(), p.eventSeq.Add(1))
	}
	anomaly.RedactEvent(&ev)

	p.closeMu.RLock()
	defer p.closeMu.RUnlock()
	if p.closed {
		return ErrPipelineClosed
	}

	idx := p.shardIndex(ev.Service)
	target := p.shards[idx]
	shardCap := int64(cap(target))
	if p.shardDepth[idx].Add(1) > shardCap {
		p.shardDepth[idx].Add(-1)
		p.dropped.Add(1)
		return ErrQueueFull
	}

	var seq uint64
	if p.wal != nil {
		var err error
		seq, err = p.wal.AppendWithHook(ev, p.trackInFlight)
		if err != nil {
			p.untrackInFlight(seq)
			p.shardDepth[idx].Add(-1)
			p.persistErrors.Add(1)
			return fmt.Errorf("wal persistence failed: %w", err)
		}
	}

	target <- queuedEvent{seq: seq, ev: ev}
	p.enqueued.Add(1)
	return nil
}

func (p *Pipeline) workerLoop(shardIdx int, ch <-chan queuedEvent) {
	defer p.wg.Done()

	batchPtr := p.batchPool.Get().(*[]model.LogEvent)
	batch := (*batchPtr)[:0]
	seqBatch := make([]uint64, 0, p.cfg.BatchSize)
	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		var batchAnomalies uint64
		for i := range batch {
			p.detector.Inspect(&batch[i])
			if batch[i].LatencyMs > 0 {
				p.metrics.RecordAt(batch[i].Service, batch[i].LatencyMs, batch[i].Timestamp)
			}
			if batch[i].IsAnomaly {
				batchAnomalies++
			}
		}
		if err := p.store.InsertBatch(batch); err != nil {
			p.persistErrors.Add(1)
			slog.Error("store_insert_batch_failed", "batch_size", len(batch), "error", err.Error())
		} else {
			p.markFlushed(seqBatch)
			p.processed.Add(uint64(len(batch)))
			p.batches.Add(1)
			if batchAnomalies > 0 {
				p.anomalies.Add(batchAnomalies)
			}
		}
		clear(batch)
		batch = batch[:0]
		seqBatch = seqBatch[:0]
	}

	for {
		select {
		case qev, ok := <-ch:
			if !ok {
				flush()
				*batchPtr = batch
				p.batchPool.Put(batchPtr)
				return
			}
			p.shardDepth[shardIdx].Add(-1)
			batch = append(batch, qev.ev)
			seqBatch = append(seqBatch, qev.seq)
			if len(batch) >= p.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (p *Pipeline) Stats() Stats {
	depth := 0
	for _, ch := range p.shards {
		depth += len(ch)
	}
	return Stats{
		Enqueued:      p.enqueued.Load(),
		Dropped:       p.dropped.Load(),
		Processed:     p.processed.Load(),
		Batches:       p.batches.Load(),
		Anomalies:     p.anomalies.Load(),
		PersistErrors: p.persistErrors.Load(),
		QueueDepth:    depth,
	}
}

// Shutdown atomically closes shards, drains pending batches, and compacts WAL when backed by cold storage.
func (p *Pipeline) Shutdown(ctx context.Context) error {
	p.closeMu.Lock()
	if p.closed {
		p.closeMu.Unlock()
		return nil
	}
	p.closed = true
	for _, ch := range p.shards {
		close(ch)
	}
	p.closeMu.Unlock()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if p.wal != nil && p.store.HasColdStorage() {
			_ = p.wal.CompactIfNeeded(false)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
