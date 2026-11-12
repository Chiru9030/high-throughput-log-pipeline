package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Chiru9030/high-throughput-log-pipeline/internal/anomaly"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/model"
	"github.com/Chiru9030/high-throughput-log-pipeline/internal/storage"
)

var ErrQueueFull = errors.New("ingestion queue saturated: apply backpressure")

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
	Enqueued    uint64 `json:"enqueued"`
	Dropped     uint64 `json:"dropped"`
	Processed   uint64 `json:"processed"`
	Batches     uint64 `json:"batches_flushed"`
	Anomalies   uint64 `json:"anomalies_detected"`
	QueueDepth  int    `json:"queue_depth"`
}

type Pipeline struct {
	cfg       Config
	queue     chan model.LogEvent
	detector  *anomaly.Detector
	store     storage.Store
	batchPool sync.Pool

	wg     sync.WaitGroup
	closed atomic.Bool

	enqueued  atomic.Uint64
	dropped   atomic.Uint64
	processed atomic.Uint64
	batches   atomic.Uint64
	anomalies atomic.Uint64
}

func New(cfg Config, detector *anomaly.Detector, store storage.Store) *Pipeline {
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

	p := &Pipeline{
		cfg:      cfg,
		queue:    make(chan model.LogEvent, cfg.QueueCapacity),
		detector: detector,
		store:    store,
		batchPool: sync.Pool{
			New: func() any {
				s := make([]model.LogEvent, 0, cfg.BatchSize)
				return &s
			},
		},
	}

	for i := 0; i < cfg.WorkerCount; i++ {
		p.wg.Add(1)
		go p.workerLoop()
	}

	return p
}

// Enqueue submits an event non-blockingly. Returns ErrQueueFull when saturated.
func (p *Pipeline) Enqueue(ev model.LogEvent) error {
	if p.closed.Load() {
		return errors.New("pipeline is shutting down")
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}

	select {
	case p.queue <- ev:
		p.enqueued.Add(1)
		return nil
	default:
		p.dropped.Add(1)
		return ErrQueueFull
	}
}

func (p *Pipeline) workerLoop() {
	defer p.wg.Done()

	batchPtr := p.batchPool.Get().(*[]model.LogEvent)
	batch := (*batchPtr)[:0]
	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		var batchAnomalies uint64
		for i := range batch {
			p.detector.Inspect(&batch[i])
			if batch[i].IsAnomaly {
				batchAnomalies++
			}
		}
		if err := p.store.InsertBatch(batch); err == nil {
			p.processed.Add(uint64(len(batch)))
			p.batches.Add(1)
			if batchAnomalies > 0 {
				p.anomalies.Add(batchAnomalies)
			}
		}
		// Reset slice length while retaining underlying array capacity
		clear(batch)
		batch = batch[:0]
	}

	for {
		select {
		case ev, ok := <-p.queue:
			if !ok {
				flush()
				*batchPtr = batch
				p.batchPool.Put(batchPtr)
				return
			}
			batch = append(batch, ev)
			if len(batch) >= p.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (p *Pipeline) Stats() Stats {
	return Stats{
		Enqueued:   p.enqueued.Load(),
		Dropped:    p.dropped.Load(),
		Processed:  p.processed.Load(),
		Batches:    p.batches.Load(),
		Anomalies:  p.anomalies.Load(),
		QueueDepth: len(p.queue),
	}
}

// Shutdown closes the ingestion channel and waits for workers to flush remaining batches.
func (p *Pipeline) Shutdown(ctx context.Context) error {
	if p.closed.Swap(true) {
		return nil
	}
	close(p.queue)

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

