package storage

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"

	"github.com/Chiru9030/high-throughput-log-pipeline/internal/model"
)

type Store interface {
	InsertBatch(events []model.LogEvent) error
	Query(filter model.QueryFilter) []model.LogEvent
	Stats() (totalStored int, totalAnomalies int)
	Close() error
}

// IndexedStore stores log batches in a bounded ring buffer with secondary indexes
// by service, level, and anomaly flag for sub-millisecond queries, optionally
// persisting batches to an append-only NDJSON segment file.
type IndexedStore struct {
	mu           sync.RWMutex
	capacity     int
	events       []model.LogEvent
	byService    map[string][]int
	anomalies    []int
	totalStored  int
	ndjsonFile   *os.File
	ndjsonWriter *bufio.Writer
}

func NewIndexedStore(capacity int, ndjsonPath string) (*IndexedStore, error) {
	if capacity <= 0 {
		capacity = 100_000
	}
	s := &IndexedStore{
		capacity:  capacity,
		events:    make([]model.LogEvent, 0, min(capacity, 4096)),
		byService: make(map[string][]int),
	}
	if ndjsonPath != "" {
		f, err := os.OpenFile(ndjsonPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		s.ndjsonFile = f
		s.ndjsonWriter = bufio.NewWriterSize(f, 64*1024)
	}
	return s, nil
}

func (s *IndexedStore) InsertBatch(batch []model.LogEvent) error {
	if len(batch) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range batch {
		ev := batch[i]
		if len(s.events) >= s.capacity {
			// Evict oldest half when ring reaches capacity and rebuild indexes cleanly
			half := s.capacity / 2
			copy(s.events, s.events[half:])
			s.events = s.events[:len(s.events)-half]
			s.rebuildIndexesLocked()
		}
		idx := len(s.events)
		s.events = append(s.events, ev)
		s.byService[ev.Service] = append(s.byService[ev.Service], idx)
		if ev.IsAnomaly {
			s.anomalies = append(s.anomalies, idx)
		}
		s.totalStored++

		if s.ndjsonWriter != nil {
			if line, err := json.Marshal(ev); err == nil {
				_, _ = s.ndjsonWriter.Write(line)
				_ = s.ndjsonWriter.WriteByte('\n')
			}
		}
	}

	if s.ndjsonWriter != nil {
		return s.ndjsonWriter.Flush()
	}
	return nil
}

func (s *IndexedStore) rebuildIndexesLocked() {
	clear(s.byService)
	s.anomalies = s.anomalies[:0]
	for idx, ev := range s.events {
		s.byService[ev.Service] = append(s.byService[ev.Service], idx)
		if ev.IsAnomaly {
			s.anomalies = append(s.anomalies, idx)
		}
	}
}

func (s *IndexedStore) Query(filter model.QueryFilter) []model.LogEvent {
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var indices []int
	switch {
	case filter.OnlyAnomaly:
		indices = s.anomalies
	case filter.Service != "":
		indices = s.byService[filter.Service]
	default:
		// Scan newest-first across all events
		out := make([]model.LogEvent, 0, min(limit, len(s.events)))
		for i := len(s.events) - 1; i >= 0 && len(out) < limit; i-- {
			ev := s.events[i]
			if matchesFilter(ev, filter) {
				out = append(out, ev)
			}
		}
		return out
	}

	out := make([]model.LogEvent, 0, min(limit, len(indices)))
	for i := len(indices) - 1; i >= 0 && len(out) < limit; i-- {
		ev := s.events[indices[i]]
		if matchesFilter(ev, filter) {
			out = append(out, ev)
		}
	}
	return out
}

func matchesFilter(ev model.LogEvent, f model.QueryFilter) bool {
	if f.Service != "" && ev.Service != f.Service {
		return false
	}
	if f.Level != "" && ev.Level != f.Level {
		return false
	}
	if f.OnlyAnomaly && !ev.IsAnomaly {
		return false
	}
	if !f.Since.IsZero() && ev.Timestamp.Before(f.Since) {
		return false
	}
	return true
}

func (s *IndexedStore) Stats() (totalStored int, totalAnomalies int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.totalStored, len(s.anomalies)
}

func (s *IndexedStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ndjsonWriter != nil {
		_ = s.ndjsonWriter.Flush()
	}
	if s.ndjsonFile != nil {
		return s.ndjsonFile.Close()
	}
	return nil
}
