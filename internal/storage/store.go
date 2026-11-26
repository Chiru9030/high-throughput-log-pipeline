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
	InsertRecoveredBatch(events []model.LogEvent) error
	Query(filter model.QueryFilter) []model.LogEvent
	Stats() (totalStored int, totalAnomalies int)
	Capacity() int
	HasColdStorage() bool
	HasHydratedID(id string) bool
	SnapshotEvents() []model.LogEvent
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
	hydratedIDs  map[string]struct{}
	ndjsonFile   *os.File
	ndjsonWriter *bufio.Writer
}

func NewIndexedStore(capacity int, ndjsonPath string) (*IndexedStore, error) {
	if capacity <= 0 {
		capacity = 100_000
	}
	s := &IndexedStore{
		capacity:    capacity,
		events:      make([]model.LogEvent, 0, min(capacity, 4096)),
		byService:   make(map[string][]int),
		hydratedIDs: make(map[string]struct{}),
	}
	if ndjsonPath != "" {
		if rf, rErr := os.Open(ndjsonPath); rErr == nil {
			sc := bufio.NewScanner(rf)
			sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
			ring := make([]model.LogEvent, s.capacity)
			totalRead := 0
			for sc.Scan() {
				var ev model.LogEvent
				if jErr := json.Unmarshal(sc.Bytes(), &ev); jErr != nil {
					continue
				}
				ring[totalRead%s.capacity] = ev
				totalRead++
			}
			scanErr := sc.Err()
			_ = rf.Close()
			if scanErr != nil {
				return nil, scanErr
			}
			if totalRead > 0 {
				var tail []model.LogEvent
				if totalRead <= s.capacity {
					tail = ring[:totalRead]
				} else {
					start := totalRead % s.capacity
					tail = make([]model.LogEvent, 0, s.capacity)
					tail = append(tail, ring[start:]...)
					tail = append(tail, ring[:start]...)
				}
				for i := range tail {
					if tail[i].ID != "" {
						s.hydratedIDs[tail[i].ID] = struct{}{}
					}
				}
				_ = s.insertBatch(tail, false)
			}
		} else if !os.IsNotExist(rErr) {
			return nil, rErr
		}
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
	return s.insertBatch(batch, true)
}

// InsertRecoveredBatch restores historical WAL events into the indexed ring buffer without duplicating NDJSON archival lines.
func (s *IndexedStore) InsertRecoveredBatch(batch []model.LogEvent) error {
	return s.insertBatch(batch, false)
}

func (s *IndexedStore) insertBatch(batch []model.LogEvent, writeNDJSON bool) error {
	if len(batch) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if writeNDJSON && s.ndjsonWriter != nil {
		for i := range batch {
			line, err := json.Marshal(batch[i])
			if err != nil {
				return err
			}
			if _, err := s.ndjsonWriter.Write(line); err != nil {
				s.ndjsonWriter.Reset(s.ndjsonFile)
				return err
			}
			if err := s.ndjsonWriter.WriteByte('\n'); err != nil {
				s.ndjsonWriter.Reset(s.ndjsonFile)
				return err
			}
		}
		if err := s.ndjsonWriter.Flush(); err != nil {
			s.ndjsonWriter.Reset(s.ndjsonFile)
			return err
		}
	}

	for i := range batch {
		ev := batch[i]
		if len(s.events) >= s.capacity {
			half := s.capacity / 2
			if half == 0 {
				half = 1
			}
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
	} else if limit > 1000 {
		limit = 1000
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

func (s *IndexedStore) Capacity() int {
	return s.capacity
}

func (s *IndexedStore) HasColdStorage() bool {
	return s.ndjsonWriter != nil
}

func (s *IndexedStore) HasHydratedID(id string) bool {
	if id == "" {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.hydratedIDs[id]
	return ok
}

func (s *IndexedStore) SnapshotEvents() []model.LogEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.LogEvent, len(s.events))
	copy(out, s.events)
	return out
}

func (s *IndexedStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ndjsonWriter != nil {
		if err := s.ndjsonWriter.Flush(); err != nil {
			s.ndjsonWriter.Reset(s.ndjsonFile)
		}
	}
	if s.ndjsonFile != nil {
		return s.ndjsonFile.Close()
	}
	return nil
}
