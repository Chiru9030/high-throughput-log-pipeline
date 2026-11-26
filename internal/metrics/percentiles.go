package metrics

import (
	"math"
	"sort"
	"sync"
	"time"
)

const (
	maxSamplesPerService = 10_000
	maxTrackedServices   = 10_000
)

type LatencySummary struct {
	Count float64 `json:"count"`
	P50   float64 `json:"p50_ms"`
	P90   float64 `json:"p90_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
	Avg   float64 `json:"avg_ms"`
}

type sampleEntry struct {
	ts  time.Time
	val float64
}

// PercentileAggregator tracks streaming latencies per service across a sliding window.
type PercentileAggregator struct {
	mu      sync.RWMutex
	samples map[string][]sampleEntry
	window  time.Duration
}

func NewPercentileAggregator(window time.Duration) *PercentileAggregator {
	if window <= 0 {
		window = time.Minute
	}
	return &PercentileAggregator{
		samples: make(map[string][]sampleEntry),
		window:  window,
	}
}

func (p *PercentileAggregator) Window() time.Duration {
	return p.window
}

// pruneSlice filters out all expired entries regardless of out-of-order ingestion order.
func (p *PercentileAggregator) pruneSlice(entries []sampleEntry, cutoff time.Time) []sampleEntry {
	if len(entries) == 0 {
		return nil
	}
	hasExpired := false
	for i := range entries {
		if entries[i].ts.Before(cutoff) {
			hasExpired = true
			break
		}
	}
	if !hasExpired {
		return entries
	}
	kept := make([]sampleEntry, 0, len(entries))
	for _, e := range entries {
		if !e.ts.Before(cutoff) {
			kept = append(kept, e)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}

func (p *PercentileAggregator) Record(service string, latencyMs float64) {
	p.RecordAt(service, latencyMs, time.Now())
}

func (p *PercentileAggregator) RecordAt(service string, latencyMs float64, ts time.Time) {
	if math.IsNaN(latencyMs) || math.IsInf(latencyMs, 0) || latencyMs <= 0 || service == "" {
		return
	}
	now := time.Now()
	// Clamp future-skewed timestamps so out-of-order events cannot pin sliding-window eviction.
	if ts.After(now) {
		ts = now
	}
	cutoff := now.Add(-p.window)
	if ts.Before(cutoff) {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.samples[service]; !exists && len(p.samples) >= maxTrackedServices {
		// Prune empty services before giving up on a new service key.
		for svc, raw := range p.samples {
			pruned := p.pruneSlice(raw, cutoff)
			if len(pruned) == 0 {
				delete(p.samples, svc)
			} else {
				p.samples[svc] = pruned
			}
		}
		if len(p.samples) >= maxTrackedServices {
			return
		}
	}

	entries := p.pruneSlice(p.samples[service], cutoff)
	if len(entries) >= maxSamplesPerService {
		trim := len(entries) - maxSamplesPerService + 1
		compacted := make([]sampleEntry, len(entries)-trim)
		copy(compacted, entries[trim:])
		entries = compacted
	}
	entries = append(entries, sampleEntry{ts: ts, val: latencyMs})
	p.samples[service] = entries
}

func (p *PercentileAggregator) Summary(service string) LatencySummary {
	p.mu.Lock()
	defer p.mu.Unlock()

	cutoff := time.Now().Add(-p.window)
	var values []float64

	if service == "" {
		for svc, raw := range p.samples {
			pruned := p.pruneSlice(raw, cutoff)
			if len(pruned) == 0 {
				delete(p.samples, svc)
				continue
			}
			p.samples[svc] = pruned
			for _, e := range pruned {
				values = append(values, e.val)
			}
		}
	} else {
		pruned := p.pruneSlice(p.samples[service], cutoff)
		if len(pruned) == 0 {
			delete(p.samples, service)
			return LatencySummary{}
		}
		p.samples[service] = pruned
		values = make([]float64, len(pruned))
		for i, e := range pruned {
			values[i] = e.val
		}
	}

	if len(values) == 0 {
		return LatencySummary{}
	}

	var sum float64
	for _, v := range values {
		sum += v
	}
	sort.Float64s(values)

	return LatencySummary{
		Count: float64(len(values)),
		P50:   percentile(values, 0.50),
		P90:   percentile(values, 0.90),
		P99:   percentile(values, 0.99),
		Max:   values[len(values)-1],
		Avg:   math.Round((sum/float64(len(values)))*100) / 100,
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	} else if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return math.Round(sorted[idx]*100) / 100
}
