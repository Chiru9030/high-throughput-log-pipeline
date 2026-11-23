package metrics

import (
	"math"
	"sort"
	"sync"
	"time"
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

func (p *PercentileAggregator) Record(service string, latencyMs float64) {
	if latencyMs <= 0 || service == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.samples[service] = append(p.samples[service], sampleEntry{
		ts:  time.Now(),
		val: latencyMs,
	})
}

func (p *PercentileAggregator) Summary(service string) LatencySummary {
	p.mu.Lock()
	defer p.mu.Unlock()

	cutoff := time.Now().Add(-p.window)
	entries := p.samples[service]

	// Prune outside window
	idx := 0
	for idx < len(entries) && entries[idx].ts.Before(cutoff) {
		idx++
	}
	entries = entries[idx:]
	p.samples[service] = entries

	if len(entries) == 0 {
		return LatencySummary{}
	}

	values := make([]float64, len(entries))
	var sum float64
	for i, e := range entries {
		values[i] = e.val
		sum += e.val
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
	idx := int(math.Floor(p * float64(len(sorted)-1)))
	return math.Round(sorted[idx]*100) / 100
}
