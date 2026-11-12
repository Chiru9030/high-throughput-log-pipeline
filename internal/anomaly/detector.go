package anomaly

import (
	"math"
	"regexp"
	"sync"

	"github.com/Chiru9030/high-throughput-log-pipeline/internal/model"
)

var (
	secretPattern = regexp.MustCompile(`(?i)(sk_(?:live|test)_[a-zA-Z0-9]{8,}|Bearer\s+[a-zA-Z0-9._\-]{10,})`)
	cardPattern   = regexp.MustCompile(`\b(?:\d[ -]*?){13,16}\b`)
)

// welfordStats tracks streaming mean and variance in O(1) memory with zero allocations.
type welfordStats struct {
	count int64
	mean  float64
	m2    float64
}

func (w *welfordStats) update(x float64) (mean, stddev float64, count int64) {
	w.count++
	delta := x - w.mean
	w.mean += delta / float64(w.count)
	delta2 := x - w.mean
	w.m2 += delta * delta2
	if w.count < 2 {
		return w.mean, 0, w.count
	}
	variance := w.m2 / float64(w.count-1)
	return w.mean, math.Sqrt(variance), w.count
}

type Detector struct {
	mu             sync.Mutex
	latencyStats   map[string]*welfordStats
	zScoreCutoff   float64
	minSamples     int64
}

func NewDetector(zScoreCutoff float64, minSamples int64) *Detector {
	if zScoreCutoff <= 0 {
		zScoreCutoff = 3.0
	}
	if minSamples <= 0 {
		minSamples = 20
	}
	return &Detector{
		latencyStats: make(map[string]*welfordStats),
		zScoreCutoff: zScoreCutoff,
		minSamples:   minSamples,
	}
}

// Inspect redacts sensitive secrets/PANs and flags anomalies (FATAL/5xx or statistical latency spikes).
func (d *Detector) Inspect(ev *model.LogEvent) {
	ev.Message = RedactSensitive(ev.Message)

	if ev.Level == model.LevelFatal || ev.StatusCode >= 500 {
		ev.IsAnomaly = true
		ev.AnomalyReason = "critical_error_or_5xx"
	}

	if ev.LatencyMs > 0 {
		d.mu.Lock()
		st, ok := d.latencyStats[ev.Service]
		if !ok {
			st = &welfordStats{}
			d.latencyStats[ev.Service] = st
		}
		// Compute z-score against historical distribution BEFORE folding extreme outlier in
		isSpike := false
		if st.count >= d.minSamples {
			variance := st.m2 / float64(st.count-1)
			stddev := math.Sqrt(variance)
			if stddev > 0 && (ev.LatencyMs-st.mean)/stddev > d.zScoreCutoff {
				isSpike = true
			}
		}
		if !isSpike {
			st.update(ev.LatencyMs)
		}
		d.mu.Unlock()

		if isSpike {
			ev.IsAnomaly = true
			if ev.AnomalyReason == "" {
				ev.AnomalyReason = "latency_zscore_spike"
			} else {
				ev.AnomalyReason += ",latency_zscore_spike"
			}
		}
	}
}

func RedactSensitive(s string) string {
	s = secretPattern.ReplaceAllString(s, "[REDACTED_SECRET]")
	s = cardPattern.ReplaceAllString(s, "[REDACTED_PAN]")
	return s
}

