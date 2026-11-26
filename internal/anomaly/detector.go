package anomaly

import (
	"math"
	"regexp"
	"sync"

	"github.com/Chiru9030/high-throughput-log-pipeline/internal/model"
)

const maxTrackedServices = 10_000

var (
	secretPattern   = regexp.MustCompile(`(?i)(sk_(?:live|test)_[a-zA-Z0-9]{8,}|Bearer\s+[a-zA-Z0-9._\-]{10,})`)
	kvSecretPattern = regexp.MustCompile(`(?i)\b(token|api_key|secret|password)=([a-zA-Z0-9._\-]{6,})`)
	cardPattern     = regexp.MustCompile(`\b(?:\d[ -]*?){13,16}\b`)
)

// passesLuhn validates 13-16 digit candidates so Unix timestamps and IDs are not mangled.
func passesLuhn(candidate string) bool {
	var digits [19]int
	n := 0
	for i := 0; i < len(candidate); i++ {
		c := candidate[i]
		if c >= '0' && c <= '9' {
			if n >= len(digits) {
				return false
			}
			digits[n] = int(c - '0')
			n++
		}
	}
	if n < 13 || n > 16 {
		return false
	}
	sum := 0
	alt := false
	for i := n - 1; i >= 0; i-- {
		d := digits[i]
		if alt {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		alt = !alt
	}
	return sum%10 == 0
}

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
	mu           sync.Mutex
	latencyStats map[string]*welfordStats
	zScoreCutoff float64
	minSamples   int64
}

func NewDetector(zScoreCutoff float64, minSamples int64) *Detector {
	if zScoreCutoff <= 0 {
		zScoreCutoff = 3.0
	}
	if minSamples <= 0 {
		minSamples = 20
	} else if minSamples < 2 {
		minSamples = 2
	}
	return &Detector{
		latencyStats: make(map[string]*welfordStats),
		zScoreCutoff: zScoreCutoff,
		minSamples:   minSamples,
	}
}

// Inspect redacts sensitive secrets/PANs across all event fields and flags anomalies (FATAL/5xx or statistical latency spikes).
func (d *Detector) Inspect(ev *model.LogEvent) {
	RedactEvent(ev)

	if ev.Level == model.LevelFatal || ev.StatusCode >= 500 {
		ev.IsAnomaly = true
		ev.AnomalyReason = "critical_error_or_5xx"
	}

	if ev.LatencyMs > 0 && !math.IsNaN(ev.LatencyMs) && !math.IsInf(ev.LatencyMs, 0) && ev.Service != "" {
		d.mu.Lock()
		st, ok := d.latencyStats[ev.Service]
		if !ok && len(d.latencyStats) < maxTrackedServices {
			st = &welfordStats{}
			d.latencyStats[ev.Service] = st
			ok = true
		}
		isSpike := false
		if ok {
			if st.count >= d.minSamples && st.count >= 2 {
				variance := st.m2 / float64(st.count-1)
				stddev := math.Sqrt(variance)
				if stddev > 0 {
					if (ev.LatencyMs-st.mean)/stddev > d.zScoreCutoff {
						isSpike = true
					}
				} else if st.mean > 0 && (ev.LatencyMs > st.mean*(1.0+d.zScoreCutoff) || ev.LatencyMs-st.mean > 50.0) {
					isSpike = true
				}
			}
			if !isSpike {
				st.update(ev.LatencyMs)
			}
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
	if s == "" {
		return s
	}
	s = secretPattern.ReplaceAllString(s, "[REDACTED_SECRET]")
	s = kvSecretPattern.ReplaceAllString(s, "$1=[REDACTED_SECRET]")
	s = cardPattern.ReplaceAllStringFunc(s, func(match string) string {
		if passesLuhn(match) {
			return "[REDACTED_PAN]"
		}
		return match
	})
	return s
}

// RedactEvent scrubs secrets and Luhn-valid PANs across Message, Service, TraceID, ID, and Attributes.
func RedactEvent(ev *model.LogEvent) {
	if ev == nil {
		return
	}
	ev.Message = RedactSensitive(ev.Message)
	ev.Service = RedactSensitive(ev.Service)
	ev.TraceID = RedactSensitive(ev.TraceID)
	ev.ID = RedactSensitive(ev.ID)
	if len(ev.Attributes) > 0 {
		clean := make(map[string]string, len(ev.Attributes))
		for k, v := range ev.Attributes {
			clean[k] = RedactSensitive(v)
		}
		ev.Attributes = clean
	}
}
