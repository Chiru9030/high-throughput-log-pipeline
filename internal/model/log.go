package model

import "time"

type LogLevel string

const (
	LevelDebug LogLevel = "DEBUG"
	LevelInfo  LogLevel = "INFO"
	LevelWarn  LogLevel = "WARN"
	LevelError LogLevel = "ERROR"
	LevelFatal LogLevel = "FATAL"
)

type LogEvent struct {
	ID            string            `json:"id,omitempty"`
	Timestamp     time.Time         `json:"timestamp"`
	Service       string            `json:"service"`
	Level         LogLevel          `json:"level"`
	Message       string            `json:"message"`
	LatencyMs     float64           `json:"latency_ms,omitempty"`
	StatusCode    int               `json:"status_code,omitempty"`
	TraceID       string            `json:"trace_id,omitempty"`
	Attributes    map[string]string `json:"attributes,omitempty"`
	IsAnomaly     bool              `json:"is_anomaly,omitempty"`
	AnomalyReason string            `json:"anomaly_reason,omitempty"`
}

type QueryFilter struct {
	Service     string
	Level       LogLevel
	OnlyAnomaly bool
	Since       time.Time
	Limit       int
}

