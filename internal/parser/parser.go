package parser

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/Chiru9030/high-throughput-log-pipeline/internal/model"
)

// ParseLogfmt parses a key=value logfmt line into a LogEvent.
// Example: service=payment level=info latency_ms=14.2 msg="charge authorized"
func ParseLogfmt(line string) (model.LogEvent, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return model.LogEvent{}, false
	}

	ev := model.LogEvent{
		Timestamp:  time.Now().UTC(),
		Level:      model.LevelInfo,
		Attributes: make(map[string]string),
	}

	parts, ok := splitLogfmt(line)
	if !ok {
		return model.LogEvent{}, false
	}
	for k, v := range parts {
		switch strings.ToLower(k) {
		case "service", "app":
			ev.Service = strings.TrimSpace(v)
		case "level", "lvl":
			lvl := model.LogLevel(strings.ToUpper(strings.TrimSpace(v)))
			switch lvl {
			case model.LevelDebug, model.LevelInfo, model.LevelWarn, model.LevelError, model.LevelFatal:
				ev.Level = lvl
			default:
				return model.LogEvent{}, false
			}
		case "msg", "message":
			ev.Message = strings.TrimSpace(v)
		case "latency_ms", "latency", "duration_ms":
			f, err := strconv.ParseFloat(v, 64)
			if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 0 {
				return model.LogEvent{}, false
			}
			ev.LatencyMs = f
		case "status_code", "status":
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 || n > 599 {
				return model.LogEvent{}, false
			}
			ev.StatusCode = n
		case "trace_id", "traceid":
			ev.TraceID = v
		case "ts", "timestamp", "time":
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return model.LogEvent{}, false
			}
			ev.Timestamp = t
		default:
			ev.Attributes[k] = v
		}
	}

	if ev.Service == "" || ev.Message == "" {
		return model.LogEvent{}, false
	}
	return ev, true
}

func splitLogfmt(s string) (map[string]string, bool) {
	res := make(map[string]string)
	inQuote := false
	escaped := false
	var key, val strings.Builder
	isKey := true

	for i := 0; i < len(s); i++ {
		c := s[i]
		if escaped {
			if isKey {
				key.WriteByte(c)
			} else {
				val.WriteByte(c)
			}
			escaped = false
			continue
		}
		if c == '\\' && inQuote {
			escaped = true
			continue
		}
		if c == '"' {
			inQuote = !inQuote
			continue
		}
		if c == '=' && isKey && !inQuote {
			isKey = false
			continue
		}
		if (c == ' ' || c == '\t') && !inQuote {
			if key.Len() > 0 {
				res[key.String()] = val.String()
				key.Reset()
				val.Reset()
				isKey = true
			}
			continue
		}
		if isKey {
			key.WriteByte(c)
		} else {
			val.WriteByte(c)
		}
	}
	if inQuote || escaped {
		return nil, false
	}
	if key.Len() > 0 {
		res[key.String()] = val.String()
	}
	return res, true
}

// ParseSyslogRFC5424 parses an RFC 5424 formatted line:
// <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID STRUCTURED-DATA MSG
func ParseSyslogRFC5424(line string) (model.LogEvent, bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "<") {
		return model.LogEvent{}, false
	}

	priEnd := strings.IndexByte(line, '>')
	if priEnd <= 1 || len(line) <= priEnd+1 {
		return model.LogEvent{}, false
	}

	pri, err := strconv.Atoi(line[1:priEnd])
	if err != nil || pri < 0 || pri > 191 {
		return model.LogEvent{}, false
	}

	var level model.LogLevel
	switch pri % 8 {
	case 0, 1, 2:
		level = model.LevelFatal
	case 3:
		level = model.LevelError
	case 4:
		level = model.LevelWarn
	case 7:
		level = model.LevelDebug
	default:
		level = model.LevelInfo
	}

	remainder := strings.TrimSpace(line[priEnd+1:])
	// Parse first 6 space-delimited header fields strictly before scanning STRUCTURED-DATA.
	var hdr [6]string
	pos := 0
	for fieldIdx := 0; fieldIdx < 6; fieldIdx++ {
		for pos < len(remainder) && (remainder[pos] == ' ' || remainder[pos] == '\t') {
			pos++
		}
		if pos >= len(remainder) {
			return model.LogEvent{}, false
		}
		start := pos
		for pos < len(remainder) && remainder[pos] != ' ' && remainder[pos] != '\t' {
			pos++
		}
		hdr[fieldIdx] = remainder[start:pos]
	}

	if hdr[0] != "1" {
		return model.LogEvent{}, false
	}

	ts := time.Now().UTC()
	if hdr[1] != "-" {
		parsedTS, err := time.Parse(time.RFC3339, hdr[1])
		if err != nil {
			return model.LogEvent{}, false
		}
		ts = parsedTS
	}
	service := strings.TrimSpace(hdr[3])
	if service == "" || service == "-" {
		return model.LogEvent{}, false
	}

	for pos < len(remainder) && (remainder[pos] == ' ' || remainder[pos] == '\t') {
		pos++
	}

	// Scan RFC 5424 STRUCTURED-DATA: "-" OR one/more bracketed [SD-ID ...] blocks with quoted/escaped chars.
	if pos < len(remainder) {
		if remainder[pos] == '-' && (pos+1 == len(remainder) || remainder[pos+1] == ' ' || remainder[pos+1] == '\t') {
			pos++
		} else if remainder[pos] == '[' {
			for pos < len(remainder) && remainder[pos] == '[' {
				pos++
				inQuote := false
				esc := false
				closed := false
				for pos < len(remainder) {
					c := remainder[pos]
					pos++
					if esc {
						esc = false
						continue
					}
					if c == '\\' {
						esc = true
						continue
					}
					if c == '"' {
						inQuote = !inQuote
						continue
					}
					if c == ']' && !inQuote {
						closed = true
						break
					}
				}
				if !closed {
					return model.LogEvent{}, false
				}
			}
		}
	}

	msg := strings.TrimSpace(remainder[pos:])
	if msg == "" {
		msg = hdr[5]
	}
	if msg == "" || msg == "-" {
		return model.LogEvent{}, false
	}

	return model.LogEvent{
		Timestamp: ts,
		Service:   service,
		Level:     level,
		Message:   msg,
	}, true
}
