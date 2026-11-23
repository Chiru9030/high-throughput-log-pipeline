package parser

import (
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

	parts := splitLogfmt(line)
	for k, v := range parts {
		switch strings.ToLower(k) {
		case "service", "app":
			ev.Service = v
		case "level", "lvl":
			ev.Level = model.LogLevel(strings.ToUpper(v))
		case "msg", "message":
			ev.Message = v
		case "latency_ms", "latency", "duration_ms":
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				ev.LatencyMs = f
			}
		case "status_code", "status":
			if n, err := strconv.Atoi(v); err == nil {
				ev.StatusCode = n
			}
		case "trace_id", "traceid":
			ev.TraceID = v
		case "ts", "timestamp", "time":
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				ev.Timestamp = t
			}
		default:
			ev.Attributes[k] = v
		}
	}

	if ev.Service == "" || ev.Message == "" {
		return model.LogEvent{}, false
	}
	return ev, true
}

func splitLogfmt(s string) map[string]string {
	res := make(map[string]string)
	inQuote := false
	var key, val strings.Builder
	isKey := true

	for i := 0; i < len(s); i++ {
		c := s[i]
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
	if key.Len() > 0 {
		res[key.String()] = val.String()
	}
	return res
}

// ParseSyslogRFC5424 parses an RFC 5424 formatted line:
// <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID STRUCTURED-DATA MSG
func ParseSyslogRFC5424(line string) (model.LogEvent, bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "<") {
		return model.LogEvent{}, false
	}

	priEnd := strings.IndexByte(line, '>')
	if priEnd == -1 || len(line) <= priEnd+1 {
		return model.LogEvent{}, false
	}

	remainder := line[priEnd+1:]
	tokens := strings.Fields(remainder)
	if len(tokens) < 6 {
		return model.LogEvent{}, false
	}

	// tokens: [version, timestamp, hostname, app-name, procid, msgid, ...msg]
	ts, err := time.Parse(time.RFC3339, tokens[1])
	if err != nil {
		ts = time.Now().UTC()
	}
	service := tokens[3]
	msg := strings.Join(tokens[6:], " ")
	if msg == "" {
		msg = tokens[5]
	}

	return model.LogEvent{
		Timestamp: ts,
		Service:   service,
		Level:     model.LevelInfo,
		Message:   msg,
	}, true
}
