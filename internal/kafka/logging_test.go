package kafka

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// decodeSingleLogRecord builds a debug-level JSON logger writing into buf,
// decodes the single emitted JSON line, and returns it as a generic map.
func decodeSingleLogRecord(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()

	line := strings.TrimSpace(buf.String())
	if line == "" {
		t.Fatal("expected a log record, got none")
	}
	if strings.Contains(line, "\n") {
		t.Fatalf("expected a single log record, got %d lines: %q", strings.Count(line, "\n")+1, line)
	}

	var record map[string]any
	if err := json.Unmarshal([]byte(line), &record); err != nil {
		t.Fatalf("decode log record %q: %v", line, err)
	}
	return record
}

func newJSONTestLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestClientLoggerDebug(t *testing.T) {
	var buf bytes.Buffer
	log := newJSONTestLogger(&buf)

	newClientLogger(log).Printf("joining group %s", "g")

	record := decodeSingleLogRecord(t, &buf)
	if got, want := record["level"], "DEBUG"; got != want {
		t.Errorf("level = %v, want %v", got, want)
	}
	if got, want := record["msg"], "kafka client"; got != want {
		t.Errorf("msg = %v, want %v", got, want)
	}
	if got, want := record["component"], "kafka"; got != want {
		t.Errorf("component = %v, want %v", got, want)
	}
	if got, want := record["detail"], "joining group g"; got != want {
		t.Errorf("detail = %v, want %v", got, want)
	}
}

func TestErrorLoggerWarn(t *testing.T) {
	var buf bytes.Buffer
	log := newJSONTestLogger(&buf)

	newErrorLogger(log).Printf("read tcp: %v", errors.New("connection reset by peer"))

	record := decodeSingleLogRecord(t, &buf)
	if got, want := record["level"], "WARN"; got != want {
		t.Errorf("level = %v, want %v", got, want)
	}
	if got, want := record["msg"], "kafka client error"; got != want {
		t.Errorf("msg = %v, want %v", got, want)
	}
	detail, ok := record["detail"].(string)
	if !ok {
		t.Fatalf("detail = %v, want a string", record["detail"])
	}
	if !strings.Contains(detail, "connection reset by peer") {
		t.Errorf("detail = %q, want it to contain %q", detail, "connection reset by peer")
	}
}

func TestClientLoggerNoArgs(t *testing.T) {
	var buf bytes.Buffer
	log := newJSONTestLogger(&buf)

	newClientLogger(log).Printf("hello")

	record := decodeSingleLogRecord(t, &buf)
	if got, want := record["detail"], "hello"; got != want {
		t.Errorf("detail = %v, want %v", got, want)
	}
}
