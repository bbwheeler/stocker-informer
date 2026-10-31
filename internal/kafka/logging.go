package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

// clientLogger adapts a *slog.Logger to kafka.Logger so kafka-go's internal
// client, coordinator, and rebalance messages flow through the application's
// structured logger instead of being discarded or logged as fatal errors.
type clientLogger struct {
	log   *slog.Logger
	level slog.Level
	msg   string
}

var _ kafka.Logger = clientLogger{}

// newClientLogger returns a clientLogger that emits kafka-go's routine client
// trace at debug level.
func newClientLogger(log *slog.Logger) clientLogger {
	return clientLogger{log: log, level: slog.LevelDebug, msg: "kafka client"}
}

// newErrorLogger returns a clientLogger that emits kafka-go's library errors
// at warn level; routine transient coordinator errors are not fatal.
func newErrorLogger(log *slog.Logger) clientLogger {
	return clientLogger{log: log, level: slog.LevelWarn, msg: "kafka client error"}
}

// Printf implements kafka.Logger.
func (l clientLogger) Printf(format string, args ...any) {
	l.log.Log(context.Background(), l.level, l.msg, "component", "kafka", "detail", fmt.Sprintf(format, args...))
}
