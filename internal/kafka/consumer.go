package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/example/stocker-informer/internal/messenger"
)

const (
	maxConsecutiveErrors = 10
	backoffBase          = 500 * time.Millisecond
	maxBackoff           = 30 * time.Second
	defaultCommitRetries = 3
)

// Consumer subscribes to a Kafka topic and dispatches messages to GoToSocial.
type Consumer struct {
	brokers        []string
	topic          string
	consumerGroup  string
	formatter      messenger.Formatter
	publisher      messenger.Publisher
	log            *slog.Logger
}

// New creates a new Kafka consumer that will subscribe to the given topic and broker list.
func New(brokers []string, topic, consumerGroup string, f messenger.Formatter, p messenger.Publisher, log *slog.Logger) *Consumer {
	return &Consumer{
		brokers:     brokers,
		topic:       topic,
		consumerGroup: consumerGroup,
		formatter:   f,
		publisher:   p,
		log:         log,
	}
}

// Run starts the Kafka consumer loop. It reads messages from the topic and publishes them to GoToSocial.
// It respects context cancellation for graceful shutdown.
func (c *Consumer) Run(ctx context.Context) error {
	c.log.Info("kafka consumer started", "brokers", c.brokers, "topic", c.topic, "group", c.consumerGroup)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:          c.brokers,
		Topic:            c.topic,
		GroupID:          c.consumerGroup,
		StartOffset:      kafka.FirstOffset,
		MaxBytes:         10_000_000,
		ReadBatchTimeout: 5 * time.Second,
		Logger:           discardLogger{},
		ErrorLogger: kafka.LoggerFunc(func(format string, args ...interface{}) {
			c.log.Error(fmt.Sprintf(format, args...))
		}),
	})

	consecutiveErrors := 0
	var backoffDelay time.Duration

	for {
		select {
		case <-ctx.Done():
			c.log.Info("kafka consumer shutting down")
			if err := reader.Close(); err != nil {
				return fmt.Errorf("close kafka reader: %w", err)
			}
			return nil
		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				consecutiveErrors++

				if backoffDelay == 0 {
					backoffDelay = backoffBase
				} else {
					backoffDelay *= 2
					if backoffDelay > maxBackoff {
						backoffDelay = maxBackoff
					}
				}

				if consecutiveErrors >= maxConsecutiveErrors {
					c.log.Error("exceeded max consecutive Kafka read failures", "count", consecutiveErrors, "backoff", backoffDelay)
				} else {
					c.log.Warn("read kafka message failed, backing off", "error", err, "retry", consecutiveErrors, "backoff", backoffDelay)
				}

				select {
				case <-time.After(backoffDelay):
					continue
				case <-ctx.Done():
					c.log.Info("kafka consumer shutting down during backoff")
					if err := reader.Close(); err != nil {
						return fmt.Errorf("close kafka reader: %w", err)
					}
					return nil
				}
			}

			consecutiveErrors = 0
			backoffDelay = 0

			event, err := c.decodeEvent(msg.Value)
			if err != nil {
				c.log.Error("decode event failed", "error", err, "offset", msg.Offset)
				c.commitWithRetries(ctx, reader, msg)
				continue
			}

			text := c.formatter.Format(event)
			if text == "" {
				c.log.Warn("formatted message is empty, skipping publish", "offset", msg.Offset)
				c.commitWithRetries(ctx, reader, msg)
				continue
			}

			if err := c.publisher.Publish(ctx, text); err != nil {
				c.log.Error("publish failed", "error", err, "offset", msg.Offset)
				c.commitWithRetries(ctx, reader, msg)
				continue
			}

			c.commitWithRetries(ctx, reader, msg)
			c.log.Info("event published on GoToSocial", "symbol", event.Symbol, "offset", msg.Offset)
		}
	}
}

func (c *Consumer) commitWithRetries(ctx context.Context, reader *kafka.Reader, msgs ...kafka.Message) {
	for i := 0; i < defaultCommitRetries; i++ {
		if err := reader.CommitMessages(ctx, msgs...); err != nil {
			c.log.Error("commit kafka offsets failed (attempt %d/%d)", "error", err, "attempt", i+1, "max_attempts", defaultCommitRetries)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return
	}
	c.log.Error("abandoning commit after max retries", "offsets", func() []int64 {
		offsets := make([]int64, len(msgs))
		for i, m := range msgs {
			offsets[i] = m.Offset
		}
		return offsets
	}())
}

func (c *Consumer) decodeEvent(data []byte) (*messenger.StockEvent, error) {
	var event messenger.StockEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("unmarshal stock event: %w", err)
	}
	if event.Symbol == "" {
		return nil, fmt.Errorf("stock event missing symbol field")
	}
	return &event, nil
}

type discardLogger struct{}

func (l discardLogger) Printf(format string, args ...interface{}) {
	// no-op
}
