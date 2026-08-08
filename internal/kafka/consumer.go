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
func New(brokers, topic, consumerGroup string, f messenger.Formatter, p messenger.Publisher, log *slog.Logger) *Consumer {
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
		Brokers:        c.brokers,
		Topic:          c.topic,
		GroupID:        c.consumerGroup,
		StartOffset:    kafka.FirstOffset,
		MaxBytes:       10e6,
		ReadBatchTimeout: 5 * time.Second,
		Logger:         discardLogger{},
		ErrorLogger: kafka.LoggerFunc(func(keyvalues ...interface{}) {
			c.log.Error("kafka error", keyvalues...)
		}),
	})

	for {
		select {
		case <-ctx.Done():
			c.log.Info("kafka consumer shutting down")
			if err := reader.Close(); err != nil {
				return fmt.Errorf("close kafka reader: %w", err)
			}
			return ctx.Err()
		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					continue
				}
				c.log.Error("read kafka message failed", "error", err)
				continue
			}

			event, err := c.decodeEvent(msg.Value)
			if err != nil {
				c.log.Error("decode event failed", "error", err, "offset", msg.Offset)
				reader.CommitMessages(ctx, msg)
				continue
			}

			text := c.formatter.Format(event)
			if text == "" {
				c.log.Warn("formatted message is empty, skipping publish", "offset", msg.Offset)
				reader.CommitMessages(ctx, msg)
				continue
			}

			if err := c.publisher.Publish(ctx, text); err != nil {
				c.log.Error("publish failed", "error", err, "offset", msg.Offset)
				continue
			}

			reader.CommitMessages(ctx, msg)
			c.log.Info("event published on GoToSocial", "symbol", event.Symbol, "offset", msg.Offset)
		}
	}
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
