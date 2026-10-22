package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	stockstorev1 "git.wheeli.ca/brian/stocker-store/proto/v1"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	"github.com/example/stocker-informer/internal/messenger"
	"github.com/example/stocker-informer/internal/summarystore"
)

const (
	maxConsecutiveErrors = 10
	backoffBase          = 500 * time.Millisecond
	maxBackoff           = 30 * time.Second
	defaultCommitRetries = 3
)

// SummaryReader reads the Markdown summary for a stock identity.
type SummaryReader interface {
	ReadSummary(ctx context.Context, exchange, symbol string) ([]byte, error)
}

// Consumer subscribes to a Kafka topic of summary-availability signals and
// publishes the referenced summaries to GoToSocial.
type Consumer struct {
	brokers       []string
	topic         string
	consumerGroup string
	summaries     SummaryReader
	publisher     messenger.Publisher
	log           *slog.Logger
}

// New creates a new Kafka consumer for the given topic and broker list.
func New(brokers []string, topic, consumerGroup string, summaries SummaryReader, publisher messenger.Publisher, log *slog.Logger) *Consumer {
	return &Consumer{
		brokers:       brokers,
		topic:         topic,
		consumerGroup: consumerGroup,
		summaries:     summaries,
		publisher:     publisher,
		log:           log,
	}
}

// Run starts the Kafka consumer loop. It reads messages from the topic and
// publishes the referenced summaries to GoToSocial. It respects context
// cancellation for graceful shutdown.
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

			c.handleMessage(ctx, msg, func(m kafka.Message) {
				c.commitWithRetries(ctx, reader, m)
			})
		}
	}
}

// handleMessage decodes a stock signal, reads its summary, and publishes it.
// Every outcome commits the offset: the pipeline is at-most-once after the
// publisher's own retries. The summary body is never logged.
func (c *Consumer) handleMessage(ctx context.Context, msg kafka.Message, commit func(kafka.Message)) {
	identity, err := decodeStock(msg.Value)
	if err != nil {
		c.log.Error("decode stock failed", "error", err, "offset", msg.Offset)
		commit(msg)
		return
	}

	summary, err := c.summaries.ReadSummary(ctx, identity.Exchange, identity.Symbol)
	if err != nil {
		if errors.Is(err, summarystore.ErrNotFound) {
			c.log.Warn("summary not found", "symbol", identity.Symbol, "exchange", identity.Exchange, "offset", msg.Offset)
		} else {
			c.log.Error("read summary failed", "error", err, "symbol", identity.Symbol, "exchange", identity.Exchange, "offset", msg.Offset)
		}
		commit(msg)
		return
	}

	text := strings.TrimSpace(string(summary))
	if text == "" {
		c.log.Warn("summary is empty, skipping publish", "symbol", identity.Symbol, "exchange", identity.Exchange, "offset", msg.Offset)
		commit(msg)
		return
	}

	if err := c.publisher.Publish(ctx, text); err != nil {
		c.log.Error("publish failed", "error", err, "symbol", identity.Symbol, "exchange", identity.Exchange, "offset", msg.Offset)
		commit(msg)
		return
	}

	commit(msg)
	c.log.Info("summary published on GoToSocial", "symbol", identity.Symbol, "exchange", identity.Exchange, "offset", msg.Offset)
}

// offsetCommitter commits consumed offsets. *kafka.Reader satisfies it; the
// interface keeps the retry and abandon behaviour testable without a broker.
type offsetCommitter interface {
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

func (c *Consumer) commitWithRetries(ctx context.Context, committer offsetCommitter, msgs ...kafka.Message) {
	for i := 0; i < defaultCommitRetries; i++ {
		if err := committer.CommitMessages(ctx, msgs...); err != nil {
			c.log.Error("commit kafka offsets failed", "error", err, "attempt", i+1, "max_attempts", defaultCommitRetries)
			if i < defaultCommitRetries-1 {
				time.Sleep(100 * time.Millisecond)
			}
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

// stockIdentity is the part of a stock signal needed to locate its summary.
type stockIdentity struct {
	Symbol   string
	Exchange string
}

// decodeStock decodes a protobuf stockstore.v1.Stock record. Only symbol and
// exchange matter; scores, key, and headers are ignored.
func decodeStock(data []byte) (stockIdentity, error) {
	stock := &stockstorev1.Stock{}
	if err := proto.Unmarshal(data, stock); err != nil {
		return stockIdentity{}, fmt.Errorf("unmarshal stock protobuf: %w", err)
	}

	identity := stockIdentity{
		Symbol:   strings.TrimSpace(stock.GetSymbol()),
		Exchange: strings.TrimSpace(stock.GetExchange()),
	}
	if identity.Symbol == "" {
		return stockIdentity{}, errors.New("stock missing symbol field")
	}
	if identity.Exchange == "" {
		return stockIdentity{}, errors.New("stock missing exchange field")
	}
	return identity, nil
}

type discardLogger struct{}

func (l discardLogger) Printf(format string, args ...interface{}) {
	// no-op
}
