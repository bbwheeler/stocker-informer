package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// ErrTopicNotFound reports that at least one broker answered the metadata
// request but none of them knows the configured topic.
var ErrTopicNotFound = errors.New("kafka topic not found")

const (
	defaultAttemptTimeout = 5 * time.Second
	defaultRetryInitial   = 1 * time.Second
	defaultRetryMax       = 30 * time.Second
	defaultHealthyEvery   = 60 * time.Second
)

// metadataConn is the subset of *kafka.Conn the probe depends on.
type metadataConn interface {
	ReadPartitions(topics ...string) ([]kafka.Partition, error)
	Close() error
}

// dialFunc opens a metadata connection to a single broker.
type dialFunc func(ctx context.Context, addr string) (metadataConn, error)

// Probe periodically verifies that the Kafka cluster is reachable and that the
// configured topic exists, so readiness reflects real connectivity.
type Probe struct {
	brokers []string
	topic   string
	log     *slog.Logger
	dial    dialFunc

	attemptTimeout time.Duration // default 5s
	retryInitial   time.Duration // default 1s
	retryMax       time.Duration // default 30s
	healthyEvery   time.Duration // default 60s
}

// NewProbe returns a Probe with production defaults and a kafka.DialContext
// based dialer; the dial func and timings are injectable for tests.
func NewProbe(brokers []string, topic string, log *slog.Logger) *Probe {
	return &Probe{
		brokers:        brokers,
		topic:          topic,
		log:            log,
		attemptTimeout: defaultAttemptTimeout,
		retryInitial:   defaultRetryInitial,
		retryMax:       defaultRetryMax,
		healthyEvery:   defaultHealthyEvery,
		dial: func(ctx context.Context, addr string) (metadataConn, error) {
			return kafka.DialContext(ctx, "tcp", addr)
		},
	}
}

// Check dials the brokers in order and returns nil once one of them reports at
// least one partition for the topic. It logs nothing.
func (p *Probe) Check(ctx context.Context) error {
	if len(p.brokers) == 0 {
		return fmt.Errorf("kafka probe: empty broker list %v", p.brokers)
	}

	var (
		topicNotFoundErr error
		transportErrs    []error
	)

	for _, broker := range p.brokers {
		partitions, err := p.readPartitions(ctx, broker)
		switch {
		case err == nil && len(partitions) > 0:
			return nil
		case err == nil || errors.Is(err, kafka.UnknownTopicOrPartition):
			if topicNotFoundErr == nil {
				topicNotFoundErr = fmt.Errorf("%w: %q", ErrTopicNotFound, p.topic)
			}
		default:
			transportErrs = append(transportErrs, fmt.Errorf("%s: %w", broker, err))
		}
	}

	// A broker that answered is a stronger signal than dial failures, but any
	// reachable broker knowing the topic (checked above) wins over both.
	if topicNotFoundErr != nil {
		return topicNotFoundErr
	}
	return errors.Join(transportErrs...)
}

// readPartitions dials one broker, reads the topic's partition metadata, and
// closes the connection; the attempt timeout applies to the whole exchange.
func (p *Probe) readPartitions(ctx context.Context, broker string) ([]kafka.Partition, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, p.attemptTimeout)
	defer cancel()

	conn, err := p.dial(attemptCtx, broker)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return conn.ReadPartitions(p.topic)
}

// Run supervises the probe until ctx is cancelled, logging connectivity
// transitions and calling onReady (at most once) on the first success. It is
// meant to be started in a goroutine by the composition root.
func (p *Probe) Run(ctx context.Context, onReady func()) {
	var (
		ready      bool
		lost       bool
		attempts   int
		retryDelay = p.retryInitial
	)

	for {
		if ctx.Err() != nil {
			return
		}

		if err := p.Check(ctx); err != nil {
			switch {
			case !ready:
				attempts++
				level := slog.LevelDebug
				if attempts == 1 || attempts%10 == 0 {
					level = slog.LevelWarn
				}
				msg := "kafka connectivity probe failed"
				if errors.Is(err, ErrTopicNotFound) {
					msg = "kafka topic not available"
				}
				p.log.Log(ctx, level, msg,
					"topic", p.topic,
					"brokers", p.brokers,
					"attempt", attempts,
					"retry_in", retryDelay,
					"error", err,
				)
			case !lost:
				lost = true
				p.log.Warn("kafka connectivity lost", "topic", p.topic, "error", err)
			default:
				p.log.Debug("kafka connectivity probe failed", "topic", p.topic, "error", err)
			}

			if !sleepCtx(ctx, retryDelay) {
				return
			}
			retryDelay = min(retryDelay*2, p.retryMax)
			continue
		}

		switch {
		case !ready:
			ready = true
			p.log.Info("kafka connectivity verified", "topic", p.topic, "brokers", p.brokers)
			if onReady != nil {
				onReady()
			}
		case lost:
			lost = false
			p.log.Info("kafka connectivity restored", "topic", p.topic)
		}

		retryDelay = p.retryInitial
		if !sleepCtx(ctx, p.healthyEvery) {
			return
		}
	}
}

// sleepCtx waits for d or until ctx ends, reporting whether the full delay
// elapsed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
