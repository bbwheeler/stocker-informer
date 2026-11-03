// Package rabbitmq consumes protobuf stockstore.v1.Stock summary signals from
// a RabbitMQ queue, reads each stock's Markdown summary, optionally rewrites
// it, and publishes the result to GoToSocial.
//
// The consumer owns the durable input exchange, queue, and binding and
// declares them on every (re)connect. amqp091-go does not reconnect on its
// own, so Run supervises the whole session — dial, channel, topology, QoS,
// consume — with a deterministic 1s→30s exponential backoff that ends only
// when the run context is cancelled.
//
// Settlement preserves the pipeline's at-most-once behaviour after
// publication: every completed outcome is acked exactly once so a poison
// message or a permanently failing stock cannot stall the queue. The single
// exception is a delivery interrupted by shutdown, which is nacked with
// requeue instead of acked. Message bodies, summaries, reports, and the raw
// broker URL are never logged.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	stockstorev1 "git.wheeli.ca/brian/stocker-store/proto/v1"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	"github.com/example/stocker-informer/internal/config"
	"github.com/example/stocker-informer/internal/messenger"
	"github.com/example/stocker-informer/internal/rewriter"
	"github.com/example/stocker-informer/internal/summarystore"
)

const (
	// backoffInitial and backoffMax bound the deterministic reconnect
	// backoff: 1s, 2s, 4s, ... capped at 30s.
	backoffInitial = time.Second
	backoffMax     = 30 * time.Second

	// consumerTagPrefix forms the per-process consumer tag with the pid.
	consumerTagPrefix = "stocker-informer-"
)

// SummaryReader reads the Markdown summary for a stock identity.
type SummaryReader interface {
	ReadSummary(ctx context.Context, exchange, symbol string) ([]byte, error)
}

// Consumer reads summary-availability signals from a RabbitMQ queue and
// publishes the referenced summaries to GoToSocial.
type Consumer struct {
	cfg config.RabbitMQConfig

	summaries SummaryReader
	rewriter  rewriter.Rewriter
	publisher messenger.Publisher
	log       *slog.Logger

	dial  dialFunc
	sleep func(ctx context.Context, d time.Duration) error
}

// New creates a RabbitMQ consumer for the given transport settings. A nil
// rewriter means summaries are published unchanged. New performs no network
// I/O; Run dials the broker. A nil logger falls back to slog.Default().
func New(cfg config.RabbitMQConfig, summaries SummaryReader, summaryRewriter rewriter.Rewriter, publisher messenger.Publisher, log *slog.Logger) *Consumer {
	return newConsumer(cfg, summaries, summaryRewriter, publisher, log, dialAMQP, sleepContext)
}

// newConsumer is the injectable constructor used by tests. The dial and sleep
// seams are the only ways to reach a broker or wall-clock time, so passing
// fakes keeps the tests offline and instantaneous.
func newConsumer(
	cfg config.RabbitMQConfig,
	summaries SummaryReader,
	summaryRewriter rewriter.Rewriter,
	publisher messenger.Publisher,
	log *slog.Logger,
	dial dialFunc,
	sleep func(ctx context.Context, d time.Duration) error,
) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	if dial == nil {
		dial = dialAMQP
	}
	if sleep == nil {
		sleep = sleepContext
	}
	return &Consumer{
		cfg:       cfg,
		summaries: summaries,
		rewriter:  summaryRewriter,
		publisher: publisher,
		log:       log,
		dial:      dial,
		sleep:     sleep,
	}
}

// Run connects, consumes, and reconnects until ctx is cancelled. Broker
// failures are always retried with a deterministic 1s→30s exponential backoff;
// only context cancellation ends Run, so every normal path returns nil. The
// failure counter resets after a session that delivered at least one message,
// so a long healthy connection reconnects after 1s instead of inheriting an
// accumulated delay.
func (c *Consumer) Run(ctx context.Context) error {
	c.log.Info("rabbitmq consumer started", c.topologyAttrs()...)

	failures := 0
	connected := false
	for {
		if ctx.Err() != nil {
			c.log.Info("rabbitmq consumer shutting down")
			return nil
		}

		sess, err := c.connect(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.log.Info("rabbitmq consumer shutting down")
				return nil
			}
			failures++
			delay := backoffDelay(failures)
			attrs := []any{"attempt", failures, "retry_in", delay, "error", err}
			// The first failure and every tenth are loud; the rest are
			// expected background noise while a broker restarts.
			if failures == 1 || failures%10 == 0 {
				c.log.Warn("rabbitmq connect failed, backing off", attrs...)
			} else {
				c.log.Debug("rabbitmq connect failed, backing off", attrs...)
			}
			if err := c.sleep(ctx, delay); err != nil {
				c.log.Info("rabbitmq consumer shutting down during backoff")
				return nil
			}
			continue
		}

		if ctx.Err() != nil {
			// Shutdown won the connect race: release the new session
			// rather than leaking it.
			_ = sess.close()
			c.log.Info("rabbitmq consumer shutting down")
			return nil
		}

		if connected {
			c.log.Info("rabbitmq consumer reconnected", "attempts", failures)
		} else {
			c.log.Info("rabbitmq consumer connected", c.topologyAttrs()...)
			connected = true
		}

		delivered, consumeErr := c.consume(ctx, sess)
		if ctx.Err() != nil {
			// The session was already cancelled and drained inside
			// consume; release it and stop.
			_ = sess.close()
			c.log.Info("rabbitmq consumer shutting down")
			return nil
		}

		// A closed deliveries channel means the session died underneath
		// us: log, tear it down, and reconnect with backoff.
		c.log.Warn("rabbitmq consumer connection lost", "error", consumeErr)
		_ = sess.close()
		if delivered {
			// A productive session must not inherit the previous
			// failure count: reconnect after a long healthy run
			// starts at 1s.
			failures = 0
		}
		failures++
		if err := c.sleep(ctx, backoffDelay(failures)); err != nil {
			c.log.Info("rabbitmq consumer shutting down during backoff")
			return nil
		}
	}
}

// connect dials the broker, opens a channel, declares the durable input
// topology, sets QoS, and starts consuming with manual acknowledgements. Any
// failure after the dial closes both the channel and the connection before
// returning.
func (c *Consumer) connect(ctx context.Context) (*session, error) {
	conn, err := c.dial(ctx, c.cfg.URL, c.cfg.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	fail := func(err error) (*session, error) {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	if err := ch.ExchangeDeclare(c.cfg.InputExchange, c.cfg.InputExchangeType, true, false, false, false, nil); err != nil {
		return fail(err)
	}
	if _, err := ch.QueueDeclare(c.cfg.Queue, true, false, false, false, nil); err != nil {
		return fail(err)
	}
	if err := ch.QueueBind(c.cfg.Queue, c.cfg.InputRoutingKey, c.cfg.InputExchange, false, nil); err != nil {
		return fail(err)
	}
	if err := ch.Qos(c.cfg.Prefetch, 0, false); err != nil {
		return fail(err)
	}

	tag := fmt.Sprintf("%s%d", consumerTagPrefix, os.Getpid())
	deliveries, err := ch.Consume(c.cfg.Queue, tag, false, false, false, false, nil)
	if err != nil {
		return fail(err)
	}
	return &session{conn: conn, ch: ch, deliveries: deliveries, tag: tag}, nil
}

// consume reads deliveries until ctx is cancelled or the deliveries channel
// closes. It reports whether at least one delivery arrived, so Run can reset
// the failure counter, and returns nil error on the shutdown path.
func (c *Consumer) consume(ctx context.Context, sess *session) (bool, error) {
	delivered := false
	for {
		if ctx.Err() != nil {
			sess.cancelConsumer()
			c.drainDeliveries(sess)
			return delivered, nil
		}
		select {
		case d, ok := <-sess.deliveries:
			if !ok {
				return delivered, errSessionClosed
			}
			delivered = true
			c.handleDelivery(ctx, sess, d)
		case <-ctx.Done():
			sess.cancelConsumer()
			c.drainDeliveries(sess)
			return delivered, nil
		}
	}
}

// drainDeliveries requeues prefetched but unhandled deliveries without
// blocking. Anything still in the broker's buffer is requeued by the broker
// itself when the channel closes.
func (c *Consumer) drainDeliveries(sess *session) {
	for {
		select {
		case d, ok := <-sess.deliveries:
			if !ok {
				return
			}
			if err := sess.nack(d.DeliveryTag, true); err != nil {
				c.logSettlementFailed(d, err)
			}
		default:
			return
		}
	}
}

// handleDelivery runs the full pipeline for one delivery and settles it
// exactly once: an ack for every completed outcome, a requeueing nack when
// shutdown interrupts the work. The summary and report bodies are never
// logged.
func (c *Consumer) handleDelivery(ctx context.Context, sess *session, d amqp.Delivery) {
	if ctx.Err() != nil {
		// Shutdown started before this delivery was processed: requeue
		// it instead of starting work the process is about to abandon.
		c.settle(ctx, sess, d)
		return
	}

	attrs := deliveryAttrs(d)

	identity, err := decodeStock(d.Body)
	if err != nil {
		c.log.Error("decode stock failed", append(attrs, "error", err)...)
		c.settle(ctx, sess, d)
		return
	}
	attrs = append(attrs, "symbol", identity.Symbol, "exchange", identity.Exchange)

	summary, err := c.summaries.ReadSummary(ctx, identity.Exchange, identity.Symbol)
	if err != nil {
		if errors.Is(err, summarystore.ErrNotFound) {
			c.log.Warn("summary not found", attrs...)
		} else {
			c.log.Error("read summary failed", append(attrs, "error", err)...)
		}
		c.settle(ctx, sess, d)
		return
	}

	text := strings.TrimSpace(string(summary))
	if text == "" {
		c.log.Warn("summary is empty, skipping publish", attrs...)
		c.settle(ctx, sess, d)
		return
	}

	publishText := text
	rewritten := false
	if c.rewriter != nil {
		report, rwErr := c.rewriter.Rewrite(ctx, text)
		switch {
		case rwErr != nil:
			c.log.Warn("rewrite failed, publishing original summary",
				append(attrs,
					"error", rwErr,
					"summary_chars", utf8.RuneCountInString(text),
				)...)
		case strings.TrimSpace(report) == "":
			c.log.Warn("rewrite returned empty output, publishing original summary", attrs...)
		default:
			publishText = strings.TrimSpace(report)
			rewritten = true
		}
	}

	if pubErr := c.publisher.Publish(ctx, publishText); pubErr != nil {
		c.log.Error("publish failed", append(attrs, "error", pubErr)...)
		c.settle(ctx, sess, d)
		return
	}

	// Settle before the success line: a dead channel requeues the delivery
	// (a possible duplicate), while the publication below already happened.
	c.settle(ctx, sess, d)

	fields := append(attrs, "rewritten", rewritten)
	if rewritten {
		fields = append(fields,
			"summary_chars", utf8.RuneCountInString(text),
			"report_chars", utf8.RuneCountInString(publishText),
		)
	}
	c.log.Info("summary published on GoToSocial", fields...)
}

// settle acknowledges a completed outcome exactly once, unless shutdown began
// first, in which case the delivery is requeued so the stock is not lost. A
// settlement failure is logged and never retried: delivery tags are
// channel-scoped, so a tag from a dead channel is meaningless on a
// replacement channel; the broker requeues whatever was never settled.
func (c *Consumer) settle(ctx context.Context, sess *session, d amqp.Delivery) {
	if ctx.Err() != nil {
		if err := sess.nack(d.DeliveryTag, true); err != nil {
			c.logSettlementFailed(d, err)
		}
		return
	}
	if err := sess.ack(d.DeliveryTag); err != nil {
		c.logSettlementFailed(d, err)
	}
}

// logSettlementFailed reports a failed ack or nack. The delivery tag died
// with its channel, so the failure is never retried on a replacement one.
func (c *Consumer) logSettlementFailed(d amqp.Delivery, err error) {
	c.log.Warn("ack delivery failed", append(deliveryAttrs(d), "error", err)...)
}

// deliveryAttrs are the stable per-message fields used by every log event
// that describes a delivery. exchange is the stock exchange added by
// handleDelivery after the decode, never the AMQP exchange.
func deliveryAttrs(d amqp.Delivery) []any {
	return []any{
		"routing_key", d.RoutingKey,
		"delivery_tag", d.DeliveryTag,
		"redelivered", d.Redelivered,
	}
}

// topologyAttrs are the fields of the startup and connect log lines. The
// broker URL is redacted: RABBITMQ_URL carries credentials.
func (c *Consumer) topologyAttrs() []any {
	return []any{
		"broker", config.RedactedAMQPURL(c.cfg.URL),
		"exchange", c.cfg.InputExchange,
		"exchange_type", c.cfg.InputExchangeType,
		"routing_key", c.cfg.InputRoutingKey,
		"queue", c.cfg.Queue,
		"prefetch", c.cfg.Prefetch,
	}
}

// backoffDelay returns the deterministic exponential delay for the given
// consecutive failure count: 1s, 2s, 4s, 8s, 16s, then capped at 30s.
func backoffDelay(failures int) time.Duration {
	if failures < 2 {
		return backoffInitial
	}
	if failures >= 6 {
		// 1s << 5 is already 32s, past the cap.
		return backoffMax
	}
	return backoffInitial << (failures - 1)
}

// sleepContext waits for d or returns ctx.Err() when the context ends first.
// Backoff sleeps never use time.Sleep, so shutdown is never delayed by a
// retry timer.
func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// stockIdentity is the part of a stock signal needed to locate its summary.
type stockIdentity struct {
	Symbol   string
	Exchange string
}

// decodeStock decodes a protobuf stockstore.v1.Stock body. Only symbol and
// exchange matter; both are trimmed and must be non-empty. The body itself
// is never logged.
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
