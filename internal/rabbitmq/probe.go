package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/example/stocker-informer/internal/config"
)

const (
	// probeAttemptTimeout bounds one Check call, including dial, channel
	// opening, and both passive declarations.
	probeAttemptTimeout = 5 * time.Second
	// probeHealthyEvery is how often a ready probe re-verifies the broker.
	probeHealthyEvery = 60 * time.Second
)

// probeConnection is the subset of *amqp.Connection a readiness attempt needs.
type probeConnection interface {
	Channel() (probeChannel, error)
	Close() error
}

// probeChannel is the subset of *amqp.Channel a readiness attempt needs. It
// deliberately offers passive declarations only: a probe must never create,
// bind, consume, or publish anything.
type probeChannel interface {
	ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	Close() error
}

// probeDialFunc opens one short-lived connection for a readiness attempt.
type probeDialFunc func(ctx context.Context, url string, timeout time.Duration) (probeConnection, error)

// Probe periodically verifies that the broker is reachable and that the
// configured exchange and queue exist with the expected flags, so readiness
// reflects real connectivity.
type Probe struct {
	cfg  config.RabbitMQConfig
	log  *slog.Logger
	dial probeDialFunc

	attemptTimeout time.Duration // default 5s
	retryInitial   time.Duration // default 1s
	retryMax       time.Duration // default 30s
	healthyEvery   time.Duration // default 60s
	sleep          func(ctx context.Context, d time.Duration) error
}

// NewProbe returns a Probe with production defaults and an amqp091-based
// dialer. It performs no network I/O; Run and Check dial the broker.
func NewProbe(cfg config.RabbitMQConfig, log *slog.Logger) *Probe {
	return newProbe(cfg, log, dialProbeAMQP)
}

// newProbe is the injectable constructor used by tests. The dial and sleep
// seams are the only ways to reach a broker or wall-clock time, so passing
// fakes keeps the tests offline and instantaneous.
func newProbe(cfg config.RabbitMQConfig, log *slog.Logger, dial probeDialFunc) *Probe {
	if log == nil {
		log = slog.Default()
	}
	if dial == nil {
		dial = dialProbeAMQP
	}
	return &Probe{
		cfg:            cfg,
		log:            log,
		dial:           dial,
		attemptTimeout: probeAttemptTimeout,
		retryInitial:   backoffInitial,
		retryMax:       backoffMax,
		healthyEvery:   probeHealthyEvery,
		sleep:          sleepContext,
	}
}

// Check opens a fresh connection and verifies the passive topology: the input
// exchange must exist with the configured type and durability, and the queue
// must exist as durable and non-exclusive. It closes the channel and
// connection before returning and logs nothing.
//
// The whole attempt is bounded by attemptTimeout. AMQP RPCs have no context
// parameter, so the attempt runs in a goroutine and a timeout closes the
// connection to unblock an in-flight declaration; without that a broker that
// accepts TCP but stops answering would hang the probe indefinitely.
func (p *Probe) Check(ctx context.Context) error {
	attemptCtx, cancel := context.WithTimeout(ctx, p.attemptTimeout)
	defer cancel()

	type dialResult struct {
		conn probeConnection
		err  error
	}
	dialed := make(chan dialResult, 1)
	done := make(chan error, 1)

	go func() {
		conn, err := p.dial(attemptCtx, p.cfg.URL, p.cfg.ConnectTimeout)
		dialed <- dialResult{conn: conn, err: err}
		if err != nil {
			done <- fmt.Errorf("dial rabbitmq: %w", err)
			return
		}
		done <- p.checkTopology(conn)
	}()

	var conn probeConnection
	select {
	case res := <-dialed:
		if res.err != nil {
			return fmt.Errorf("dial rabbitmq: %w", res.err)
		}
		conn = res.conn
	case <-attemptCtx.Done():
		// The dialer honors the attempt context, so there is nothing
		// to close yet.
		return fmt.Errorf("rabbitmq probe attempt aborted after %v: %w", p.attemptTimeout, attemptCtx.Err())
	}

	select {
	case err := <-done:
		return err
	case <-attemptCtx.Done():
		// Closing the connection is what unblocks the in-flight
		// declaration. Do not wait for the attempt goroutine: a broker
		// that ignores the close must not extend Check's bound.
		_ = conn.Close()
		return fmt.Errorf("rabbitmq probe attempt timed out after %v: %w", p.attemptTimeout, attemptCtx.Err())
	}
}

// checkTopology opens one channel and passively declares the exchange then the
// queue. It always closes the channel and the connection.
func (p *Probe) checkTopology(conn probeConnection) error {
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open probe channel: %w", err)
	}
	defer conn.Close()
	defer ch.Close()

	if err := ch.ExchangeDeclarePassive(p.cfg.InputExchange, p.cfg.InputExchangeType, true, false, false, false, nil); err != nil {
		return fmt.Errorf("exchange %q is unavailable: %w", p.cfg.InputExchange, err)
	}
	if _, err := ch.QueueDeclarePassive(p.cfg.Queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("queue %q is unavailable: %w", p.cfg.Queue, err)
	}
	return nil
}

// Run supervises the probe until ctx is cancelled, logging connectivity
// transitions and calling onReady (at most once) on the first success.
// Readiness is sticky: a later failure logs a loss but never calls onReady
// again. Backoff is deterministic exponential, 1s doubling to 30s, and a ready
// probe re-checks every healthyEvery. All sleeps are cancel-aware.
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
				// The first failure and every tenth are loud; the
				// rest are expected background noise until the
				// consumer has declared the topology.
				if attempts == 1 || attempts%10 == 0 {
					level = slog.LevelWarn
				}
				p.log.Log(ctx, level, "rabbitmq connectivity probe failed",
					append(p.topologyAttrs(),
						"attempt", attempts,
						"retry_in", retryDelay,
						"error", err,
					)...)
			case !lost:
				lost = true
				p.log.Warn("rabbitmq connectivity lost", append(p.topologyAttrs(), "error", err)...)
			default:
				p.log.Debug("rabbitmq connectivity probe failed", append(p.topologyAttrs(), "error", err)...)
			}

			if err := p.sleep(ctx, retryDelay); err != nil {
				return
			}
			retryDelay = min(retryDelay*2, p.retryMax)
			continue
		}

		switch {
		case !ready:
			ready = true
			p.log.Info("rabbitmq connectivity verified", p.topologyAttrs()...)
			if onReady != nil {
				onReady()
			}
		case lost:
			lost = false
			p.log.Info("rabbitmq connectivity restored", p.topologyAttrs()...)
		}

		retryDelay = p.retryInitial
		if err := p.sleep(ctx, p.healthyEvery); err != nil {
			return
		}
	}
}

// topologyAttrs are the stable fields of every probe log event. The broker URL
// is redacted because RABBITMQ_URL carries credentials.
func (p *Probe) topologyAttrs() []any {
	return []any{
		"broker", config.RedactedAMQPURL(p.cfg.URL),
		"exchange", p.cfg.InputExchange,
		"exchange_type", p.cfg.InputExchangeType,
		"routing_key", p.cfg.InputRoutingKey,
		"queue", p.cfg.Queue,
	}
}

// dialProbeAMQP reuses the consumer's dialer and narrows the result to the
// passive-declaration seams. *amqp.Channel implements probeChannel, so the
// assertion in probeConn.Channel cannot fail for the production dialer.
func dialProbeAMQP(ctx context.Context, rawURL string, timeout time.Duration) (probeConnection, error) {
	conn, err := dialAMQP(ctx, rawURL, timeout)
	if err != nil {
		return nil, err
	}
	return probeConn{conn: conn}, nil
}

// probeConn adapts the consumer's connection seam to the probe's, which needs
// the passive declaration methods that the consumer never uses.
type probeConn struct {
	conn connection
}

// Channel opens a channel and exposes it as a probeChannel.
func (c probeConn) Channel() (probeChannel, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}
	pc, ok := ch.(probeChannel)
	if !ok {
		_ = ch.Close()
		return nil, errors.New("rabbitmq probe: channel does not support passive declarations")
	}
	return pc, nil
}

// Close releases the underlying connection.
func (c probeConn) Close() error { return c.conn.Close() }
