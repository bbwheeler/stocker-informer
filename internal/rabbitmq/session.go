package rabbitmq

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// errSessionClosed reports that the session's channel is closed. It is
// returned by a settlement that races an abnormal session end or Close; the
// broker requeues the delivery, so the caller must not retry on a replacement
// channel (delivery tags are channel-scoped).
var errSessionClosed = errors.New("consumer: session closed")

// connection is the seam over *amqp.Connection so the connect/consume loop is
// testable without a broker.
type connection interface {
	Channel() (channel, error)
	Close() error
}

// channel is the seam over *amqp.Channel. Every operation is serialized by the
// owning session's mutex because an AMQP channel is not safe for concurrent
// use.
type channel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Cancel(consumer string, noWait bool) error
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple, requeue bool) error
	IsClosed() bool
	Close() error
}

// dialFunc opens one connection. The context bounds the TCP dial and timeout
// bounds the dial attempt, so shutdown never waits on an unreachable broker.
type dialFunc func(ctx context.Context, url string, timeout time.Duration) (connection, error)

// session owns one connection, channel, and deliveries stream.
type session struct {
	conn       connection
	ch         channel
	deliveries <-chan amqp.Delivery
	tag        string

	mu     sync.Mutex // serializes every operation on ch
	closed bool
}

// ack settles tag with the broker. A closed session returns errSessionClosed
// without touching the channel: the tag died with the channel, so a retry on
// a replacement channel would settle the wrong delivery.
func (s *session) ack(tag uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errSessionClosed
	}
	return s.ch.Ack(tag, false)
}

// nack releases tag. Requeue is explicit so a shutdown drain can requeue an
// undelivered record while the channel is still open.
func (s *session) nack(tag uint64, requeue bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errSessionClosed
	}
	return s.ch.Nack(tag, false, requeue)
}

// cancelConsumer stops new deliveries without closing the channel, leaving it
// open for settlements already in flight.
func (s *session) cancelConsumer() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		_ = s.ch.Cancel(s.tag, false)
	}
}

// close releases the channel then the connection. It is idempotent: only the
// first call touches the channel or the connection.
func (s *session) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	chErr := s.ch.Close()
	connErr := s.conn.Close()
	return errors.Join(chErr, connErr)
}

// amqpConnection adapts *amqp.Connection to the connection seam.
type amqpConnection struct {
	conn *amqp.Connection
}

func (c *amqpConnection) Channel() (channel, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

func (c *amqpConnection) Close() error { return c.conn.Close() }

// dialAMQP is the production dialer. The TCP dial is bounded by timeout and by
// ctx; the 10s heartbeat bounds a broker that accepts the connection but
// stops answering. Client errors are returned unwrapped: they describe the
// host, port, and protocol, never the URL, so credentials cannot leak.
func dialAMQP(ctx context.Context, rawURL string, timeout time.Duration) (connection, error) {
	conf := amqp.Config{
		Heartbeat: 10 * time.Second,
		Dial: func(network, addr string) (net.Conn, error) {
			dialer := &net.Dialer{Timeout: timeout}
			return dialer.DialContext(ctx, network, addr)
		},
	}
	conn, err := amqp.DialConfig(rawURL, conf)
	if err != nil {
		return nil, err
	}
	return &amqpConnection{conn: conn}, nil
}
