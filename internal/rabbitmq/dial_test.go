package rabbitmq

import (
	"context"
	"net"
	"testing"
	"time"
)

// stallServer accepts TCP connections and then holds them open without ever
// answering the AMQP handshake, so `amqp.DialConfig` must not rely on the
// broker for any progress. TCP connect succeeds; the handshake never starts.
type stallServer struct {
	ln     net.Listener
	active []net.Conn
	done   chan struct{}
}

func newStallServer(t *testing.T) *stallServer {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &stallServer{ln: ln, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.active = append(s.active, c)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-s.done
		for _, c := range s.active {
			_ = c.Close()
		}
	})
	return s
}

func (s *stallServer) addr() string { return s.ln.Addr().String() }

// TestDialAMQPBoundsStalledHandshake proves the dialer bounds the whole
// connect attempt — TCP dial and AMQP handshake — and not just the TCP dial:
// amqp091-go applies its own handshake deadline only to its default dialer,
// so a broker that accepts TCP and stalls would otherwise hang forever.
func TestDialAMQPBoundsStalledHandshake(t *testing.T) {
	addr := newStallServer(t).addr()

	start := time.Now()
	_, err := dialAMQP(context.Background(), "amqp://"+addr+"/", 300*time.Millisecond)
	if err == nil {
		t.Fatal("dial succeeded against a stalling broker, want an error")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("dial took %v, want it bounded close to the 300ms connect timeout", elapsed)
	}
}

// TestDialAMQPDeadlineHonoursContext proves an earlier context deadline wins
// over the configured connect timeout, so callers like the 5s-bounded probe
// never wait longer than their attempt bound.
func TestDialAMQPDeadlineHonoursContext(t *testing.T) {
	addr := newStallServer(t).addr()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := dialAMQP(ctx, "amqp://"+addr+"/", time.Minute)
	if err == nil {
		t.Fatal("dial succeeded against a stalling broker, want an error")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("dial took %v with a 300ms context deadline, want it bounded", elapsed)
	}
}
