package rabbitmq

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestSessionAckForwardsTag(t *testing.T) {
	fch := newFakeChannel()
	sess := newTestSession(fch)

	if err := sess.ack(42); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if acks := fch.acked(); !reflect.DeepEqual(acks, []uint64{42}) {
		t.Errorf("acks = %v, want [42]", acks)
	}
}

func TestSessionNackForwardsTagAndRequeue(t *testing.T) {
	fch := newFakeChannel()
	sess := newTestSession(fch)

	if err := sess.nack(7, true); err != nil {
		t.Fatalf("nack: %v", err)
	}
	want := []nackCall{{tag: 7, multiple: false, requeue: true}}
	if nacks := fch.nacked(); !reflect.DeepEqual(nacks, want) {
		t.Errorf("nacks = %+v, want %+v", nacks, want)
	}
}

// TestSessionSettlementAfterClose proves a closed session returns
// errSessionClosed and never touches the channel: the tag died with it, so a
// retry on a replacement channel would settle the wrong delivery.
func TestSessionSettlementAfterClose(t *testing.T) {
	fch := newFakeChannel()
	sess := newTestSession(fch)

	if err := sess.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := sess.ack(1); !errors.Is(err, errSessionClosed) {
		t.Errorf("ack after close = %v, want errSessionClosed", err)
	}
	if err := sess.nack(1, true); !errors.Is(err, errSessionClosed) {
		t.Errorf("nack after close = %v, want errSessionClosed", err)
	}
	if acks := fch.acked(); len(acks) != 0 {
		t.Errorf("acks after close = %v, want none", acks)
	}
	if nacks := fch.nacked(); len(nacks) != 0 {
		t.Errorf("nacks after close = %+v, want none", nacks)
	}
}

func TestSessionCloseIdempotent(t *testing.T) {
	fch := newFakeChannel()
	fconn := &fakeConnection{ch: fch}
	sess := &session{conn: fconn, ch: fch, deliveries: make(chan amqp.Delivery), tag: consumerTagPrefix + "idem"}

	if err := sess.close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := sess.close(); err != nil {
		t.Errorf("second close = %v, want nil", err)
	}
	if fch.closeCount() != 1 {
		t.Errorf("channel closes = %d, want 1", fch.closeCount())
	}
	if fconn.closed() != 1 {
		t.Errorf("connection closes = %d, want 1", fconn.closed())
	}
}

func TestSessionCloseReleasesChannelBeforeConnection(t *testing.T) {
	trace := &traceLog{}
	fch := newFakeChannel()
	fch.trace = trace
	fconn := &fakeConnection{ch: fch, trace: trace}
	sess := &session{conn: fconn, ch: fch, deliveries: make(chan amqp.Delivery), tag: consumerTagPrefix + "order"}

	if err := sess.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	want := []string{"channel closed", "connection closed"}
	if got := trace.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("close order = %v, want %v", got, want)
	}
}

func TestSessionCloseReportsBothErrors(t *testing.T) {
	fch := newFakeChannel()
	fch.closeErr = errors.New("channel boom")
	fconn := &fakeConnection{ch: fch, closeErr: errors.New("connection boom")}
	sess := &session{conn: fconn, ch: fch, deliveries: make(chan amqp.Delivery), tag: consumerTagPrefix + "errs"}

	err := sess.close()
	if err == nil {
		t.Fatal("close = nil, want both errors joined")
	}
	if !strings.Contains(err.Error(), "channel boom") {
		t.Errorf("close error %q missing the channel error", err)
	}
	if !strings.Contains(err.Error(), "connection boom") {
		t.Errorf("close error %q missing the connection error", err)
	}
	// A failed close still marks the session closed: a second attempt
	// would repeat the same failures.
	if err := sess.close(); err != nil {
		t.Errorf("second close = %v, want nil", err)
	}
}

func TestSessionCancelConsumer(t *testing.T) {
	fch := newFakeChannel()
	sess := newTestSession(fch)
	wantTag := consumerTagPrefix + "test"

	sess.cancelConsumer()
	if cancels := fch.cancelCalls(); len(cancels) != 1 || cancels[0] != wantTag {
		t.Errorf("Cancel calls = %v, want [%q]", cancels, wantTag)
	}

	// After close the channel is gone, so cancel must not touch it.
	if err := sess.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	sess.cancelConsumer()
	if cancels := fch.cancelCalls(); len(cancels) != 1 {
		t.Errorf("Cancel calls after close = %v, want still one", cancels)
	}
}
