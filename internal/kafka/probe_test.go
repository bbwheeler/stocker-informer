package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// fakeConn implements metadataConn with scripted responses.
type fakeConn struct {
	partitions []kafka.Partition
	err        error

	closed    bool
	gotTopics []string

	onSetDeadline func(time.Time)
}

func (c *fakeConn) ReadPartitions(topics ...string) ([]kafka.Partition, error) {
	c.gotTopics = append(c.gotTopics, topics...)
	return c.partitions, c.err
}

func (c *fakeConn) SetDeadline(t time.Time) error {
	if c.onSetDeadline != nil {
		c.onSetDeadline(t)
	}
	return nil
}

func (c *fakeConn) Close() error {
	c.closed = true
	return nil
}

// hangingConn mimics a broker that accepts the connection but never answers:
// ReadPartitions blocks until the deadline set through SetDeadline passes.
// If no deadline is ever set it blocks forever, like a real kafka-go
// connection whose deadlines are zero.
type hangingConn struct {
	deadline time.Time
	set      chan struct{}
}

func newHangingConn() *hangingConn {
	return &hangingConn{set: make(chan struct{})}
}

func (c *hangingConn) SetDeadline(t time.Time) error {
	c.deadline = t
	close(c.set)
	return nil
}

func (c *hangingConn) ReadPartitions(topics ...string) ([]kafka.Partition, error) {
	<-c.set
	<-time.After(time.Until(c.deadline))
	return nil, errors.New("i/o timeout")
}

func (c *hangingConn) Close() error { return nil }

// syncBuffer is a race-safe sink for the capturing logger.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newCapturingLogger returns a Debug-level JSON logger writing into buf so
// every record the probe emits is captured.
func newCapturingLogger(buf *syncBuffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// logRecords decodes each captured JSON line into a map.
func logRecords(t *testing.T, buf *syncBuffer) []map[string]any {
	t.Helper()

	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		records = append(records, rec)
	}
	return records
}

// countRecords returns how many captured records match msg and level; an empty
// level matches any level.
func countRecords(t *testing.T, buf *syncBuffer, level, msg string) int {
	t.Helper()

	count := 0
	for _, rec := range logRecords(t, buf) {
		if rec["msg"] != msg {
			continue
		}
		if level != "" && rec["level"] != level {
			continue
		}
		count++
	}
	return count
}

// waitFor polls cond until it holds or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

// newTestProbe returns a probe with millisecond timings so tests finish fast.
func newTestProbe(log *slog.Logger, brokers ...string) *Probe {
	p := NewProbe(brokers, "topic", log)
	p.attemptTimeout = time.Second
	p.retryInitial = time.Millisecond
	p.retryMax = time.Millisecond
	p.healthyEvery = time.Millisecond
	return p
}

func TestProbeCheckSuccess(t *testing.T) {
	conn := &fakeConn{partitions: []kafka.Partition{{}, {}}}
	p := newTestProbe(newTestLogger(), "broker-1:9092")
	p.dial = func(_ context.Context, addr string) (metadataConn, error) {
		if addr != "broker-1:9092" {
			t.Errorf("dial address = %q, want %q", addr, "broker-1:9092")
		}
		return conn, nil
	}

	if err := p.Check(context.Background()); err != nil {
		t.Fatalf("Check() = %v, want nil", err)
	}
	if len(conn.gotTopics) != 1 || conn.gotTopics[0] != "topic" {
		t.Errorf("ReadPartitions topics = %v, want [topic]", conn.gotTopics)
	}
	if !conn.closed {
		t.Error("connection was not closed")
	}
}

func TestProbeCheckTopicNotFound(t *testing.T) {
	p := newTestProbe(newTestLogger(), "broker-1:9092")
	p.dial = func(context.Context, string) (metadataConn, error) {
		return &fakeConn{err: kafka.UnknownTopicOrPartition}, nil
	}

	err := p.Check(context.Background())
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Check() = %v, want ErrTopicNotFound", err)
	}
	if !strings.Contains(err.Error(), "topic") {
		t.Errorf("error %q does not name the topic", err)
	}
}

func TestProbeCheckZeroPartitions(t *testing.T) {
	p := newTestProbe(newTestLogger(), "broker-1:9092")
	p.dial = func(context.Context, string) (metadataConn, error) {
		return &fakeConn{}, nil
	}

	err := p.Check(context.Background())
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Check() = %v, want ErrTopicNotFound for zero partitions", err)
	}
}

func TestProbeCheckNoBrokerReachable(t *testing.T) {
	brokers := []string{"broker-1:9092", "broker-2:9092"}
	p := newTestProbe(newTestLogger(), brokers...)
	p.dial = func(_ context.Context, addr string) (metadataConn, error) {
		return nil, fmt.Errorf("dial %s: connection refused", addr)
	}

	err := p.Check(context.Background())
	if err == nil {
		t.Fatal("Check() = nil, want error")
	}
	if errors.Is(err, ErrTopicNotFound) {
		t.Errorf("Check() = %v, transport failures must not be ErrTopicNotFound", err)
	}
	for _, broker := range brokers {
		if !strings.Contains(err.Error(), broker) {
			t.Errorf("error %q does not name broker %q", err, broker)
		}
	}
}

func TestProbeCheckFallsBackToBroker(t *testing.T) {
	p := newTestProbe(newTestLogger(), "broker-1:9092", "broker-2:9092")
	p.dial = func(_ context.Context, addr string) (metadataConn, error) {
		if addr == "broker-1:9092" {
			return &fakeConn{err: kafka.UnknownTopicOrPartition}, nil
		}
		return &fakeConn{partitions: []kafka.Partition{{}}}, nil
	}

	if err := p.Check(context.Background()); err != nil {
		t.Fatalf("Check() = %v, want nil: a partial metadata view is not a topic-missing verdict", err)
	}
}

func TestProbeCheckEmptyBrokerList(t *testing.T) {
	p := newTestProbe(newTestLogger())

	err := p.Check(context.Background())
	if err == nil {
		t.Fatal("Check() = nil, want error for an empty broker list")
	}
	if errors.Is(err, ErrTopicNotFound) {
		t.Errorf("Check() = %v, an empty broker list must not be ErrTopicNotFound", err)
	}
}

func TestProbeCheckSetsAttemptDeadlineOnConn(t *testing.T) {
	p := newTestProbe(newTestLogger(), "broker-1:9092")
	p.attemptTimeout = 5 * time.Second

	var got time.Time
	p.dial = func(context.Context, string) (metadataConn, error) {
		conn := &fakeConn{partitions: []kafka.Partition{{}}}
		conn.onSetDeadline = func(d time.Time) { got = d }
		return conn, nil
	}

	start := time.Now()
	if err := p.Check(context.Background()); err != nil {
		t.Fatalf("Check() = %v, want nil", err)
	}
	if got.IsZero() {
		t.Fatal("SetDeadline was not called on the connection; the metadata exchange is unbounded")
	}
	if until := got.Sub(start); until <= 0 || until > p.attemptTimeout+100*time.Millisecond {
		t.Errorf("SetDeadline deadline %v from Check start, want within (0, attemptTimeout]", until)
	}
}

func TestProbeCheckHangBounded(t *testing.T) {
	p := newTestProbe(newTestLogger(), "broker-1:9092")
	p.attemptTimeout = 50 * time.Millisecond

	conn := newHangingConn()
	p.dial = func(context.Context, string) (metadataConn, error) { return conn, nil }

	start := time.Now()
	errCh := make(chan error, 1)
	go func() { errCh <- p.Check(context.Background()) }()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Check() = nil, want a timeout error from the hanging broker")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Check() did not return; the metadata exchange is not bounded by the attempt timeout")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Check() took %v, want close to the attempt timeout (50ms)", elapsed)
	}
}

// runProbe starts Run in a goroutine and returns a signal closed when Run
// returns plus an onReady hook counting and signalling readiness flips.
func runProbe(ctx context.Context, p *Probe) (done <-chan struct{}, ready <-chan struct{}, readyCalls *atomic.Int32) {
	var (
		doneCh    = make(chan struct{})
		readyCh   = make(chan struct{}, 1)
		callCount atomic.Int32
	)

	go func() {
		defer close(doneCh)
		p.Run(ctx, func() {
			callCount.Add(1)
			select {
			case readyCh <- struct{}{}:
			default:
			}
		})
	}()

	return doneCh, readyCh, &callCount
}

func TestProbeRunFlipsReadyOnce(t *testing.T) {
	var buf syncBuffer
	p := newTestProbe(newCapturingLogger(&buf), "broker-1:9092")
	p.dial = func(context.Context, string) (metadataConn, error) {
		return &fakeConn{partitions: []kafka.Partition{{}}}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done, ready, readyCalls := runProbe(ctx, p)

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("onReady not called within 2s")
	}
	cancel()
	<-done

	if got := readyCalls.Load(); got != 1 {
		t.Errorf("onReady calls = %d, want exactly 1", got)
	}
	if count := countRecords(t, &buf, "INFO", "kafka connectivity verified"); count != 1 {
		t.Errorf("INFO kafka connectivity verified records = %d, want 1:\n%s", count, buf.String())
	}
}

func TestProbeRunRetriesThenReady(t *testing.T) {
	var buf syncBuffer
	p := newTestProbe(newCapturingLogger(&buf), "broker-1:9092")

	var checks atomic.Int32
	p.dial = func(context.Context, string) (metadataConn, error) {
		if checks.Add(1) <= 2 {
			return &fakeConn{err: kafka.UnknownTopicOrPartition}, nil
		}
		return &fakeConn{partitions: []kafka.Partition{{}}}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done, ready, readyCalls := runProbe(ctx, p)

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("onReady not called within 2s")
	}
	cancel()
	<-done

	if got := readyCalls.Load(); got != 1 {
		t.Errorf("onReady calls = %d, want exactly 1", got)
	}
	if count := countRecords(t, &buf, "WARN", "kafka topic not available"); count < 1 {
		t.Errorf("want at least one WARN kafka topic not available record:\n%s", buf.String())
	}
	if count := countRecords(t, &buf, "INFO", "kafka connectivity verified"); count != 1 {
		t.Errorf("INFO kafka connectivity verified records = %d, want 1:\n%s", count, buf.String())
	}
}

func TestProbeRunReadinessSticky(t *testing.T) {
	var buf syncBuffer
	p := newTestProbe(newCapturingLogger(&buf), "broker-1:9092")

	var checks atomic.Int32
	p.dial = func(context.Context, string) (metadataConn, error) {
		if checks.Add(1) == 2 {
			return nil, errors.New("connection refused")
		}
		return &fakeConn{partitions: []kafka.Partition{{}}}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done, ready, readyCalls := runProbe(ctx, p)

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("onReady not called within 2s")
	}
	waitFor(t, 2*time.Second, func() bool {
		return countRecords(t, &buf, "INFO", "kafka connectivity restored") == 1
	})
	cancel()
	<-done

	if got := readyCalls.Load(); got != 1 {
		t.Errorf("onReady calls = %d, want exactly 1 despite the failure", got)
	}
	if count := countRecords(t, &buf, "WARN", "kafka connectivity lost"); count != 1 {
		t.Errorf("WARN kafka connectivity lost records = %d, want 1:\n%s", count, buf.String())
	}
	if count := countRecords(t, &buf, "INFO", "kafka connectivity restored"); count != 1 {
		t.Errorf("INFO kafka connectivity restored records = %d, want 1:\n%s", count, buf.String())
	}
}

func TestProbeRunStopsOnCancel(t *testing.T) {
	var buf syncBuffer
	p := newTestProbe(newCapturingLogger(&buf), "broker-1:9092")
	p.dial = func(context.Context, string) (metadataConn, error) {
		return nil, errors.New("connection refused")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx, nil)
	}()

	waitFor(t, 2*time.Second, func() bool {
		return countRecords(t, &buf, "WARN", "kafka connectivity probe failed") >= 1
	})
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of cancellation")
	}
}
