package rabbitmq

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// --- fakes ---

type passiveExchangeDeclare struct {
	name, kind                            string
	durable, autoDelete, internal, noWait bool
	args                                  amqp.Table
}

type passiveQueueDeclare struct {
	name                                   string
	durable, autoDelete, exclusive, noWait bool
	args                                   amqp.Table
}

// fakeProbeChannel records passive declarations and returns injectable errors.
// It has no non-passive methods to call: probeChannel exposes only passive
// declares, which is itself the guarantee under test.
type fakeProbeChannel struct {
	mu sync.Mutex

	exchangeCalls []passiveExchangeDeclare
	queueCalls    []passiveQueueDeclare
	exchangeErr   error
	queueErr      error
	closeCalls    int

	// block, when non-nil, makes the exchange declare wait for it. The fake
	// connection closes it on Close, mimicking a connection close that
	// unblocks an in-flight AMQP RPC.
	block <-chan struct{}
}

func (c *fakeProbeChannel) ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	c.mu.Lock()
	c.exchangeCalls = append(c.exchangeCalls, passiveExchangeDeclare{
		name: name, kind: kind,
		durable: durable, autoDelete: autoDelete, internal: internal, noWait: noWait,
		args: args,
	})
	block, err := c.block, c.exchangeErr
	c.mu.Unlock()

	if block != nil {
		<-block
	}
	return err
}

func (c *fakeProbeChannel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queueCalls = append(c.queueCalls, passiveQueueDeclare{
		name:    name,
		durable: durable, autoDelete: autoDelete, exclusive: exclusive, noWait: noWait,
		args: args,
	})
	return amqp.Queue{Name: name}, c.queueErr
}

func (c *fakeProbeChannel) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeCalls++
	return nil
}

func (c *fakeProbeChannel) exchanges() []passiveExchangeDeclare {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]passiveExchangeDeclare(nil), c.exchangeCalls...)
}

func (c *fakeProbeChannel) queues() []passiveQueueDeclare {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]passiveQueueDeclare(nil), c.queueCalls...)
}

func (c *fakeProbeChannel) closes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeCalls
}

// fakeProbeConnection records channel opens and closes and can run a hook on
// Close to unblock a hanging channel operation.
type fakeProbeConnection struct {
	mu sync.Mutex

	ch         probeChannel
	channelErr error
	closeCalls int
	onClose    func()
}

func (c *fakeProbeConnection) Channel() (probeChannel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.channelErr != nil {
		return nil, c.channelErr
	}
	return c.ch, nil
}

func (c *fakeProbeConnection) Close() error {
	c.mu.Lock()
	c.closeCalls++
	onClose := c.onClose
	c.mu.Unlock()

	if onClose != nil {
		onClose()
	}
	return nil
}

func (c *fakeProbeConnection) closes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeCalls
}

// newTestProbe returns a probe with short fake timings and a scripted dialer,
// so tests never reach a broker or wait on a real backoff.
func newTestProbe(t *testing.T, dial probeDialFunc) *Probe {
	t.Helper()

	p := newProbe(testCfg(), newTestLogger(), dial)
	p.attemptTimeout = time.Second
	p.retryInitial = 10 * time.Millisecond
	p.retryMax = 40 * time.Millisecond
	p.healthyEvery = 20 * time.Millisecond
	return p
}

// scriptedProbeDial replays steps in order and repeats the last step once
// exhausted, like scriptedDial does for the consumer.
func scriptedProbeDial(steps ...dialResult) (probeDialFunc, func() int) {
	var mu sync.Mutex
	calls := 0
	return func(context.Context, string, time.Duration) (probeConnection, error) {
			mu.Lock()
			defer mu.Unlock()
			calls++
			idx := calls - 1
			if idx >= len(steps) {
				idx = len(steps) - 1
			}
			return steps[idx].conn, steps[idx].err
		}, func() int {
			mu.Lock()
			defer mu.Unlock()
			return calls
		}
}

type dialResult struct {
	conn probeConnection
	err  error
}

// --- Check ---

// TestProbeCheckSuccess asserts the exact passive topology probe: the exchange
// is declared with the configured type and durable/non-auto-delete flags, the
// queue with durable/non-exclusive flags, and both channel and connection are
// closed.
func TestProbeCheckSuccess(t *testing.T) {
	var (
		gotURL     string
		gotTimeout time.Duration
	)
	fch := &fakeProbeChannel{}
	fconn := &fakeProbeConnection{ch: fch}
	dial := func(_ context.Context, url string, timeout time.Duration) (probeConnection, error) {
		gotURL, gotTimeout = url, timeout
		return fconn, nil
	}

	cfg := testCfg()
	p := newTestProbe(t, dial)
	if err := p.Check(context.Background()); err != nil {
		t.Fatalf("Check() = %v, want nil", err)
	}

	if gotURL != cfg.URL {
		t.Errorf("dial URL = %q, want the configured URL", gotURL)
	}
	if gotTimeout != cfg.ConnectTimeout {
		t.Errorf("dial timeout = %v, want %v", gotTimeout, cfg.ConnectTimeout)
	}

	exchanges := fch.exchanges()
	wantExchange := []passiveExchangeDeclare{{
		name: cfg.InputExchange, kind: cfg.InputExchangeType,
		durable: true,
	}}
	if !reflect.DeepEqual(exchanges, wantExchange) {
		t.Errorf("exchange declares = %+v, want %+v", exchanges, wantExchange)
	}

	queues := fch.queues()
	wantQueue := []passiveQueueDeclare{{name: cfg.Queue, durable: true}}
	if !reflect.DeepEqual(queues, wantQueue) {
		t.Errorf("queue declares = %+v, want %+v", queues, wantQueue)
	}

	if got := fch.closes(); got != 1 {
		t.Errorf("channel closes = %d, want 1", got)
	}
	if got := fconn.closes(); got != 1 {
		t.Errorf("connection closes = %d, want 1", got)
	}
}

// TestProbeCheckExchangeNotFound asserts a 404 on the exchange is reported with
// the exchange name, skips the queue, and still closes both handles.
func TestProbeCheckExchangeNotFound(t *testing.T) {
	fch := &fakeProbeChannel{exchangeErr: notFoundError("no exchange 'stock.summary' in vhost '/'")}
	fconn := &fakeProbeConnection{ch: fch}
	p := newTestProbe(t, func(context.Context, string, time.Duration) (probeConnection, error) {
		return fconn, nil
	})

	err := p.Check(context.Background())
	if err == nil {
		t.Fatal("Check() = nil, want an exchange error")
	}
	if !strings.Contains(err.Error(), `exchange "stock.summary"`) {
		t.Errorf("error %q does not name the exchange", err)
	}
	var amqpErr *amqp.Error
	if !errors.As(err, &amqpErr) || amqpErr.Code != amqp.NotFound {
		t.Errorf("error %v does not wrap the broker 404", err)
	}
	if got := fch.queues(); len(got) != 0 {
		t.Errorf("queue declares = %+v, want none after an exchange failure", got)
	}
	if got := fch.closes(); got != 1 {
		t.Errorf("channel closes = %d, want 1", got)
	}
	if got := fconn.closes(); got != 1 {
		t.Errorf("connection closes = %d, want 1", got)
	}
}

// TestProbeCheckQueueNotFound asserts a 404 on the queue is reported with the
// queue name.
func TestProbeCheckQueueNotFound(t *testing.T) {
	fch := &fakeProbeChannel{queueErr: notFoundError("no queue 'stocker-informer' in vhost '/'")}
	fconn := &fakeProbeConnection{ch: fch}
	p := newTestProbe(t, func(context.Context, string, time.Duration) (probeConnection, error) {
		return fconn, nil
	})

	err := p.Check(context.Background())
	if err == nil {
		t.Fatal("Check() = nil, want a queue error")
	}
	if !strings.Contains(err.Error(), `queue "stocker-informer"`) {
		t.Errorf("error %q does not name the queue", err)
	}
	if got := fch.closes(); got != 1 {
		t.Errorf("channel closes = %d, want 1", got)
	}
	if got := fconn.closes(); got != 1 {
		t.Errorf("connection closes = %d, want 1", got)
	}
}

func TestProbeCheckDialError(t *testing.T) {
	p := newTestProbe(t, func(context.Context, string, time.Duration) (probeConnection, error) {
		return nil, errors.New("connection refused")
	})

	err := p.Check(context.Background())
	if err == nil {
		t.Fatal("Check() = nil, want a dial error")
	}
	if !strings.Contains(err.Error(), "dial rabbitmq") {
		t.Errorf("error %q does not identify the dial", err)
	}
}

// TestProbeCheckChannelError asserts a failed channel open closes the
// connection.
func TestProbeCheckChannelError(t *testing.T) {
	fconn := &fakeProbeConnection{channelErr: errors.New("channel open refused")}
	p := newTestProbe(t, func(context.Context, string, time.Duration) (probeConnection, error) {
		return fconn, nil
	})

	err := p.Check(context.Background())
	if err == nil || !strings.Contains(err.Error(), "open probe channel") {
		t.Fatalf("Check() = %v, want a channel-open error", err)
	}
	if got := fconn.closes(); got != 1 {
		t.Errorf("connection closes = %d, want 1", got)
	}
}

// TestProbeCheckTimeoutClosesConnection proves a broker that accepts the
// connection but never answers cannot hang the probe: Check returns at the
// attempt deadline with context.DeadlineExceeded and closes the connection,
// which is what unblocks the in-flight declare.
func TestProbeCheckTimeoutClosesConnection(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	fch := &fakeProbeChannel{block: release}
	fconn := &fakeProbeConnection{ch: fch, onClose: func() { once.Do(func() { close(release) }) }}

	p := newTestProbe(t, func(context.Context, string, time.Duration) (probeConnection, error) {
		return fconn, nil
	})
	p.attemptTimeout = 50 * time.Millisecond

	start := time.Now()
	err := p.Check(context.Background())
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Check() = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > time.Second {
		t.Errorf("Check() took %v, want close to the 50ms attempt timeout", elapsed)
	}
	waitFor(t, "connection close", func() bool { return fconn.closes() >= 1 })
}

// --- Run ---

// runProbeAsync starts Run in a goroutine and reports when it returns plus how
// often onReady fired.
func runProbeAsync(ctx context.Context, p *Probe) (<-chan struct{}, func() int) {
	done := make(chan struct{})
	var mu sync.Mutex
	readyCalls := 0
	go func() {
		defer close(done)
		p.Run(ctx, func() {
			mu.Lock()
			readyCalls++
			mu.Unlock()
		})
	}()
	return done, func() int {
		mu.Lock()
		defer mu.Unlock()
		return readyCalls
	}
}

func waitProbeDone(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
}

// TestProbeRunReadyOnce proves repeated successful checks call onReady exactly
// once and log the verification line exactly once.
func TestProbeRunReadyOnce(t *testing.T) {
	var buf syncBuffer
	fconn := &fakeProbeConnection{ch: &fakeProbeChannel{}}
	p := newTestProbe(t, func(context.Context, string, time.Duration) (probeConnection, error) {
		return fconn, nil
	})
	p.log = capturingLogger(&buf)
	p.healthyEvery = time.Hour // only the fake sleep sees this value

	ctx, cancel := context.WithCancel(context.Background())
	recorder := &sleepRecorder{after: func(n int) {
		if n == 3 {
			cancel()
		}
	}}
	p.sleep = recorder.sleepWith

	done, readyCalls := runProbeAsync(ctx, p)
	waitProbeDone(t, done)

	if got := readyCalls(); got != 1 {
		t.Errorf("onReady calls = %d, want exactly 1", got)
	}
	if got := len(findRecords(logRecords(t, buf.String()), "rabbitmq connectivity verified")); got != 1 {
		t.Errorf("verified records = %d, want 1:\n%s", got, buf.String())
	}
	if sleeps := recorder.recorded(); len(sleeps) != 3 {
		t.Errorf("sleeps = %v, want three healthy re-checks", sleeps)
	}
}

// TestProbeRunLossAndRestoration proves readiness is sticky: a failure after
// success logs a loss, a later success logs a restoration, and onReady never
// fires again.
func TestProbeRunLossAndRestoration(t *testing.T) {
	var buf syncBuffer
	fch := &fakeProbeChannel{}
	okConn := &fakeProbeConnection{ch: fch}
	dial, _ := scriptedProbeDial(
		dialResult{conn: okConn},
		dialResult{err: errors.New("connection refused")},
		dialResult{conn: okConn},
	)

	p := newTestProbe(t, dial)
	p.log = capturingLogger(&buf)

	ctx, cancel := context.WithCancel(context.Background())
	recorder := &sleepRecorder{after: func(n int) {
		if n == 3 {
			cancel()
		}
	}}
	p.sleep = recorder.sleepWith

	done, readyCalls := runProbeAsync(ctx, p)
	waitProbeDone(t, done)

	if got := readyCalls(); got != 1 {
		t.Errorf("onReady calls = %d, want exactly 1 despite the loss", got)
	}
	records := logRecords(t, buf.String())
	for msg, want := range map[string]int{
		"rabbitmq connectivity verified": 1,
		"rabbitmq connectivity lost":     1,
		"rabbitmq connectivity restored": 1,
	} {
		if got := len(findRecords(records, msg)); got != want {
			t.Errorf("%q records = %d, want %d:\n%s", msg, got, want, buf.String())
		}
	}
	loss := mustFindRecord(t, records, "rabbitmq connectivity lost")
	if loss["broker"] != "amqp://192.168.1.48:5672/" {
		t.Errorf("loss broker = %v, want the redacted URL", loss["broker"])
	}
	for _, field := range []string{"exchange", "exchange_type", "routing_key", "queue"} {
		if _, ok := loss[field]; !ok {
			t.Errorf("loss record missing %q: %v", field, loss)
		}
	}
}

// TestProbeRunFailureLogLevels asserts the first and every tenth failure are
// Warn, the rest Debug, and each failure carries attempt, retry_in, and error.
func TestProbeRunFailureLogLevels(t *testing.T) {
	var buf syncBuffer
	p := newTestProbe(t, func(context.Context, string, time.Duration) (probeConnection, error) {
		return nil, errors.New("connection refused")
	})
	p.log = capturingLogger(&buf)

	ctx, cancel := context.WithCancel(context.Background())
	recorder := &sleepRecorder{after: func(n int) {
		if n == 4 {
			cancel()
		}
	}}
	p.sleep = recorder.sleepWith

	done, readyCalls := runProbeAsync(ctx, p)
	waitProbeDone(t, done)

	if got := readyCalls(); got != 0 {
		t.Errorf("onReady calls = %d, want 0", got)
	}
	records := findRecords(logRecords(t, buf.String()), "rabbitmq connectivity probe failed")
	if len(records) != 4 {
		t.Fatalf("probe-failed records = %d, want 4:\n%s", len(records), buf.String())
	}
	wantLevels := []string{"WARN", "DEBUG", "DEBUG", "DEBUG"}
	for i, want := range wantLevels {
		if got := records[i]["level"]; got != want {
			t.Errorf("attempt %d level = %v, want %s", i+1, got, want)
		}
		if records[i]["attempt"] != float64(i+1) {
			t.Errorf("attempt field = %v, want %d", records[i]["attempt"], i+1)
		}
		if _, ok := records[i]["retry_in"]; !ok {
			t.Errorf("attempt %d missing retry_in", i+1)
		}
		if _, ok := records[i]["error"]; !ok {
			t.Errorf("attempt %d missing error", i+1)
		}
	}
}

// TestProbeRunBackoffGrowth asserts the deterministic 1s, 2s, 4s, … 30s cap.
func TestProbeRunBackoffGrowth(t *testing.T) {
	p := newTestProbe(t, func(context.Context, string, time.Duration) (probeConnection, error) {
		return nil, errors.New("connection refused")
	})
	// Production backoff values, exercised through the fake sleep seam.
	p.retryInitial = time.Second
	p.retryMax = 30 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	recorder := &sleepRecorder{after: func(n int) {
		if n == 8 {
			cancel()
		}
	}}
	p.sleep = recorder.sleepWith

	done, _ := runProbeAsync(ctx, p)
	waitProbeDone(t, done)

	want := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
		30 * time.Second,
		30 * time.Second,
	}
	if got := recorder.recorded(); !reflect.DeepEqual(got, want) {
		t.Errorf("backoff sleeps = %v, want %v", got, want)
	}
}

// TestProbeRunCancelExitsPromptly uses the real cancel-aware sleep: Run must
// return as soon as the context ends, without waiting out the 1s retry.
func TestProbeRunCancelExitsPromptly(t *testing.T) {
	var buf syncBuffer
	p := newProbe(testCfg(), capturingLogger(&buf), func(context.Context, string, time.Duration) (probeConnection, error) {
		return nil, errors.New("connection refused")
	})

	ctx, cancel := context.WithCancel(context.Background())
	done, _ := runProbeAsync(ctx, p)

	waitFor(t, "first probe failure", func() bool {
		return len(findRecords(logRecords(t, buf.String()), "rabbitmq connectivity probe failed")) >= 1
	})
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1s of cancellation")
	}
}

// TestProbeRunRedactedLogs proves the broker password never reaches the logs.
func TestProbeRunRedactedLogs(t *testing.T) {
	var buf syncBuffer
	fconn := &fakeProbeConnection{ch: &fakeProbeChannel{}}
	p := newTestProbe(t, func(context.Context, string, time.Duration) (probeConnection, error) {
		return fconn, nil
	})
	p.log = capturingLogger(&buf)

	ctx, cancel := context.WithCancel(context.Background())
	recorder := &sleepRecorder{after: func(n int) {
		if n == 1 {
			cancel()
		}
	}}
	p.sleep = recorder.sleepWith

	done, _ := runProbeAsync(ctx, p)
	waitProbeDone(t, done)

	logged := buf.String()
	if strings.Contains(logged, secretPassword) {
		t.Errorf("logs contain the broker password:\n%s", logged)
	}
	if !strings.Contains(logged, "amqp://192.168.1.48:5672/") {
		t.Errorf("logs do not contain the redacted broker:\n%s", logged)
	}
}

// capturingLogger returns a Debug-level JSON logger so every probe record is
// captured.
func capturingLogger(buf *syncBuffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func notFoundError(reason string) *amqp.Error {
	return &amqp.Error{Code: amqp.NotFound, Reason: reason, Server: true}
}
