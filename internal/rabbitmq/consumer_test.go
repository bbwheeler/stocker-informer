package rabbitmq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	stockstorev1 "git.wheeli.ca/brian/stocker-store/proto/v1"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	"github.com/example/stocker-informer/internal/config"
	"github.com/example/stocker-informer/internal/rewriter"
	"github.com/example/stocker-informer/internal/summarystore"
)

// secretPassword is embedded in every test broker URL so log-hygiene tests can
// prove credentials never reach the logs.
const secretPassword = "supersecret"

// syncBuffer is a race-free bytes.Buffer for logs written by Run's goroutine
// while the test reads them.
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

// traceLog records ordered events across the fake channel and connection so
// close ordering can be asserted.
type traceLog struct {
	mu     sync.Mutex
	events []string
}

func (l *traceLog) record(event string) {
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

func (l *traceLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// --- fake channel / connection ---

type exchangeDeclareCall struct {
	name, kind                            string
	durable, autoDelete, internal, noWait bool
	args                                  amqp.Table
}

type queueDeclareCall struct {
	name                                   string
	durable, autoDelete, exclusive, noWait bool
	args                                   amqp.Table
}

type queueBindCall struct {
	name, key, exchange string
	noWait              bool
	args                amqp.Table
}

type qosCall struct {
	prefetchCount, prefetchSize int
	global                      bool
}

type consumeCall struct {
	queue, consumer                     string
	autoAck, exclusive, noLocal, noWait bool
	args                                amqp.Table
}

type nackCall struct {
	tag      uint64
	multiple bool
	requeue  bool
}

// fakeChannel records every channel operation and returns injectable errors.
type fakeChannel struct {
	mu sync.Mutex

	trace      *traceLog
	deliveries chan amqp.Delivery

	exchangeDeclares []exchangeDeclareCall
	queueDeclares    []queueDeclareCall
	queueBinds       []queueBindCall
	qoses            []qosCall
	consumes         []consumeCall
	cancels          []string
	acks             []uint64
	nacks            []nackCall
	closeCalls       int

	exchangeErr error
	consumeErr  error
	ackErr      error
	closeErr    error
}

func newFakeChannel() *fakeChannel {
	return &fakeChannel{deliveries: make(chan amqp.Delivery, 16)}
}

func (f *fakeChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exchangeDeclares = append(f.exchangeDeclares, exchangeDeclareCall{
		name: name, kind: kind, durable: durable, autoDelete: autoDelete,
		internal: internal, noWait: noWait, args: args,
	})
	return f.exchangeErr
}

func (f *fakeChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queueDeclares = append(f.queueDeclares, queueDeclareCall{
		name: name, durable: durable, autoDelete: autoDelete,
		exclusive: exclusive, noWait: noWait, args: args,
	})
	return amqp.Queue{Name: name}, nil
}

func (f *fakeChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queueBinds = append(f.queueBinds, queueBindCall{
		name: name, key: key, exchange: exchange, noWait: noWait, args: args,
	})
	return nil
}

func (f *fakeChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.qoses = append(f.qoses, qosCall{prefetchCount: prefetchCount, prefetchSize: prefetchSize, global: global})
	return nil
}

func (f *fakeChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.consumes = append(f.consumes, consumeCall{
		queue: queue, consumer: consumer, autoAck: autoAck, exclusive: exclusive,
		noLocal: noLocal, noWait: noWait, args: args,
	})
	return f.deliveries, f.consumeErr
}

func (f *fakeChannel) Cancel(consumer string, noWait bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancels = append(f.cancels, consumer)
	return nil
}

func (f *fakeChannel) Ack(tag uint64, multiple bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks = append(f.acks, tag)
	return f.ackErr
}

func (f *fakeChannel) Nack(tag uint64, multiple, requeue bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nacks = append(f.nacks, nackCall{tag: tag, multiple: multiple, requeue: requeue})
	return nil
}

func (f *fakeChannel) IsClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeCalls > 0
}

func (f *fakeChannel) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	if f.trace != nil {
		f.trace.record("channel closed")
	}
	return f.closeErr
}

func (f *fakeChannel) acked() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.acks...)
}

func (f *fakeChannel) nacked() []nackCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]nackCall(nil), f.nacks...)
}

func (f *fakeChannel) consumeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.consumes)
}

func (f *fakeChannel) consumeCalls() []consumeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]consumeCall(nil), f.consumes...)
}

func (f *fakeChannel) qosCalls() []qosCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]qosCall(nil), f.qoses...)
}

func (f *fakeChannel) exchangeCalls() []exchangeDeclareCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]exchangeDeclareCall(nil), f.exchangeDeclares...)
}

func (f *fakeChannel) queueCalls() []queueDeclareCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]queueDeclareCall(nil), f.queueDeclares...)
}

func (f *fakeChannel) bindCalls() []queueBindCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]queueBindCall(nil), f.queueBinds...)
}

func (f *fakeChannel) cancelCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cancels...)
}

func (f *fakeChannel) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeCalls
}

// fakeConnection hands out one fake channel (or an error) and counts closes.
type fakeConnection struct {
	mu         sync.Mutex
	trace      *traceLog
	ch         *fakeChannel
	channelErr error
	closeErr   error
	closeCalls int
}

func (f *fakeConnection) Channel() (channel, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.channelErr != nil {
		return nil, f.channelErr
	}
	return f.ch, nil
}

func (f *fakeConnection) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	if f.trace != nil {
		f.trace.record("connection closed")
	}
	return f.closeErr
}

func (f *fakeConnection) closed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeCalls
}

// --- dial and sleep seams ---

type dialStep struct {
	conn connection
	err  error
}

// scriptedDial replays steps in order and repeats the last step once
// exhausted. It returns the dial function and an accessor for the call count.
func scriptedDial(steps ...dialStep) (dialFunc, func() int) {
	if len(steps) == 0 {
		panic("scriptedDial requires at least one step")
	}
	var mu sync.Mutex
	calls := 0
	fn := func(context.Context, string, time.Duration) (connection, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		idx := calls - 1
		if idx >= len(steps) {
			idx = len(steps) - 1
		}
		return steps[idx].conn, steps[idx].err
	}
	return fn, func() int {
		mu.Lock()
		defer mu.Unlock()
		return calls
	}
}

// sleepRecorder records requested backoff delays without waiting. The optional
// hook runs after each record, so a test can cancel the run context after the
// Nth backoff.
type sleepRecorder struct {
	mu     sync.Mutex
	delays []time.Duration
	after  func(n int)
}

func (r *sleepRecorder) sleepWith(_ context.Context, d time.Duration) error {
	r.mu.Lock()
	r.delays = append(r.delays, d)
	n := len(r.delays)
	after := r.after
	r.mu.Unlock()
	if after != nil {
		after(n)
	}
	return nil
}

func (r *sleepRecorder) recorded() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.delays...)
}

// --- pipeline fakes ---

type summaryRequest struct {
	exchange string
	symbol   string
}

// fakeSummaries records every read and returns a fixed body or error.
type fakeSummaries struct {
	mu       sync.Mutex
	body     []byte
	err      error
	requests []summaryRequest
}

func (f *fakeSummaries) ReadSummary(_ context.Context, exchange, symbol string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, summaryRequest{exchange: exchange, symbol: symbol})
	return f.body, f.err
}

func (f *fakeSummaries) reads() []summaryRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]summaryRequest(nil), f.requests...)
}

// fakePublisher records every published text and returns a fixed error.
type fakePublisher struct {
	mu    sync.Mutex
	err   error
	texts []string
}

func (f *fakePublisher) Publish(_ context.Context, text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.texts = append(f.texts, text)
	return f.err
}

func (f *fakePublisher) published() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.texts...)
}

// fakeRewriter records every input and returns a fixed report or error.
type fakeRewriter struct {
	mu     sync.Mutex
	report string
	err    error
	inputs []string
}

func (f *fakeRewriter) Rewrite(_ context.Context, summary string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inputs = append(f.inputs, summary)
	return f.report, f.err
}

func (f *fakeRewriter) rewrites() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.inputs...)
}

// blockingPublisher blocks in Publish until the run context is cancelled, so
// a test can interrupt a delivery mid-flight.
type blockingPublisher struct {
	entered chan struct{}
	once    sync.Once
}

func newBlockingPublisher() *blockingPublisher {
	return &blockingPublisher{entered: make(chan struct{})}
}

func (p *blockingPublisher) Publish(ctx context.Context, _ string) error {
	p.once.Do(func() { close(p.entered) })
	<-ctx.Done()
	return ctx.Err()
}

// identityRewriter returns the summary unchanged. It is used by the live test
// so the published text can be compared to the summary verbatim.
type identityRewriter struct{}

func (identityRewriter) Rewrite(_ context.Context, summary string) (string, error) {
	return summary, nil
}

// --- helpers ---

func testCfg() config.RabbitMQConfig {
	return config.RabbitMQConfig{
		URL:               "amqp://stocker:" + secretPassword + "@192.168.1.48:5672/",
		InputExchange:     "stock.summary",
		InputExchangeType: "topic",
		InputRoutingKey:   "stock.summary.available.v1",
		Queue:             "stocker-informer",
		Prefetch:          1,
		ConnectTimeout:    10 * time.Second,
	}
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// newTestSession wraps a fake channel in a session without any Run machinery.
func newTestSession(fch *fakeChannel) *session {
	return &session{
		conn:       &fakeConnection{ch: fch},
		ch:         fch,
		deliveries: make(chan amqp.Delivery),
		tag:        consumerTagPrefix + "test",
	}
}

func mustMarshal(t *testing.T, stock *stockstorev1.Stock) []byte {
	t.Helper()

	data, err := proto.Marshal(stock)
	if err != nil {
		t.Fatalf("marshal stock: %v", err)
	}
	return data
}

func runAsync(ctx context.Context, c *Consumer) <-chan error {
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	return done
}

func waitRun(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
		return errors.New("timeout")
	}
}

// waitFor polls cond until it holds or the deadline expires.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// logRecords parses newline-delimited JSON log output into records.
func logRecords(t *testing.T, raw string) []map[string]any {
	t.Helper()

	var records []map[string]any
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("parse log line %q: %v", line, err)
		}
		records = append(records, rec)
	}
	return records
}

func findRecords(records []map[string]any, msg string) []map[string]any {
	var found []map[string]any
	for _, rec := range records {
		if rec["msg"] == msg {
			found = append(found, rec)
		}
	}
	return found
}

// mustFindRecord returns the single record with the given message.
func mustFindRecord(t *testing.T, records []map[string]any, msg string) map[string]any {
	t.Helper()

	found := findRecords(records, msg)
	if len(found) != 1 {
		t.Fatalf("records with msg %q = %d, want 1 (all records: %v)", msg, len(found), records)
	}
	return found[0]
}

// --- decodeStock ---

func TestDecodeStock(t *testing.T) {
	value := mustMarshal(t, &stockstorev1.Stock{
		Symbol:   "aapl",
		Exchange: "nasdaq",
		Scores: []*stockstorev1.ScoreEntry{
			{Category: "sentiment", Value: 0.75},
		},
	})

	identity, err := decodeStock(value)
	if err != nil {
		t.Fatalf("decodeStock: %v", err)
	}
	if identity.Symbol != "aapl" {
		t.Errorf("symbol = %q, want %q", identity.Symbol, "aapl")
	}
	if identity.Exchange != "nasdaq" {
		t.Errorf("exchange = %q, want %q", identity.Exchange, "nasdaq")
	}
}

func TestDecodeStockTrims(t *testing.T) {
	value := mustMarshal(t, &stockstorev1.Stock{Symbol: " AAPL ", Exchange: " NASDAQ "})

	identity, err := decodeStock(value)
	if err != nil {
		t.Fatalf("decodeStock: %v", err)
	}
	if identity.Symbol != "AAPL" {
		t.Errorf("symbol = %q, want %q", identity.Symbol, "AAPL")
	}
	if identity.Exchange != "NASDAQ" {
		t.Errorf("exchange = %q, want %q", identity.Exchange, "NASDAQ")
	}
}

func TestDecodeStockRejectsEmptySymbol(t *testing.T) {
	value := mustMarshal(t, &stockstorev1.Stock{Symbol: "  ", Exchange: "NASDAQ"})

	if _, err := decodeStock(value); err == nil {
		t.Fatal("expected error for empty symbol")
	}
}

func TestDecodeStockRejectsEmptyExchange(t *testing.T) {
	value := mustMarshal(t, &stockstorev1.Stock{Symbol: "AAPL", Exchange: ""})

	if _, err := decodeStock(value); err == nil {
		t.Fatal("expected error for empty exchange")
	}
}

func TestDecodeStockRejectsMalformed(t *testing.T) {
	if _, err := decodeStock([]byte{0xff, 0xff, 0xff, 0xff}); err == nil {
		t.Fatal("expected error for malformed protobuf")
	}
}

// --- topology ---

// TestRunDeclaresTopology asserts every declaration flag and the consume
// arguments Run uses when it connects.
func TestRunDeclaresTopology(t *testing.T) {
	cfg := testCfg()
	fch := newFakeChannel()
	fconn := &fakeConnection{ch: fch}
	dial, _ := scriptedDial(dialStep{conn: fconn})

	c := newConsumer(cfg, &fakeSummaries{}, nil, &fakePublisher{}, newTestLogger(), dial, (&sleepRecorder{}).sleepWith)
	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, c)

	waitFor(t, "consume to start", func() bool { return fch.consumeCount() > 0 })
	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run: %v", err)
	}

	exchanges := fch.exchangeCalls()
	if len(exchanges) != 1 {
		t.Fatalf("ExchangeDeclare calls = %d, want 1", len(exchanges))
	}
	got := exchanges[0]
	if got.name != cfg.InputExchange || got.kind != cfg.InputExchangeType {
		t.Errorf("exchange = (%q, %q), want (%q, %q)", got.name, got.kind, cfg.InputExchange, cfg.InputExchangeType)
	}
	if !got.durable || got.autoDelete || got.internal || got.noWait || got.args != nil {
		t.Errorf("exchange flags = {durable:%t autoDelete:%t internal:%t noWait:%t args:%v}, want {true false false false nil}",
			got.durable, got.autoDelete, got.internal, got.noWait, got.args)
	}

	queues := fch.queueCalls()
	if len(queues) != 1 {
		t.Fatalf("QueueDeclare calls = %d, want 1", len(queues))
	}
	q := queues[0]
	if q.name != cfg.Queue {
		t.Errorf("queue name = %q, want %q", q.name, cfg.Queue)
	}
	if !q.durable || q.autoDelete || q.exclusive || q.noWait || q.args != nil {
		t.Errorf("queue flags = {durable:%t autoDelete:%t exclusive:%t noWait:%t args:%v}, want {true false false false nil}",
			q.durable, q.autoDelete, q.exclusive, q.noWait, q.args)
	}

	binds := fch.bindCalls()
	if len(binds) != 1 {
		t.Fatalf("QueueBind calls = %d, want 1", len(binds))
	}
	b := binds[0]
	if b.name != cfg.Queue || b.key != cfg.InputRoutingKey || b.exchange != cfg.InputExchange {
		t.Errorf("bind = (queue %q, key %q, exchange %q), want (%q, %q, %q)",
			b.name, b.key, b.exchange, cfg.Queue, cfg.InputRoutingKey, cfg.InputExchange)
	}
	if b.noWait || b.args != nil {
		t.Errorf("bind noWait=%t args=%v, want false nil", b.noWait, b.args)
	}

	qoss := fch.qosCalls()
	if len(qoss) != 1 {
		t.Fatalf("Qos calls = %d, want 1", len(qoss))
	}
	if qoss[0] != (qosCall{prefetchCount: cfg.Prefetch, prefetchSize: 0, global: false}) {
		t.Errorf("Qos = %+v, want {prefetch 1 0 false}", qoss[0])
	}

	consumes := fch.consumeCalls()
	if len(consumes) != 1 {
		t.Fatalf("Consume calls = %d, want 1", len(consumes))
	}
	cc := consumes[0]
	wantTag := fmt.Sprintf("%s%d", consumerTagPrefix, os.Getpid())
	if cc.queue != cfg.Queue || cc.consumer != wantTag {
		t.Errorf("Consume(queue %q, tag %q), want (%q, %q)", cc.queue, cc.consumer, cfg.Queue, wantTag)
	}
	if cc.autoAck || cc.exclusive || cc.noLocal || cc.noWait || cc.args != nil {
		t.Errorf("Consume flags = {autoAck:%t exclusive:%t noLocal:%t noWait:%t args:%v}, want all false with nil args",
			cc.autoAck, cc.exclusive, cc.noLocal, cc.noWait, cc.args)
	}
}

// TestRunForwardsPrefetch proves RABBITMQ_PREFETCH reaches Qos unchanged.
func TestRunForwardsPrefetch(t *testing.T) {
	cfg := testCfg()
	cfg.Prefetch = 7
	fch := newFakeChannel()
	fconn := &fakeConnection{ch: fch}
	dial, _ := scriptedDial(dialStep{conn: fconn})

	c := newConsumer(cfg, &fakeSummaries{}, nil, &fakePublisher{}, newTestLogger(), dial, (&sleepRecorder{}).sleepWith)
	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, c)

	waitFor(t, "consume to start", func() bool { return fch.consumeCount() > 0 })
	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run: %v", err)
	}

	qoss := fch.qosCalls()
	if len(qoss) != 1 || qoss[0].prefetchCount != 7 || qoss[0].prefetchSize != 0 || qoss[0].global {
		t.Errorf("Qos calls = %+v, want one call {7 0 false}", qoss)
	}
}

// TestRunStartedLogHasRedactedTopology asserts the startup line carries the
// full topology with a redacted broker URL.
func TestRunStartedLogHasRedactedTopology(t *testing.T) {
	cfg := testCfg()
	fch := newFakeChannel()
	fconn := &fakeConnection{ch: fch}
	dial, _ := scriptedDial(dialStep{conn: fconn})

	var buf syncBuffer
	c := newConsumer(cfg, &fakeSummaries{}, nil, &fakePublisher{}, slog.New(slog.NewJSONHandler(&buf, nil)), dial, (&sleepRecorder{}).sleepWith)
	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, c)

	waitFor(t, "consume to start", func() bool { return fch.consumeCount() > 0 })
	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run: %v", err)
	}

	rec := mustFindRecord(t, logRecords(t, buf.String()), "rabbitmq consumer started")
	checks := map[string]any{
		"broker":        "amqp://192.168.1.48:5672/",
		"exchange":      cfg.InputExchange,
		"exchange_type": cfg.InputExchangeType,
		"routing_key":   cfg.InputRoutingKey,
		"queue":         cfg.Queue,
		"prefetch":      float64(cfg.Prefetch),
	}
	for field, want := range checks {
		if got := rec[field]; got != want {
			t.Errorf("started log %s = %v, want %v", field, got, want)
		}
	}
}

// --- connect failure paths ---

func TestConnectExchangeDeclareFailureClosesResources(t *testing.T) {
	fch := newFakeChannel()
	fch.exchangeErr = errors.New("precondition failed")
	fconn := &fakeConnection{ch: fch}
	dial, _ := scriptedDial(dialStep{conn: fconn})

	c := newConsumer(testCfg(), &fakeSummaries{}, nil, &fakePublisher{}, newTestLogger(), dial, nil)
	if _, err := c.connect(context.Background()); err == nil {
		t.Fatal("connect succeeded, want exchange declare error")
	}
	if fch.closeCount() != 1 {
		t.Errorf("channel closes = %d, want 1", fch.closeCount())
	}
	if fconn.closed() != 1 {
		t.Errorf("connection closes = %d, want 1", fconn.closed())
	}
}

func TestConnectChannelFailureClosesConnection(t *testing.T) {
	fconn := &fakeConnection{channelErr: errors.New("channel refused")}
	dial, _ := scriptedDial(dialStep{conn: fconn})

	c := newConsumer(testCfg(), &fakeSummaries{}, nil, &fakePublisher{}, newTestLogger(), dial, nil)
	if _, err := c.connect(context.Background()); err == nil {
		t.Fatal("connect succeeded, want channel open error")
	}
	if fconn.closed() != 1 {
		t.Errorf("connection closes = %d, want 1", fconn.closed())
	}
}

func TestConnectDialFailureLeavesNoResources(t *testing.T) {
	dial, _ := scriptedDial(dialStep{err: errors.New("connection refused")})

	c := newConsumer(testCfg(), &fakeSummaries{}, nil, &fakePublisher{}, newTestLogger(), dial, nil)
	if _, err := c.connect(context.Background()); err == nil {
		t.Fatal("connect succeeded, want dial error")
	}
}

func TestConnectConsumeFailureClosesResources(t *testing.T) {
	fch := newFakeChannel()
	fch.consumeErr = errors.New("consume refused")
	fconn := &fakeConnection{ch: fch}
	dial, _ := scriptedDial(dialStep{conn: fconn})

	c := newConsumer(testCfg(), &fakeSummaries{}, nil, &fakePublisher{}, newTestLogger(), dial, nil)
	if _, err := c.connect(context.Background()); err == nil {
		t.Fatal("connect succeeded, want consume error")
	}
	if fch.closeCount() != 1 {
		t.Errorf("channel closes = %d, want 1", fch.closeCount())
	}
	if fconn.closed() != 1 {
		t.Errorf("connection closes = %d, want 1", fconn.closed())
	}
}

// --- backoff and supervision ---

// TestRunReconnectAfterDialFailureAndConnectionLoss covers the full
// supervision sequence: failed dial (1s backoff), connect, a productive
// session, connection loss (failure counter reset, 1s backoff), reconnect.
func TestRunReconnectAfterDialFailureAndConnectionLoss(t *testing.T) {
	fch1 := newFakeChannel()
	fch2 := newFakeChannel()
	conn1 := &fakeConnection{ch: fch1}
	conn2 := &fakeConnection{ch: fch2}

	dial, dialCalls := scriptedDial(
		dialStep{err: errors.New("connection refused")},
		dialStep{conn: conn1},
		dialStep{conn: conn2},
	)

	recorder := &sleepRecorder{}
	var buf syncBuffer
	summaries := &fakeSummaries{body: []byte("# AAPL")}
	publisher := &fakePublisher{}
	c := newConsumer(testCfg(), summaries, nil, publisher, slog.New(slog.NewJSONHandler(&buf, nil)), dial, recorder.sleepWith)

	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, c)

	// First dial fails, second succeeds.
	waitFor(t, "first consume", func() bool { return fch1.consumeCount() > 0 })

	// Deliver one message so the session is productive.
	fch1.deliveries <- amqp.Delivery{DeliveryTag: 1, RoutingKey: "rk", Body: mustMarshal(t, &stockstorev1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"})}
	waitFor(t, "first ack", func() bool { return len(fch1.acked()) == 1 })

	// Session loss triggers a reconnect on a fresh connection.
	close(fch1.deliveries)
	waitFor(t, "reconnect consume", func() bool { return fch2.consumeCount() > 0 })

	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run: %v", err)
	}

	gotSleeps := recorder.recorded()
	wantSleeps := []time.Duration{time.Second, time.Second}
	if !reflect.DeepEqual(gotSleeps, wantSleeps) {
		t.Errorf("sleeps = %v, want %v", gotSleeps, wantSleeps)
	}
	if dialCalls() < 3 {
		t.Errorf("dial calls = %d, want at least 3", dialCalls())
	}
	if conn1.closed() != 1 {
		t.Errorf("first connection closes = %d, want 1", conn1.closed())
	}

	logged := buf.String()
	for _, msg := range []string{
		"rabbitmq connect failed, backing off",
		"rabbitmq consumer connected",
		"rabbitmq consumer connection lost",
		"rabbitmq consumer reconnected",
	} {
		if !strings.Contains(logged, msg) {
			t.Errorf("log missing %q:\n%s", msg, logged)
		}
	}
}

// TestRunConnectionLossReconnects closes the deliveries channel without any
// message: Run must log the loss and reconnect.
func TestRunConnectionLossReconnects(t *testing.T) {
	fch1 := newFakeChannel()
	fch2 := newFakeChannel()
	conn1 := &fakeConnection{ch: fch1}
	conn2 := &fakeConnection{ch: fch2}

	dial, dialCalls := scriptedDial(
		dialStep{conn: conn1},
		dialStep{conn: conn2},
	)

	recorder := &sleepRecorder{}
	var buf syncBuffer
	c := newConsumer(testCfg(), &fakeSummaries{}, nil, &fakePublisher{}, slog.New(slog.NewJSONHandler(&buf, nil)), dial, recorder.sleepWith)

	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, c)

	waitFor(t, "first consume", func() bool { return fch1.consumeCount() > 0 })
	close(fch1.deliveries)
	waitFor(t, "reconnect consume", func() bool { return fch2.consumeCount() > 0 })

	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if dialCalls() < 2 {
		t.Errorf("dial calls = %d, want at least 2", dialCalls())
	}
	gotSleeps := recorder.recorded()
	if len(gotSleeps) != 1 || gotSleeps[0] != time.Second {
		t.Errorf("sleeps = %v, want [1s]", gotSleeps)
	}
	logged := buf.String()
	if !strings.Contains(logged, "rabbitmq consumer connection lost") {
		t.Errorf("log missing connection lost:\n%s", logged)
	}
	if !strings.Contains(logged, "rabbitmq consumer reconnected") {
		t.Errorf("log missing reconnected:\n%s", logged)
	}
}

// TestRunBackoffGrowsAndCaps asserts the deterministic sequence
// 1s, 2s, 4s, 8s, 16s, then 30s for every later failure.
func TestRunBackoffGrowsAndCaps(t *testing.T) {
	dial, _ := scriptedDial(dialStep{err: errors.New("connection refused")})

	var buf syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	recorder := &sleepRecorder{after: func(n int) {
		if n == 8 {
			cancel()
		}
	}}
	c := newConsumer(testCfg(), &fakeSummaries{}, nil, &fakePublisher{}, slog.New(slog.NewJSONHandler(&buf, nil)), dial, recorder.sleepWith)

	if err := waitRun(t, runAsync(ctx, c)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second,
		30 * time.Second, 30 * time.Second, 30 * time.Second,
	}
	if got := recorder.recorded(); !reflect.DeepEqual(got, want) {
		t.Errorf("sleeps = %v, want %v", got, want)
	}
}

// TestRunConnectFailureLevels asserts the first and tenth connect failures
// log at Warn and the rest at Debug, with a monotonic attempt counter.
func TestRunConnectFailureLevels(t *testing.T) {
	dial, _ := scriptedDial(dialStep{err: errors.New("connection refused")})

	var buf syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	recorder := &sleepRecorder{after: func(n int) {
		if n == 10 {
			cancel()
		}
	}}
	c := newConsumer(testCfg(), &fakeSummaries{}, nil, &fakePublisher{}, slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), dial, recorder.sleepWith)

	if err := waitRun(t, runAsync(ctx, c)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	connects := findRecords(logRecords(t, buf.String()), "rabbitmq connect failed, backing off")
	if len(connects) != 10 {
		t.Fatalf("connect failure records = %d, want 10", len(connects))
	}
	for i, rec := range connects {
		wantLevel := "DEBUG"
		if i == 0 || (i+1)%10 == 0 {
			wantLevel = "WARN"
		}
		if rec["level"] != wantLevel {
			t.Errorf("failure %d level = %v, want %s", i+1, rec["level"], wantLevel)
		}
		if rec["attempt"] != float64(i+1) {
			t.Errorf("failure %d attempt = %v, want %d", i+1, rec["attempt"], i+1)
		}
	}
}

// TestRunCancelDuringBackoff returns nil promptly while parked in a backoff
// sleep and logs the dedicated shutdown line.
func TestRunCancelDuringBackoff(t *testing.T) {
	dial, _ := scriptedDial(dialStep{err: errors.New("connection refused")})

	var buf syncBuffer
	sleep := func(ctx context.Context, _ time.Duration) error {
		<-ctx.Done()
		return ctx.Err()
	}
	c := newConsumer(testCfg(), &fakeSummaries{}, nil, &fakePublisher{}, slog.New(slog.NewJSONHandler(&buf, nil)), dial, sleep)

	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, c)

	waitFor(t, "connect failure log", func() bool {
		return strings.Contains(buf.String(), "rabbitmq connect failed, backing off")
	})
	cancel()

	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	if !strings.Contains(buf.String(), "rabbitmq consumer shutting down during backoff") {
		t.Errorf("log missing shutdown-during-backoff line:\n%s", buf.String())
	}
}

// --- shutdown ---

// TestRunShutdownClosesSession cancels an idle Run and asserts the shutdown
// line, the consumer cancel, and the session teardown.
func TestRunShutdownClosesSession(t *testing.T) {
	fch := newFakeChannel()
	fconn := &fakeConnection{ch: fch}
	dial, _ := scriptedDial(dialStep{conn: fconn})

	var buf syncBuffer
	c := newConsumer(testCfg(), &fakeSummaries{}, nil, &fakePublisher{}, slog.New(slog.NewJSONHandler(&buf, nil)), dial, (&sleepRecorder{}).sleepWith)

	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, c)

	waitFor(t, "consume to start", func() bool { return fch.consumeCount() > 0 })
	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	wantTag := fmt.Sprintf("%s%d", consumerTagPrefix, os.Getpid())
	if cancels := fch.cancelCalls(); len(cancels) != 1 || cancels[0] != wantTag {
		t.Errorf("Cancel calls = %v, want [%q]", cancels, wantTag)
	}
	if fch.closeCount() != 1 {
		t.Errorf("channel closes = %d, want 1", fch.closeCount())
	}
	if fconn.closed() != 1 {
		t.Errorf("connection closes = %d, want 1", fconn.closed())
	}
	if !strings.Contains(buf.String(), "rabbitmq consumer shutting down") {
		t.Errorf("log missing shutdown line:\n%s", buf.String())
	}
}

// TestRunShutdownMidDeliveryNacks cancels the context while the publisher is
// in flight: no ack, exactly one requeueing nack, and Run returns nil.
func TestRunShutdownMidDeliveryNacks(t *testing.T) {
	fch := newFakeChannel()
	fconn := &fakeConnection{ch: fch}
	dial, _ := scriptedDial(dialStep{conn: fconn})

	publisher := newBlockingPublisher()
	var buf syncBuffer
	c := newConsumer(testCfg(), &fakeSummaries{body: []byte("# AAPL")}, nil, publisher,
		slog.New(slog.NewJSONHandler(&buf, nil)), dial, (&sleepRecorder{}).sleepWith)

	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, c)

	waitFor(t, "consume to start", func() bool { return fch.consumeCount() > 0 })
	fch.deliveries <- amqp.Delivery{DeliveryTag: 9, RoutingKey: "rk", Body: mustMarshal(t, &stockstorev1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"})}
	<-publisher.entered // the publisher is blocked mid-delivery

	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if acks := fch.acked(); len(acks) != 0 {
		t.Errorf("acks = %v, want none after shutdown", acks)
	}
	nacks := fch.nacked()
	if len(nacks) != 1 {
		t.Fatalf("nacks = %+v, want exactly one requeueing nack", nacks)
	}
	if nacks[0] != (nackCall{tag: 9, multiple: false, requeue: true}) {
		t.Errorf("nack = %+v, want {tag:9 requeue:true}", nacks[0])
	}
	if !strings.Contains(buf.String(), "rabbitmq consumer shutting down") {
		t.Errorf("log missing shutdown line:\n%s", buf.String())
	}
}

// TestHandleDeliveryCancelledContextNacks asserts a delivery that arrives
// after cancellation is requeued without any downstream work.
func TestHandleDeliveryCancelledContextNacks(t *testing.T) {
	var buf syncBuffer
	summaries := &fakeSummaries{body: []byte("# AAPL")}
	publisher := &fakePublisher{}
	c := newConsumer(testCfg(), summaries, nil, publisher, slog.New(slog.NewJSONHandler(&buf, nil)), nil, nil)
	fch := newFakeChannel()
	sess := newTestSession(fch)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.handleDelivery(ctx, sess, amqp.Delivery{
		DeliveryTag: 6,
		Body:        mustMarshal(t, &stockstorev1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"}),
	})

	if acks := fch.acked(); len(acks) != 0 {
		t.Errorf("acks = %v, want none", acks)
	}
	if nacks := fch.nacked(); !reflect.DeepEqual(nacks, []nackCall{{tag: 6, requeue: true}}) {
		t.Errorf("nacks = %+v, want one requeueing nack", nacks)
	}
	if got := summaries.reads(); len(got) != 0 {
		t.Errorf("summary reads = %d, want 0 on a cancelled context", len(got))
	}
	if got := publisher.published(); len(got) != 0 {
		t.Errorf("publishes = %d, want 0 on a cancelled context", len(got))
	}
}

// TestDrainDeliveriesNacksPrefetched requeues every already-prefetched
// delivery without blocking, and stops at a closed channel.
func TestDrainDeliveriesNacksPrefetched(t *testing.T) {
	fch := newFakeChannel()
	deliveries := make(chan amqp.Delivery, 2)
	deliveries <- amqp.Delivery{DeliveryTag: 1}
	deliveries <- amqp.Delivery{DeliveryTag: 2}
	sess := &session{
		conn:       &fakeConnection{ch: fch},
		ch:         fch,
		deliveries: deliveries,
		tag:        consumerTagPrefix + "drain",
	}
	c := newConsumer(testCfg(), &fakeSummaries{}, nil, &fakePublisher{}, newTestLogger(), nil, nil)

	c.drainDeliveries(sess)

	want := []nackCall{{tag: 1, requeue: true}, {tag: 2, requeue: true}}
	if got := fch.nacked(); !reflect.DeepEqual(got, want) {
		t.Errorf("nacks = %+v, want %+v", got, want)
	}
	if acks := fch.acked(); len(acks) != 0 {
		t.Errorf("acks = %v, want none", acks)
	}

	// A closed deliveries channel ends the drain instead of blocking.
	close(deliveries)
	c.drainDeliveries(sess)
	if got := fch.nacked(); len(got) != 2 {
		t.Errorf("nacks after closed channel = %+v, want still two", got)
	}
}

// --- handleDelivery ---

// TestHandleDeliveryAcksEveryOutcome is the at-most-once table: every
// pipeline outcome settles with exactly one ack and the expected log line.
func TestHandleDeliveryAcksEveryOutcome(t *testing.T) {
	const (
		summary = "# AAPL\n\nRevenue up 12% year over year."
		report  = "# Cora-9 report"
	)
	stockBody := mustMarshal(t, &stockstorev1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"})

	tests := []struct {
		name      string
		body      []byte
		summaries *fakeSummaries
		rw        rewriter.Rewriter
		publisher *fakePublisher
		wantLog   string
		wantTexts []string
		wantReads int
	}{
		{
			name:      "invalid protobuf",
			body:      []byte{0xff, 0xff, 0xff, 0xff},
			summaries: &fakeSummaries{body: []byte(summary)},
			publisher: &fakePublisher{},
			wantLog:   "decode stock failed",
		},
		{
			name:      "missing symbol",
			body:      mustMarshal(t, &stockstorev1.Stock{Symbol: "  ", Exchange: "NASDAQ"}),
			summaries: &fakeSummaries{body: []byte(summary)},
			publisher: &fakePublisher{},
			wantLog:   "decode stock failed",
		},
		{
			name:      "missing exchange",
			body:      mustMarshal(t, &stockstorev1.Stock{Symbol: "AAPL", Exchange: ""}),
			summaries: &fakeSummaries{body: []byte(summary)},
			publisher: &fakePublisher{},
			wantLog:   "decode stock failed",
		},
		{
			name:      "summary not found",
			body:      stockBody,
			summaries: &fakeSummaries{err: summarystore.ErrNotFound},
			publisher: &fakePublisher{},
			wantLog:   "summary not found",
			wantReads: 1,
		},
		{
			name:      "summary read error",
			body:      stockBody,
			summaries: &fakeSummaries{err: errors.New("s3 boom")},
			publisher: &fakePublisher{},
			wantLog:   "read summary failed",
			wantReads: 1,
		},
		{
			name:      "empty summary",
			body:      stockBody,
			summaries: &fakeSummaries{body: []byte("  \n\t ")},
			publisher: &fakePublisher{},
			wantLog:   "summary is empty, skipping publish",
			wantReads: 1,
		},
		{
			name:      "rewrite error",
			body:      stockBody,
			summaries: &fakeSummaries{body: []byte(summary)},
			rw:        &fakeRewriter{err: errors.New("ollama down")},
			publisher: &fakePublisher{},
			wantLog:   "rewrite failed, publishing original summary",
			wantTexts: []string{summary},
			wantReads: 1,
		},
		{
			name:      "blank rewrite",
			body:      stockBody,
			summaries: &fakeSummaries{body: []byte(summary)},
			rw:        &fakeRewriter{report: "  \n"},
			publisher: &fakePublisher{},
			wantLog:   "rewrite returned empty output, publishing original summary",
			wantTexts: []string{summary},
			wantReads: 1,
		},
		{
			name:      "publish error",
			body:      stockBody,
			summaries: &fakeSummaries{body: []byte(summary)},
			publisher: &fakePublisher{err: errors.New("gotosocial down")},
			wantLog:   "publish failed",
			wantTexts: []string{summary},
			wantReads: 1,
		},
		{
			name:      "publish success",
			body:      stockBody,
			summaries: &fakeSummaries{body: []byte(summary)},
			publisher: &fakePublisher{},
			wantLog:   "summary published on GoToSocial",
			wantTexts: []string{summary},
			wantReads: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf syncBuffer
			c := newConsumer(testCfg(), tt.summaries, tt.rw, tt.publisher,
				slog.New(slog.NewJSONHandler(&buf, nil)), nil, nil)
			fch := newFakeChannel()
			sess := newTestSession(fch)

			c.handleDelivery(context.Background(), sess, amqp.Delivery{
				DeliveryTag: 7,
				RoutingKey:  "stock.summary.available.v1",
				Redelivered: true,
				Body:        tt.body,
			})

			if acks := fch.acked(); !reflect.DeepEqual(acks, []uint64{7}) {
				t.Errorf("acks = %v, want exactly [7]", acks)
			}
			if nacks := fch.nacked(); len(nacks) != 0 {
				t.Errorf("nacks = %+v, want none", nacks)
			}
			records := logRecords(t, buf.String())
			if len(findRecords(records, tt.wantLog)) != 1 {
				t.Errorf("missing log %q in:\n%s", tt.wantLog, buf.String())
			}
			if got := tt.publisher.published(); !reflect.DeepEqual(got, tt.wantTexts) {
				t.Errorf("published = %q, want %q", got, tt.wantTexts)
			}
			if got := len(tt.summaries.reads()); got != tt.wantReads {
				t.Errorf("summary reads = %d, want %d", got, tt.wantReads)
			}
		})
	}
}

// TestHandleDeliveryNilRewriter publishes the trimmed original and reports
// rewritten=false without character counts.
func TestHandleDeliveryNilRewriter(t *testing.T) {
	const summary = "  # AAPL\n\nRevenue up 12%.  "

	var buf syncBuffer
	summaries := &fakeSummaries{body: []byte(summary)}
	publisher := &fakePublisher{}
	c := newConsumer(testCfg(), summaries, nil, publisher, slog.New(slog.NewJSONHandler(&buf, nil)), nil, nil)
	fch := newFakeChannel()
	sess := newTestSession(fch)

	c.handleDelivery(context.Background(), sess, amqp.Delivery{
		DeliveryTag: 3,
		Body:        mustMarshal(t, &stockstorev1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"}),
	})

	if got := publisher.published(); !reflect.DeepEqual(got, []string{"# AAPL\n\nRevenue up 12%."}) {
		t.Errorf("published = %q, want the trimmed original", got)
	}
	rec := mustFindRecord(t, logRecords(t, buf.String()), "summary published on GoToSocial")
	if rec["rewritten"] != false {
		t.Errorf("rewritten = %v, want false", rec["rewritten"])
	}
	if _, ok := rec["summary_chars"]; ok {
		t.Errorf("summary_chars present without a rewriter: %v", rec)
	}
	if _, ok := rec["report_chars"]; ok {
		t.Errorf("report_chars present without a rewriter: %v", rec)
	}
}

// TestHandleDeliveryRewriteSuccessFields asserts the success line carries the
// delivery metadata plus rewritten and both character counts.
func TestHandleDeliveryRewriteSuccessFields(t *testing.T) {
	const summary = "# AAPL\n\nRevenue up 12%."
	const report = "# Cora-9 report on AAPL"

	var buf syncBuffer
	c := newConsumer(testCfg(), &fakeSummaries{body: []byte(summary)}, &fakeRewriter{report: report},
		&fakePublisher{}, slog.New(slog.NewJSONHandler(&buf, nil)), nil, nil)
	fch := newFakeChannel()
	sess := newTestSession(fch)

	c.handleDelivery(context.Background(), sess, amqp.Delivery{
		DeliveryTag: 11,
		RoutingKey:  "stock.summary.available.v1",
		Redelivered: true,
		Body:        mustMarshal(t, &stockstorev1.Stock{Symbol: " AAPL ", Exchange: " NASDAQ "}),
	})

	rec := mustFindRecord(t, logRecords(t, buf.String()), "summary published on GoToSocial")
	checks := map[string]any{
		"routing_key":   "stock.summary.available.v1",
		"delivery_tag":  float64(11),
		"redelivered":   true,
		"symbol":        "AAPL",
		"exchange":      "NASDAQ",
		"rewritten":     true,
		"summary_chars": float64(len([]rune(summary))),
		"report_chars":  float64(len([]rune(report))),
	}
	for field, want := range checks {
		if got := rec[field]; got != want {
			t.Errorf("%s = %v, want %v", field, got, want)
		}
	}
	if acks := fch.acked(); !reflect.DeepEqual(acks, []uint64{11}) {
		t.Errorf("acks = %v, want [11]", acks)
	}
}

// TestHandleDeliveryAckFailureLogged asserts a failing ack is logged once and
// never retried on a replacement channel.
func TestHandleDeliveryAckFailureLogged(t *testing.T) {
	var buf syncBuffer
	c := newConsumer(testCfg(), &fakeSummaries{body: []byte("# AAPL")}, nil, &fakePublisher{},
		slog.New(slog.NewJSONHandler(&buf, nil)), nil, nil)
	fch := newFakeChannel()
	fch.ackErr = errors.New("consumer: session closed")
	sess := newTestSession(fch)

	c.handleDelivery(context.Background(), sess, amqp.Delivery{
		DeliveryTag: 5,
		Body:        mustMarshal(t, &stockstorev1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"}),
	})

	if acks := fch.acked(); len(acks) != 1 || acks[0] != 5 {
		t.Errorf("acks = %v, want exactly one attempt on tag 5 (no retry)", acks)
	}
	if nacks := fch.nacked(); len(nacks) != 0 {
		t.Errorf("nacks = %+v, want none", nacks)
	}
	if !strings.Contains(buf.String(), "ack delivery failed") {
		t.Errorf("log missing ack delivery failed:\n%s", buf.String())
	}
}

// --- log hygiene ---

// TestLogHygieneNeverLeaksSecrets runs a full happy-path delivery and proves
// the summary body, the report body, and the broker password never reach the
// log; the broker appears only in redacted form.
func TestLogHygieneNeverLeaksSecrets(t *testing.T) {
	const summary = "SECRET-SUMMARY-BODY confidential market analysis"
	const report = "SECRET-REPORT-BODY confidential Cora-9 rewrite"

	fch := newFakeChannel()
	fconn := &fakeConnection{ch: fch}
	dial, _ := scriptedDial(dialStep{conn: fconn})

	var buf syncBuffer
	summaries := &fakeSummaries{body: []byte(summary)}
	publisher := &fakePublisher{}
	c := newConsumer(testCfg(), summaries, &fakeRewriter{report: report}, publisher,
		slog.New(slog.NewJSONHandler(&buf, nil)), dial, (&sleepRecorder{}).sleepWith)

	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, c)

	waitFor(t, "consume to start", func() bool { return fch.consumeCount() > 0 })
	fch.deliveries <- amqp.Delivery{DeliveryTag: 4, RoutingKey: "stock.summary.available.v1",
		Body: mustMarshal(t, &stockstorev1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"})}
	waitFor(t, "publish", func() bool { return len(publisher.published()) == 1 })

	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run: %v", err)
	}

	logged := buf.String()
	for _, secret := range []string{secretPassword, summary, report, "SECRET-SUMMARY", "SECRET-REPORT"} {
		if strings.Contains(logged, secret) {
			t.Errorf("log leaked %q:\n%s", secret, logged)
		}
	}
	if !strings.Contains(logged, "amqp://192.168.1.48:5672/") {
		t.Errorf("log missing redacted broker URL:\n%s", logged)
	}
	if !strings.Contains(logged, "summary published on GoToSocial") {
		t.Errorf("log missing publish line:\n%s", logged)
	}
}
