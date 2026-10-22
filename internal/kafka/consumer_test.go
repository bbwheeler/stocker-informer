package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	stockstorev1 "git.wheeli.ca/brian/stocker-store/proto/v1"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	"github.com/example/stocker-informer/internal/summarystore"
)

type summaryRequest struct {
	exchange string
	symbol   string
}

// fakeSummaries records every read and returns a fixed body or error.
type fakeSummaries struct {
	body     []byte
	err      error
	requests []summaryRequest
}

func (f *fakeSummaries) ReadSummary(_ context.Context, exchange, symbol string) ([]byte, error) {
	f.requests = append(f.requests, summaryRequest{exchange: exchange, symbol: symbol})
	return f.body, f.err
}

// fakePublisher records every published text and returns a fixed error.
type fakePublisher struct {
	err   error
	texts []string
}

func (f *fakePublisher) Publish(_ context.Context, text string) error {
	f.texts = append(f.texts, text)
	return f.err
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func newTestConsumer(summaries SummaryReader, publisher *fakePublisher) *Consumer {
	return New([]string{"localhost:9092"}, "topic", "group", summaries, publisher, newTestLogger())
}

// handleAndCountCommits runs handleMessage and returns the number of commits.
func handleAndCountCommits(t *testing.T, c *Consumer, msg kafka.Message) int {
	t.Helper()

	commits := 0
	c.handleMessage(context.Background(), msg, func(kafka.Message) { commits++ })
	return commits
}

func mustMarshal(t *testing.T, stock *stockstorev1.Stock) []byte {
	t.Helper()

	data, err := proto.Marshal(stock)
	if err != nil {
		t.Fatalf("marshal stock: %v", err)
	}
	return data
}

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

	_, err := decodeStock(value)
	if err == nil {
		t.Fatal("expected error for empty symbol")
	}
	if !strings.Contains(err.Error(), "symbol") {
		t.Errorf("error %q does not name the symbol field", err)
	}
}

func TestDecodeStockRejectsEmptyExchange(t *testing.T) {
	value := mustMarshal(t, &stockstorev1.Stock{Symbol: "AAPL", Exchange: ""})

	_, err := decodeStock(value)
	if err == nil {
		t.Fatal("expected error for empty exchange")
	}
	if !strings.Contains(err.Error(), "exchange") {
		t.Errorf("error %q does not name the exchange field", err)
	}
}

func TestDecodeStockRejectsMalformed(t *testing.T) {
	if _, err := decodeStock([]byte{0xff, 0xff, 0xff, 0xff}); err == nil {
		t.Fatal("expected error for malformed protobuf")
	}
}

func TestHandleMessagePublishesSummary(t *testing.T) {
	const markdown = "# AAPL\n\nRevenue up **12%** year over year."

	summaries := &fakeSummaries{body: []byte(markdown)}
	publisher := &fakePublisher{}
	c := newTestConsumer(summaries, publisher)
	msg := kafka.Message{Offset: 42, Value: mustMarshal(t, &stockstorev1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"})}

	commits := handleAndCountCommits(t, c, msg)

	if len(summaries.requests) != 1 {
		t.Fatalf("store calls = %d, want 1", len(summaries.requests))
	}
	if got := summaries.requests[0]; got.exchange != "NASDAQ" || got.symbol != "AAPL" {
		t.Errorf("store request = %+v, want exchange NASDAQ symbol AAPL", got)
	}
	if len(publisher.texts) != 1 {
		t.Fatalf("publishes = %d, want 1", len(publisher.texts))
	}
	if publisher.texts[0] != markdown {
		t.Errorf("published text = %q, want %q", publisher.texts[0], markdown)
	}
	if commits != 1 {
		t.Errorf("commits = %d, want 1", commits)
	}
}

func TestHandleMessageSummaryNotFoundSkips(t *testing.T) {
	summaries := &fakeSummaries{err: summarystore.ErrNotFound}
	publisher := &fakePublisher{}
	c := newTestConsumer(summaries, publisher)
	msg := kafka.Message{Offset: 1, Value: mustMarshal(t, &stockstorev1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"})}

	commits := handleAndCountCommits(t, c, msg)

	if len(publisher.texts) != 0 {
		t.Errorf("publishes = %d, want 0", len(publisher.texts))
	}
	if commits != 1 {
		t.Errorf("commits = %d, want 1", commits)
	}
}

func TestHandleMessageStorageErrorSkips(t *testing.T) {
	summaries := &fakeSummaries{err: errors.New("boom")}
	publisher := &fakePublisher{}
	c := newTestConsumer(summaries, publisher)
	msg := kafka.Message{Offset: 2, Value: mustMarshal(t, &stockstorev1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"})}

	commits := handleAndCountCommits(t, c, msg)

	if len(publisher.texts) != 0 {
		t.Errorf("publishes = %d, want 0", len(publisher.texts))
	}
	if commits != 1 {
		t.Errorf("commits = %d, want 1", commits)
	}
}

func TestHandleMessageEmptySummarySkips(t *testing.T) {
	summaries := &fakeSummaries{body: []byte("  \n\t ")}
	publisher := &fakePublisher{}
	c := newTestConsumer(summaries, publisher)
	msg := kafka.Message{Offset: 3, Value: mustMarshal(t, &stockstorev1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"})}

	commits := handleAndCountCommits(t, c, msg)

	if len(publisher.texts) != 0 {
		t.Errorf("publishes = %d, want 0", len(publisher.texts))
	}
	if commits != 1 {
		t.Errorf("commits = %d, want 1", commits)
	}
}

func TestHandleMessagePublishErrorCommits(t *testing.T) {
	summaries := &fakeSummaries{body: []byte("# AAPL")}
	publisher := &fakePublisher{err: errors.New("publish boom")}
	c := newTestConsumer(summaries, publisher)
	msg := kafka.Message{Offset: 4, Value: mustMarshal(t, &stockstorev1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"})}

	commits := handleAndCountCommits(t, c, msg)

	if len(publisher.texts) != 1 {
		t.Errorf("publishes = %d, want 1", len(publisher.texts))
	}
	if commits != 1 {
		t.Errorf("commits = %d, want 1", commits)
	}
}

func TestHandleMessageDecodeErrorCommits(t *testing.T) {
	summaries := &fakeSummaries{body: []byte("# AAPL")}
	publisher := &fakePublisher{}
	c := newTestConsumer(summaries, publisher)
	msg := kafka.Message{Offset: 5, Value: []byte{0xff, 0xff, 0xff, 0xff}}

	commits := handleAndCountCommits(t, c, msg)

	if len(summaries.requests) != 0 {
		t.Errorf("store calls = %d, want 0", len(summaries.requests))
	}
	if len(publisher.texts) != 0 {
		t.Errorf("publishes = %d, want 0", len(publisher.texts))
	}
	if commits != 1 {
		t.Errorf("commits = %d, want 1", commits)
	}
}

func TestHandleMessagePassesTrimmedIdentity(t *testing.T) {
	summaries := &fakeSummaries{body: []byte("# AAPL")}
	publisher := &fakePublisher{}
	c := newTestConsumer(summaries, publisher)
	msg := kafka.Message{Offset: 6, Value: mustMarshal(t, &stockstorev1.Stock{Symbol: " AAPL ", Exchange: " NASDAQ "})}

	handleAndCountCommits(t, c, msg)

	if len(summaries.requests) != 1 {
		t.Fatalf("store calls = %d, want 1", len(summaries.requests))
	}
	if got := summaries.requests[0]; got.exchange != "NASDAQ" || got.symbol != "AAPL" {
		t.Errorf("store request = %+v, want trimmed exchange NASDAQ symbol AAPL", got)
	}
}
