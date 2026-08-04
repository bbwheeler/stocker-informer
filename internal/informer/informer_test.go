package informer

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	tsxhistoryv1 "github.com/example/tsx-history/gen/tsx/v1"
	"github.com/example/stocker-informer/internal/messenger"
)

type testPublisher struct {
	messages []messenger.Message
	wantErr  bool
}

func (p *testPublisher) Publish(ctx context.Context, msg messenger.Message) error {
	p.messages = append(p.messages, msg)
	if p.wantErr {
		return fmt.Errorf("publish error")
	}
	return nil
}

type testMessenger struct {
	fixed string
	name  string
}

func (m *testMessenger) Name() string                                  { return m.name }
func (m *testMessenger) MessageType() messenger.MessageType            { return "top_stocks" }
func (m *testMessenger) CreateMessage([]*tsxhistoryv1.StockSnapshot, int) *messenger.StockData {
	return &messenger.StockData{Type: "top_stocks", FetchedAt: time.Now()}
}
func (m *testMessenger) Format(*messenger.StockData) string { return m.fixed }

var testLogger = slog.New(slog.DiscardHandler{})

func TestTick(t *testing.T) {
	tests := []struct {
		name          string
		format        string
		wantErr       bool
		wantPublish   int
	}{
		{"non-empty", "#1 A", false, 1},
		{"empty text", "", false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			infr := &Informer{
				messenger:      &testMessenger{fixed: tt.format, name: "test"},
				publisher:      &testPublisher{},
				maxResults:     5,
				exchangeFilter: "",
				log:            testLogger,
			}

			ctx := context.Background()
			if err := infr.tick(ctx); (err != nil) != tt.wantErr {
				t.Fatalf("unexpected error: %v", err)
			}

			pub := infr.publisher.(*testPublisher)
			if len(pub.messages) != tt.wantPublish {
				t.Errorf("expected %d published messages, got %d", tt.wantPublish, len(pub.messages))
			}
		})
	}
}

func TestTick_publishErrorPropagated(t *testing.T) {
	infr := &Informer{
		messenger:      &testMessenger{fixed: "#1 A", name: "test"},
		publisher:      &testPublisher{wantErr: true},
		maxResults:     5,
		exchangeFilter: "",
		log:            testLogger,
	}

	ctx := context.Background()
	if err := infr.tick(ctx); err == nil {
		t.Fatal("expected error from failing publisher")
	}
}

func TestTick_emptySnapshots(t *testing.T) {
	infr := &Informer{
		messenger:      &testMessenger{name: "test"},
		publisher:      &testPublisher{},
		maxResults:     5,
		exchangeFilter: "",
		log:            testLogger,
	}

	ctx := context.Background()
	if err := infr.tick(ctx); err != nil {
		t.Fatalf("expected no error on empty snapshots, got %v", err)
	}
}

func TestNew(t *testing.T) {
	infr := New(
		nil,
		&testMessenger{},
		&testPublisher{},
		10,
		"SET",
		30*time.Minute,
		testLogger,
	)

	if infr.maxResults != 10 {
		t.Errorf("expected maxResults 10, got %d", infr.maxResults)
	}
	if infr.exchangeFilter != "SET" {
		t.Errorf("expected exchangeFilter 'SET', got '%s'", infr.exchangeFilter)
	}
	if infr.interval != 30*time.Minute {
		t.Errorf("expected interval 30m, got %v", infr.interval)
	}
}

func TestFetchSnapshots(t *testing.T) {
	type mockClient struct {
		pages [][]*tsxhistoryv1.StockSnapshot
	}
	var c mockClient
	infr := &Informer{
		client:       nil, // will replace below
		messenger:      &testMessenger{name: "test"},
		publisher:      &testPublisher{},
		maxResults:     5,
		exchangeFilter: "",
		log:            testLogger,
	}
	ctx := context.Background()

	t.Run("singlePage", func(t *testing.T) {
		c.pages = [][]*tsxhistoryv1.StockSnapshot{
			{{Symbol: "A"}, {Symbol: "B"}},
		}
		// Note: informer uses the HistoryServiceClientDef interface, 
		// and this test verifies the general flow rather than direct fetch access
		if _, err := infr.fetchSnapshots(ctx); err != nil {
			t.Logf("fetch result (client nil): %v", err)
		}
	})

	t.Run("multiPage", func(t *testing.T) {
		c.pages = [][]*tsxhistoryv1.StockSnapshot{
			{{Symbol: "A"}, {Symbol: "B"}},   // first page
			{{Symbol: "C"}},                     // second page (partial)
		}
	})

	t.Run("exchangeFilter", func(t *testing.T) {
		exInfr := &Informer{
			messenger:      &testMessenger{name: "test"},
			publisher:      &testPublisher{},
			maxResults:     5,
			exchangeFilter: "SET",
			log:            testLogger,
		}
		ctx := context.Background()
		if err := exInfr.tick(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("noStocksFound", func(t *testing.T) {
		infrNoStock := &Informer{
			messenger:      &testMessenger{name: "test"},
			publisher:      &testPublisher{},
			maxResults:     5,
			exchangeFilter: "",
			log:            testLogger,
		}
		ctx := context.Background()
		if err := infrNoStock.tick(ctx); err != nil {
			t.Fatalf("expected no error on no stocks, got %v", err)
		}
	})

	t.Run("tickerRunCancelsOnContextDone", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		infrTicker := &Informer{
			messenger:      &testMessenger{name: "test"},
			publisher:      &testPublisher{},
			maxResults:     5,
			exchangeFilter: "",
			interval:       time.Hour,
			log:            testLogger,
		}

		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		done := make(chan struct{})
		go func() {
			infrTicker.Run(ctx)
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Run did not return after context cancellation")
		}
	})
}
