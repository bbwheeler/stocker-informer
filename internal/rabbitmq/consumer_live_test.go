package rabbitmq

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	stockstorev1 "git.wheeli.ca/brian/stocker-store/proto/v1"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	"github.com/example/stocker-informer/internal/config"
)

const (
	// liveTestEnv gates the live test. Any value other than "1" keeps it
	// skipped, so the normal suite stays offline.
	liveTestEnv = "RABBITMQ_LIVE_TEST"

	// defaultLiveBroker is the fleet's shared test broker, matching the
	// RABBITMQ_URL default in config.
	defaultLiveBroker = "amqp://stocker:dYO81E36p5KOON@192.168.1.48:5672/"
)

// TestLiveConsumer runs the real broker flow end to end: it declares a
// unique timestamped direct exchange and queue, starts Consumer.Run with fake
// collaborators, publishes one protobuf Stock, and asserts the publisher sees
// the summary exactly once. The unique topology means a live run can never
// touch the shared stock.summary exchange or the stocker-informer queue, and
// everything is deleted in cleanup. Skipped unless RABBITMQ_LIVE_TEST=1.
func TestLiveConsumer(t *testing.T) {
	if os.Getenv(liveTestEnv) != "1" {
		t.Skipf("set %s=1 to run the live RabbitMQ consumer test", liveTestEnv)
	}

	rawURL := strings.TrimSpace(os.Getenv("RABBITMQ_URL"))
	if rawURL == "" {
		rawURL = defaultLiveBroker
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	cfg := config.RabbitMQConfig{
		URL:               rawURL,
		InputExchange:     "stocker-informer-live-ex-" + suffix,
		InputExchangeType: "direct",
		InputRoutingKey:   "stocker-informer-live." + suffix,
		Queue:             "stocker-informer-live-" + suffix,
		Prefetch:          1,
		ConnectTimeout:    10 * time.Second,
	}

	conn, err := amqp.Dial(rawURL)
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		t.Fatalf("open channel: %v", err)
	}
	t.Cleanup(func() {
		_, _ = ch.QueueDelete(cfg.Queue, false, false, false)
		_ = ch.ExchangeDelete(cfg.InputExchange, false, false)
		_ = ch.Close()
		_ = conn.Close()
	})

	// Declare the topology exactly as the consumer will, so the test owns
	// the objects it deletes and the consumer's declares are idempotent.
	if err := ch.ExchangeDeclare(cfg.InputExchange, cfg.InputExchangeType, true, false, false, false, nil); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	if _, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, nil); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	if err := ch.QueueBind(cfg.Queue, cfg.InputRoutingKey, cfg.InputExchange, false, nil); err != nil {
		t.Fatalf("bind queue: %v", err)
	}

	const summary = "# LIVE\n\nLive consumer integration summary, safe to delete."
	summaries := &fakeSummaries{body: []byte(summary)}
	publisher := &fakePublisher{}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	done := runAsync(ctx, New(cfg, summaries, identityRewriter{}, publisher, logger))

	body, err := proto.Marshal(&stockstorev1.Stock{Symbol: "LIVE", Exchange: "NASDAQ"})
	if err != nil {
		cancel()
		t.Fatalf("marshal stock: %v", err)
	}
	if err := ch.PublishWithContext(context.Background(), cfg.InputExchange, cfg.InputRoutingKey, false, false, amqp.Publishing{
		ContentType:  "application/x-protobuf",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}); err != nil {
		cancel()
		t.Fatalf("publish stock: %v", err)
	}

	waitFor(t, "the consumer to publish the summary", func() bool {
		return len(publisher.published()) >= 1
	})
	// Give a stray duplicate a moment to arrive before asserting exactly
	// one publication.
	time.Sleep(500 * time.Millisecond)

	published := publisher.published()
	if len(published) != 1 {
		t.Errorf("published %d texts, want exactly 1: %q", len(published), published)
	}
	if got := published[0]; got != summary {
		t.Errorf("published = %q, want the summary %q", got, summary)
	}

	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
}
