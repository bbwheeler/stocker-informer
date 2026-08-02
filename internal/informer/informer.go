package informer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	tsxhistoryv1 "github.com/example/tsx-history/gen/tsx/v1"
	"github.com/example/tsx-informer/internal/messenger"
	"google.golang.org/grpc"
)

const maxPageSize = 500

// HistoryServiceClientDef interface for ListLatestStocks, allowing it to be mocked in tests.
type HistoryServiceClientDef interface {
	ListLatestStocks(ctx context.Context, in *tsxhistoryv1.ListLatestStocksRequest, opts ...grpc.CallOption) (*tsxhistoryv1.ListLatestStocksResponse, error)
}

// Informer periodically fetches stock snapshots from tsx-history and publishes
// the top-scoring stocks via a publisher (e.g., GoToSocial).
type Informer struct {
	client       HistoryServiceClientDef
	messenger    messenger.Messenger
	publisher    messenger.Publisher
	maxResults   int
	exchangeFilter string
	interval     time.Duration
	log          *slog.Logger
}

func New(client tsxhistoryv1.HistoryServiceClient, m messenger.Messenger, p messenger.Publisher, maxResults int, exchangeFilter string, interval time.Duration, log *slog.Logger) *Informer {
	return &Informer{
		client:       client,
		messenger:    m,
		publisher:    p,
		maxResults:   maxResults,
		exchangeFilter: exchangeFilter,
		interval:     interval,
		log:          log,
	}
}

func (i *Informer) Run(ctx context.Context) error {
	i.log.Info("inform loop started", "messenger", i.messenger.Name(), "interval", i.interval.String())
	ticker := time.NewTicker(i.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := i.tick(ctx); err != nil {
				i.log.Error("inform tick failed", "error", err)
			}
		}
	}
}

func (i *Informer) tick(ctx context.Context) error {
	snapshots, err := i.fetchSnapshots(ctx)
	if err != nil {
		return fmt.Errorf("fetch snapshots: %w", err)
	}

	if len(snapshots) == 0 {
		i.log.Warn("no stocks found")
		return nil
	}

	data := i.messenger.CreateMessage(snapshots, i.maxResults)
	text := i.messenger.Format(data)

	if text == "" {
		i.log.Info("message content is empty after formatting, skipping publish")
		return nil
	}

	msg := messenger.Message{Type: i.messenger.MessageType(), Text: text}
	if err := i.publisher.Publish(ctx, msg); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	i.log.Info("inform completed", "messenger", i.messenger.Name())
	return nil
}

func (i *Informer) fetchSnapshots(ctx context.Context) ([]*tsxhistoryv1.StockSnapshot, error) {
	var allSnapshots []*tsxhistoryv1.StockSnapshot
	var nextToken string

	for {
		req := &tsxhistoryv1.ListLatestStocksRequest{
			PageSize:  maxPageSize,
			PageToken: nextToken,
		}

		resp, err := i.client.ListLatestStocks(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("list latest stocks page %q: %w", nextToken, err)
		}

		if resp == nil || len(resp.GetSnapshots()) == 0 {
			break
		}

		for _, s := range resp.GetSnapshots() {
			if i.exchangeFilter != "" && s.Exchange != i.exchangeFilter {
				continue
			}
			allSnapshots = append(allSnapshots, s)
		}

		nextToken = resp.GetNextPageToken()
		if nextToken == "" || len(resp.GetSnapshots()) < maxPageSize {
			break
		}
	}

	return allSnapshots, nil
}
