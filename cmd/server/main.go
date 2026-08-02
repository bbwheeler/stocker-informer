// Command tsx-informer periodically queries tsx-history for top scoring stocks
// and publishes the results on a GoToSocial instance.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/example/tsx-informer/internal/config"
	"github.com/example/tsx-informer/internal/informer"
	"github.com/example/tsx-informer/internal/messenger"

	tsxhistoryv1 "github.com/example/tsx-history/gen/tsx/v1"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(log); err != nil {
		log.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := grpc.Dial(cfg.HistoryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial history gRPC: %w", err)
	}
	defer conn.Close()

	client := tsxhistoryv1.NewHistoryServiceClient(conn)

	publisher := messenger.NewGoToSocialPublisher(
		cfg.GotoSocialInstance,
		cfg.GotoSocialUser,
		cfg.GotoSocialToken,
		log,
	)

	messengerInst := messenger.NewTopStocksMessenger(
		cfg.WeightsFinancials,
		cfg.WeightsSentiment,
		cfg.WeightsLeadership,
		cfg.WeightsTypeSentiment,
	)

	infr := informer.New(
		client,
		messengerInst,
		publisher,
		cfg.MaxResults,
		cfg.ExchangeFilter,
		cfg.InformerInterval,
		log,
	)

	if err := infr.Run(ctx); err != nil {
		return fmt.Errorf("informer run: %w", err)
	}

	return nil
}
