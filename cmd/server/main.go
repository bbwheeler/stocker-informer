package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/example/stocker-informer/internal/config"
	"github.com/example/stocker-informer/internal/kafka"
	"github.com/example/stocker-informer/internal/messenger"
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

	publisher := messenger.NewGoToSocialPublisher(
		cfg.GotoSocialInstance,
		cfg.GotoSocialUser,
		cfg.GotoSocialToken,
		log,
	)

	var formatter messenger.Formatter = messenger.NewStockEventFormatter()

	consumer := kafka.New(
		cfg.KafkaServers(),
		cfg.KafkaTopic,
		cfg.KafkaConsumerGroup,
		formatter,
		publisher,
		log,
	)

	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		return fmt.Errorf("kafka consumer run: %w", err)
	}

	return nil
}
