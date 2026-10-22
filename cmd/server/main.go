package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/example/stocker-informer/internal/config"
	"github.com/example/stocker-informer/internal/kafka"
	"github.com/example/stocker-informer/internal/messenger"
	"github.com/example/stocker-informer/internal/summarystore"
)

var (
	Version   = "dev"
	Commit    = "none"
	BuildTime = "unknown"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(log); err != nil {
		log.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	log.Info("starting stocker-informer", "version", Version, "commit", Commit, "build_time", BuildTime)

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("configuration loaded",
		"kafka_topic", cfg.KafkaTopic,
		"s3_endpoint", cfg.S3.Endpoint,
		"s3_bucket", cfg.S3.Bucket,
		"audience", cfg.GotoSocialAudience,
	)

	store, err := summarystore.New(&cfg.S3)
	if err != nil {
		return fmt.Errorf("create summary store: %w", err)
	}
	defer func() { _ = store.Close() }()

	publisher := messenger.NewGoToSocialPublisher(
		cfg.GotoSocialInstance,
		cfg.GotoSocialUser,
		cfg.GotoSocialToken,
		cfg.GotoSocialAudience,
		cfg.GotoSocialRecipient,
		log,
	)

	consumer := kafka.New(
		cfg.KafkaServers(),
		cfg.KafkaTopic,
		cfg.KafkaConsumerGroup,
		store,
		publisher,
		log,
	)

	ready := new(atomic.Bool)
	ready.Store(true)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if ready.Load() {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"status":"ok","version":"%s"}`+"\n", Version)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, `{"status":"starting","version":"%s"}`+"\n", Version)
		}
	})
	server := &http.Server{Addr: ":8080", Handler: mux}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Warn("health check server error", "error", err)
		}
	}()

	if err := consumer.Run(ctx); err != nil {
		return fmt.Errorf("run consumer: %w", err)
	}

	log.Info("shutting down")
	return nil
}
