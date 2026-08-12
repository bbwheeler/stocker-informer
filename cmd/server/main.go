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

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

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

	consumer.Run(ctx)

	log.Info("shutting down")
	return nil
}
