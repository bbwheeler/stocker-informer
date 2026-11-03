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
	"github.com/example/stocker-informer/internal/messenger"
	"github.com/example/stocker-informer/internal/rabbitmq"
	"github.com/example/stocker-informer/internal/rewriter"
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
		"rabbitmq_broker", config.RedactedAMQPURL(cfg.RabbitMQ.URL),
		"rabbitmq_exchange", cfg.RabbitMQ.InputExchange,
		"rabbitmq_exchange_type", cfg.RabbitMQ.InputExchangeType,
		"rabbitmq_routing_key", cfg.RabbitMQ.InputRoutingKey,
		"rabbitmq_queue", cfg.RabbitMQ.Queue,
		"rabbitmq_prefetch", cfg.RabbitMQ.Prefetch,
		"s3_endpoint", cfg.S3.Endpoint,
		"s3_bucket", cfg.S3.Bucket,
		"audience", cfg.GotoSocialAudience,
		"ollama_enabled", cfg.Ollama.Enabled,
		"ollama_base_url", cfg.Ollama.BaseURL,
		"ollama_model", cfg.Ollama.Model,
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

	var summaryRewriter rewriter.Rewriter
	if cfg.Ollama.Enabled {
		summaryRewriter = rewriter.New(cfg.Ollama)
	}

	consumer := rabbitmq.New(
		cfg.RabbitMQ,
		store,
		summaryRewriter,
		publisher,
		log,
	)

	ready := new(atomic.Bool)

	probe := rabbitmq.NewProbe(cfg.RabbitMQ, log)
	log.Info("rabbitmq connectivity probe started",
		"broker", config.RedactedAMQPURL(cfg.RabbitMQ.URL),
		"exchange", cfg.RabbitMQ.InputExchange,
		"queue", cfg.RabbitMQ.Queue,
	)
	go probe.Run(ctx, func() { ready.Store(true) })

	server := &http.Server{Addr: ":8080", Handler: newHealthMux(ready)}

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

// newHealthMux builds the /health mux. Readiness is sticky: it starts false
// and flips true once the RabbitMQ probe verifies connectivity, never back.
func newHealthMux(ready *atomic.Bool) http.Handler {
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
	return mux
}
