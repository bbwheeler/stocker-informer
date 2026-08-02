package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	InformerInterval time.Duration

	HistoryAddr          string
	ExchangeFilter       string
	MaxResults           int
	WeightsFinancials    float64
	WeightsSentiment     float64
	WeightsLeadership    float64
	WeightsTypeSentiment float64

	GotoSocialInstance string
	GotoSocialUser     string
	GotoSocialToken    string
}

func Load() (*Config, error) {
	cfg := &Config{
		InformerInterval: getEnvDuration("INFORMER_INTERVAL", 1*time.Hour),

		HistoryAddr:          getEnv("HISTORY_ADDR", "localhost:50053"),
		ExchangeFilter:       getEnv("EXCHANGE_FILTER", ""),
		MaxResults:           getEnvInt("MAX_RESULTS", 5),
		WeightsFinancials:   getEnvFloat64("WEIGHTS_FINANCIALS", 0.25),
		WeightsSentiment:    getEnvFloat64("WEIGHTS_SENTIMENT", 0.25),
		WeightsLeadership:   getEnvFloat64("WEIGHTS_LEADERSHIP", 0.25),
		WeightsTypeSentiment: getEnvFloat64("WEIGHTS_TYPE_SENTIMENT", 0.25),

		GotoSocialInstance: getEnv("GOTOSOCIAL_INSTANCE", ""),
		GotoSocialUser:     getEnv("GOTOSOCIAL_USER", ""),
		GotoSocialToken:    getEnv("GOTOSOCIAL_TOKEN", ""),
	}

	if cfg.InformerInterval <= 0 || cfg.InformerInterval > 24*time.Hour {
		return nil, fmt.Errorf("INFORMER_INTERVAL must be >0 and <=24h, got %s", cfg.InformerInterval)
	}

	if err := validateWeights(cfg.WeightsFinancials, cfg.WeightsSentiment, cfg.WeightsLeadership, cfg.WeightsTypeSentiment); err != nil {
		return nil, fmt.Errorf("weights config: %w", err)
	}

	if cfg.GotoSocialInstance == "" || cfg.GotoSocialUser == "" || cfg.GotoSocialToken == "" {
		return nil, fmt.Errorf("GOTOSOCIAL_INSTANCE, GOTOSOCIAL_USER, and GOTOSOCIAL_TOKEN are all required")
	}

	return cfg, nil
}

const (
	minValidWeight = 0.0
	maxValidWeight = 1.0
)

func validateWeights(f, s, l, t float64) error {
	for _, w := range []float64{f, s, l, t} {
		if w < minValidWeight || w > maxValidWeight {
			return fmt.Errorf("each weight must be in range 0-1, got %f", w)
		}
	}
	total := f + s + l + t
	if total <= 0 || total > 1.0 {
		return fmt.Errorf("weights sum must be between 0 and 1, got %f", total)
	}
	return nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvFloat64(key string, fallback float64) float64 {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
