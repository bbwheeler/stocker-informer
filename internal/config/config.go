package config

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	KafkaBootstrapServers string
	KafkaTopic            string
	KafkaConsumerGroup    string

	GotoSocialInstance string
	GotoSocialUser     string
	GotoSocialToken    string
}

func Load() (*Config, error) {
	cfg := &Config{
		KafkaBootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", ""),
		KafkaTopic:            getEnv("KAFKA_TOPIC", ""),
		KafkaConsumerGroup:    getEnv("KAFKA_CONSUMER_GROUP", ""),

		GotoSocialInstance: getEnv("GOTOSOCIAL_INSTANCE", ""),
		GotoSocialUser:     getEnv("GOTOSOCIAL_USER", ""),
		GotoSocialToken:    getEnv("GOTOSOCIAL_TOKEN", ""),
	}

	if cfg.KafkaBootstrapServers == "" {
		return nil, fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS is required")
	}
	if cfg.KafkaTopic == "" {
		return nil, fmt.Errorf("KAFKA_TOPIC is required")
	}
	if cfg.KafkaConsumerGroup == "" {
		return nil, fmt.Errorf("KAFKA_CONSUMER_GROUP is required")
	}

	if cfg.GotoSocialInstance == "" || cfg.GotoSocialUser == "" || cfg.GotoSocialToken == "" {
		return nil, fmt.Errorf("GOTOSOCIAL_INSTANCE, GOTOSOCIAL_USER, and GOTOSOCIAL_TOKEN are all required")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func (c *Config) KafkaServers() []string {
	if c.KafkaBootstrapServers == "" {
		return nil
	}
	servers := strings.Split(c.KafkaBootstrapServers, ",")
	result := make([]string, 0, len(servers))
	for _, s := range servers {
		s = strings.TrimSpace(s)
		if s != "" {
			result = append(result, s)
		}
	}
	return result
}
