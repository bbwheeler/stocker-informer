package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultS3Endpoint       = "http://192.168.1.31:3900"
	defaultS3Region         = "us-east-1"
	defaultS3TimeoutSeconds = 10
)

// Audience selects the GoToSocial visibility used when publishing.
type Audience string

const (
	AudienceGlobal Audience = "global"
	AudienceLocal  Audience = "local"
	AudienceDirect Audience = "direct"
)

// S3Config holds the Garage/S3 settings used to read summaries.
type S3Config struct {
	Endpoint   string
	Region     string
	Bucket     string
	AccessKey  string
	SecretKey  string
	PathPrefix string
	Timeout    time.Duration
}

type Config struct {
	KafkaBootstrapServers string
	KafkaTopic            string
	KafkaConsumerGroup    string

	GotoSocialInstance  string
	GotoSocialUser      string
	GotoSocialToken     string
	GotoSocialAudience  Audience
	GotoSocialRecipient string

	S3 S3Config
}

// Load reads configuration from the environment, applies defaults, and
// validates every value. All problems are returned as a single aggregated
// error that names each offending variable; secret values are never included.
func Load() (*Config, error) {
	cfg := &Config{
		KafkaBootstrapServers: strings.TrimSpace(getEnv("KAFKA_BOOTSTRAP_SERVERS", "")),
		KafkaTopic:            strings.TrimSpace(getEnv("KAFKA_TOPIC", "")),
		KafkaConsumerGroup:    strings.TrimSpace(getEnv("KAFKA_CONSUMER_GROUP", "")),

		GotoSocialInstance: strings.TrimSpace(getEnv("GOTOSOCIAL_INSTANCE", "")),
		GotoSocialUser:     strings.TrimSpace(getEnv("GOTOSOCIAL_USER", "")),
		GotoSocialToken:    strings.TrimSpace(getEnv("GOTOSOCIAL_TOKEN", "")),

		S3: S3Config{
			Endpoint:   strings.TrimSpace(getEnv("S3_ENDPOINT", defaultS3Endpoint)),
			Region:     strings.TrimSpace(getEnv("S3_REGION", defaultS3Region)),
			Bucket:     strings.TrimSpace(getEnv("S3_BUCKET", "")),
			AccessKey:  strings.TrimSpace(getEnv("S3_ACCESS_KEY", "")),
			SecretKey:  strings.TrimSpace(getEnv("S3_SECRET_KEY", "")),
			PathPrefix: normalizePrefix(getEnv("S3_PATH_PREFIX", "")),
		},
	}

	var errs []string
	add := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}

	required := []struct {
		name  string
		value string
	}{
		{"KAFKA_TOPIC", cfg.KafkaTopic},
		{"KAFKA_CONSUMER_GROUP", cfg.KafkaConsumerGroup},
		{"GOTOSOCIAL_INSTANCE", cfg.GotoSocialInstance},
		{"GOTOSOCIAL_USER", cfg.GotoSocialUser},
		{"GOTOSOCIAL_TOKEN", cfg.GotoSocialToken},
		{"S3_BUCKET", cfg.S3.Bucket},
		{"S3_ACCESS_KEY", cfg.S3.AccessKey},
		{"S3_SECRET_KEY", cfg.S3.SecretKey},
	}
	for _, v := range required {
		if v.value == "" {
			add("%s is required and must be non-empty", v.name)
		}
	}

	if cfg.KafkaBootstrapServers == "" {
		add("KAFKA_BOOTSTRAP_SERVERS is required and must be non-empty")
	} else if len(cfg.KafkaServers()) == 0 {
		add("KAFKA_BOOTSTRAP_SERVERS must contain at least one non-empty broker")
	}

	if cfg.GotoSocialInstance != "" {
		if err := validateURL("GOTOSOCIAL_INSTANCE", cfg.GotoSocialInstance); err != nil {
			add("%v", err)
		}
	}
	if err := validateURL("S3_ENDPOINT", cfg.S3.Endpoint); err != nil {
		add("%v", err)
	}

	audience := Audience(strings.ToLower(strings.TrimSpace(getEnv("GOTOSOCIAL_AUDIENCE", ""))))
	if audience == "" {
		audience = AudienceGlobal
	}
	switch audience {
	case AudienceGlobal, AudienceLocal, AudienceDirect:
		cfg.GotoSocialAudience = audience
	default:
		add("GOTOSOCIAL_AUDIENCE must be one of global|local|direct (got %q)", string(audience))
	}

	if cfg.GotoSocialAudience == AudienceDirect {
		recipient, err := normalizeRecipient(getEnv("GOTOSOCIAL_RECIPIENT", ""), cfg.GotoSocialInstance)
		if err != nil {
			add("%v", err)
		}
		cfg.GotoSocialRecipient = recipient
	}

	timeoutSeconds := defaultS3TimeoutSeconds
	if raw := strings.TrimSpace(getEnv("S3_TIMEOUT_SECONDS", "")); raw != "" {
		seconds, err := strconv.Atoi(raw)
		switch {
		case err != nil:
			add("S3_TIMEOUT_SECONDS must be an integer (got %q)", raw)
		case seconds <= 0:
			add("S3_TIMEOUT_SECONDS must be > 0 (got %d)", seconds)
		default:
			timeoutSeconds = seconds
		}
	}
	cfg.S3.Timeout = time.Duration(timeoutSeconds) * time.Second

	if len(errs) > 0 {
		return nil, fmt.Errorf("invalid configuration: %s", strings.Join(errs, "; "))
	}
	return cfg, nil
}

// normalizeRecipient converts an operator-supplied handle to user@domain form.
// One leading "@" is stripped; a handle without a domain borrows the hostname
// (no port) of the GoToSocial instance.
func normalizeRecipient(raw, instance string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("GOTOSOCIAL_RECIPIENT is required when GOTOSOCIAL_AUDIENCE=direct")
	}

	invalid := func() error {
		return fmt.Errorf("GOTOSOCIAL_RECIPIENT must be a fediverse handle (got %q)", trimmed)
	}

	handle := strings.TrimPrefix(trimmed, "@")
	if handle == "" || strings.Count(handle, "@") > 1 {
		return "", invalid()
	}

	if local, domain, found := strings.Cut(handle, "@"); found {
		if local == "" || domain == "" {
			return "", invalid()
		}
		return handle, nil
	}

	host := instanceHostname(instance)
	if host == "" {
		return "", invalid()
	}
	return handle + "@" + host, nil
}

func instanceHostname(instance string) string {
	u, err := url.Parse(instance)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

func validateURL(name, raw string) error {
	if u, err := url.Parse(raw); err == nil && u.Host != "" {
		switch strings.ToLower(u.Scheme) {
		case "http", "https":
			return nil
		}
	}
	return fmt.Errorf("%s must be an absolute URL with scheme and host (got %q)", name, raw)
}

// normalizePrefix trims slashes and drops empty segments from S3_PATH_PREFIX.
func normalizePrefix(raw string) string {
	parts := strings.Split(strings.TrimSpace(raw), "/")
	kept := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			kept = append(kept, part)
		}
	}
	return strings.Join(kept, "/")
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
