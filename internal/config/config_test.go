package config

import (
	"strings"
	"testing"
	"time"
)

const (
	testInstance = "https://wheeli.social"
	testToken    = "test-token-value"
	testAccess   = "test-access-key"
	testSecret   = "test-secret-key"
)

// setValidEnv sets every variable the loader reads, so tests do not depend on
// the caller's environment. Optional variables are explicitly cleared.
func setValidEnv(t *testing.T) {
	t.Helper()

	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	t.Setenv("KAFKA_TOPIC", "stock.summary.available.v1")
	t.Setenv("KAFKA_CONSUMER_GROUP", "stocker-informer")

	t.Setenv("GOTOSOCIAL_INSTANCE", testInstance)
	t.Setenv("GOTOSOCIAL_USER", "bot")
	t.Setenv("GOTOSOCIAL_TOKEN", testToken)
	t.Setenv("GOTOSOCIAL_AUDIENCE", "")
	t.Setenv("GOTOSOCIAL_RECIPIENT", "")

	t.Setenv("S3_ENDPOINT", "")
	t.Setenv("S3_REGION", "")
	t.Setenv("S3_BUCKET", "stocks")
	t.Setenv("S3_ACCESS_KEY", testAccess)
	t.Setenv("S3_SECRET_KEY", testSecret)
	t.Setenv("S3_PATH_PREFIX", "")
	t.Setenv("S3_TIMEOUT_SECONDS", "")
}

func TestLoadDefaults(t *testing.T) {
	setValidEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}

	if cfg.GotoSocialAudience != AudienceGlobal {
		t.Errorf("GotoSocialAudience = %q, want %q", cfg.GotoSocialAudience, AudienceGlobal)
	}
	if cfg.GotoSocialRecipient != "" {
		t.Errorf("GotoSocialRecipient = %q, want empty", cfg.GotoSocialRecipient)
	}
	if cfg.S3.Endpoint != "http://192.168.1.31:3900" {
		t.Errorf("S3.Endpoint = %q, want default", cfg.S3.Endpoint)
	}
	if cfg.S3.Region != "us-east-1" {
		t.Errorf("S3.Region = %q, want %q", cfg.S3.Region, "us-east-1")
	}
	if cfg.S3.PathPrefix != "" {
		t.Errorf("S3.PathPrefix = %q, want empty", cfg.S3.PathPrefix)
	}
	if cfg.S3.Timeout != 10*time.Second {
		t.Errorf("S3.Timeout = %v, want 10s", cfg.S3.Timeout)
	}
	if cfg.S3.Bucket != "stocks" {
		t.Errorf("S3.Bucket = %q, want %q", cfg.S3.Bucket, "stocks")
	}
	if cfg.S3.AccessKey != testAccess {
		t.Errorf("S3.AccessKey = %q, want %q", cfg.S3.AccessKey, testAccess)
	}
	if cfg.S3.SecretKey != testSecret {
		t.Errorf("S3.SecretKey = %q, want %q", cfg.S3.SecretKey, testSecret)
	}
}

func TestLoadMissingRequired(t *testing.T) {
	required := []string{
		"KAFKA_BOOTSTRAP_SERVERS",
		"KAFKA_TOPIC",
		"KAFKA_CONSUMER_GROUP",
		"GOTOSOCIAL_INSTANCE",
		"GOTOSOCIAL_USER",
		"GOTOSOCIAL_TOKEN",
		"S3_BUCKET",
		"S3_ACCESS_KEY",
		"S3_SECRET_KEY",
	}

	for _, name := range required {
		t.Run(name, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv(name, "")

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() error = nil, want error for missing %s", name)
			}
			if !strings.Contains(err.Error(), name) {
				t.Errorf("error %q does not name %s", err, name)
			}
			if !strings.Contains(err.Error(), "required") {
				t.Errorf("error %q does not say required", err)
			}
		})
	}
}

func TestLoadInvalidAudience(t *testing.T) {
	setValidEnv(t)
	t.Setenv("GOTOSOCIAL_AUDIENCE", "friends")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "GOTOSOCIAL_AUDIENCE") {
		t.Errorf("error %q does not name GOTOSOCIAL_AUDIENCE", err)
	}
	if !strings.Contains(err.Error(), "global|local|direct") {
		t.Errorf("error %q does not list valid audiences", err)
	}
}

func TestLoadDirectRequiresRecipient(t *testing.T) {
	setValidEnv(t)
	t.Setenv("GOTOSOCIAL_AUDIENCE", "direct")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "GOTOSOCIAL_RECIPIENT") {
		t.Errorf("error %q does not name GOTOSOCIAL_RECIPIENT", err)
	}
}

func TestLoadRecipientNormalization(t *testing.T) {
	tests := []struct {
		name      string
		recipient string
		instance  string
		want      string
	}{
		{
			name:      "full handle",
			recipient: "papa@wheeli.social",
			want:      "papa@wheeli.social",
		},
		{
			name:      "leading at stripped",
			recipient: "@papa@wheeli.social",
			want:      "papa@wheeli.social",
		},
		{
			name:      "username only uses instance host",
			recipient: "papa",
			instance:  testInstance,
			want:      "papa@wheeli.social",
		},
		{
			name:      "username only drops instance port",
			recipient: "papa",
			instance:  "https://wheeli.social:8443",
			want:      "papa@wheeli.social",
		},
		{
			name:      "surrounding whitespace trimmed",
			recipient: "  papa@wheeli.social  ",
			want:      "papa@wheeli.social",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("GOTOSOCIAL_AUDIENCE", "direct")
			t.Setenv("GOTOSOCIAL_RECIPIENT", tc.recipient)
			if tc.instance != "" {
				t.Setenv("GOTOSOCIAL_INSTANCE", tc.instance)
			}

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v, want nil", err)
			}
			if cfg.GotoSocialRecipient != tc.want {
				t.Errorf("GotoSocialRecipient = %q, want %q", cfg.GotoSocialRecipient, tc.want)
			}
		})
	}
}

func TestLoadInvalidRecipient(t *testing.T) {
	setValidEnv(t)
	t.Setenv("GOTOSOCIAL_AUDIENCE", "direct")
	t.Setenv("GOTOSOCIAL_RECIPIENT", "a@b@c")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "GOTOSOCIAL_RECIPIENT") {
		t.Errorf("error %q does not name GOTOSOCIAL_RECIPIENT", err)
	}
}

func TestLoadRecipientIgnoredWhenNotDirect(t *testing.T) {
	setValidEnv(t)
	t.Setenv("GOTOSOCIAL_AUDIENCE", "local")
	t.Setenv("GOTOSOCIAL_RECIPIENT", "papa@wheeli.social")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
	if cfg.GotoSocialRecipient != "" {
		t.Errorf("GotoSocialRecipient = %q, want empty for local audience", cfg.GotoSocialRecipient)
	}
}

func TestLoadInvalidAudienceURL(t *testing.T) {
	setValidEnv(t)
	t.Setenv("GOTOSOCIAL_INSTANCE", "not-a-url")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "GOTOSOCIAL_INSTANCE") {
		t.Errorf("error %q does not name GOTOSOCIAL_INSTANCE", err)
	}
	if !strings.Contains(err.Error(), "absolute URL") {
		t.Errorf("error %q does not mention absolute URL", err)
	}
}

func TestLoadInvalidS3Endpoint(t *testing.T) {
	setValidEnv(t)
	t.Setenv("S3_ENDPOINT", "garage:3900")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "S3_ENDPOINT") {
		t.Errorf("error %q does not name S3_ENDPOINT", err)
	}
}

func TestLoadS3Timeout(t *testing.T) {
	for _, raw := range []string{"abc", "0", "-1"} {
		t.Run(raw, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("S3_TIMEOUT_SECONDS", raw)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() with S3_TIMEOUT_SECONDS=%q error = nil, want error", raw)
			}
			if !strings.Contains(err.Error(), "S3_TIMEOUT_SECONDS") {
				t.Errorf("error %q does not name S3_TIMEOUT_SECONDS", err)
			}
		})
	}
}

func TestLoadS3TimeoutValue(t *testing.T) {
	setValidEnv(t)
	t.Setenv("S3_TIMEOUT_SECONDS", "25")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
	if cfg.S3.Timeout != 25*time.Second {
		t.Errorf("S3.Timeout = %v, want 25s", cfg.S3.Timeout)
	}
}

func TestLoadS3PathPrefixNormalized(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{"corp", "corp"},
		{"/corp/", "corp"},
		{"corp//summaries/", "corp/summaries"},
		{"/", ""},
	}

	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("S3_PATH_PREFIX", tc.raw)

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v, want nil", err)
			}
			if cfg.S3.PathPrefix != tc.want {
				t.Errorf("S3.PathPrefix = %q, want %q", cfg.S3.PathPrefix, tc.want)
			}
		})
	}
}

func TestLoadBrokers(t *testing.T) {
	setValidEnv(t)
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "a:1, b:2,,")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
	got := cfg.KafkaServers()
	want := []string{"a:1", "b:2"}
	if len(got) != len(want) {
		t.Fatalf("KafkaServers() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("KafkaServers()[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	t.Run("no non-empty broker", func(t *testing.T) {
		setValidEnv(t)
		t.Setenv("KAFKA_BOOTSTRAP_SERVERS", ",")

		if _, err := Load(); err == nil {
			t.Fatal("Load() error = nil, want error")
		} else if !strings.Contains(err.Error(), "KAFKA_BOOTSTRAP_SERVERS") {
			t.Errorf("error %q does not name KAFKA_BOOTSTRAP_SERVERS", err)
		}
	})
}

func TestLoadAggregatesErrors(t *testing.T) {
	setValidEnv(t)
	t.Setenv("KAFKA_TOPIC", "")
	t.Setenv("S3_BUCKET", "")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "invalid configuration:") {
		t.Errorf("error %q is not aggregated", msg)
	}
	for _, name := range []string{"KAFKA_TOPIC", "S3_BUCKET"} {
		if !strings.Contains(msg, name) {
			t.Errorf("error %q does not name %s", msg, name)
		}
	}
}

func TestLoadErrorsDoNotLeakSecrets(t *testing.T) {
	setValidEnv(t)
	t.Setenv("KAFKA_TOPIC", "")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	for _, secret := range []string{testToken, testAccess, testSecret} {
		if strings.Contains(err.Error(), secret) {
			t.Errorf("error %q leaks a secret value", err)
		}
	}
}
