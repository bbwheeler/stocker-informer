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

	t.Setenv("OLLAMA_ENABLED", "")
	t.Setenv("OLLAMA_BASE_URL", "")
	t.Setenv("OLLAMA_MODEL", "")
	t.Setenv("OLLAMA_TIMEOUT_SECONDS", "")
	t.Setenv("OLLAMA_SYSTEM_PROMPT", "")
	t.Setenv("OLLAMA_TEMPERATURE", "")
	t.Setenv("OLLAMA_MAX_TOKENS", "")
	t.Setenv("OLLAMA_MAX_OUTPUT_CHARS", "")
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

func TestLoadOllamaDefaults(t *testing.T) {
	setValidEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}

	got := cfg.Ollama
	if !got.Enabled {
		t.Error("Ollama.Enabled = false, want true")
	}
	if got.BaseURL != "http://192.168.1.71:11434" {
		t.Errorf("Ollama.BaseURL = %q, want default", got.BaseURL)
	}
	if got.Model != "gemma4:26b" {
		t.Errorf("Ollama.Model = %q, want %q", got.Model, "gemma4:26b")
	}
	if got.SystemPrompt != DefaultOllamaSystemPrompt {
		t.Errorf("Ollama.SystemPrompt = %q, want DefaultOllamaSystemPrompt", got.SystemPrompt)
	}
	if got.Timeout != 120*time.Second {
		t.Errorf("Ollama.Timeout = %v, want 120s", got.Timeout)
	}
	if got.Temperature != 0.7 {
		t.Errorf("Ollama.Temperature = %v, want 0.7", got.Temperature)
	}
	if got.MaxTokens != 1024 {
		t.Errorf("Ollama.MaxTokens = %d, want 1024", got.MaxTokens)
	}
	if got.MaxOutputChars != 0 {
		t.Errorf("Ollama.MaxOutputChars = %d, want 0 (unlimited)", got.MaxOutputChars)
	}
}

func TestLoadOllamaOverrides(t *testing.T) {
	setValidEnv(t)
	t.Setenv("OLLAMA_ENABLED", "false")
	t.Setenv("OLLAMA_BASE_URL", "https://ollama.internal:11434/")
	t.Setenv("OLLAMA_MODEL", "llama3.3:70b")
	t.Setenv("OLLAMA_TIMEOUT_SECONDS", "45")
	t.Setenv("OLLAMA_SYSTEM_PROMPT", "  You are a test.  ")
	t.Setenv("OLLAMA_TEMPERATURE", "0.2")
	t.Setenv("OLLAMA_MAX_TOKENS", "256")
	t.Setenv("OLLAMA_MAX_OUTPUT_CHARS", "2000")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}

	want := OllamaConfig{
		Enabled:        false,
		BaseURL:        "https://ollama.internal:11434/",
		Model:          "llama3.3:70b",
		SystemPrompt:   "You are a test.",
		Timeout:        45 * time.Second,
		Temperature:    0.2,
		MaxTokens:      256,
		MaxOutputChars: 2000,
	}
	if cfg.Ollama != want {
		t.Errorf("Ollama = %+v, want %+v", cfg.Ollama, want)
	}
}

func TestLoadOllamaInvalidEnabled(t *testing.T) {
	setValidEnv(t)
	t.Setenv("OLLAMA_ENABLED", "maybe")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	for _, want := range []string{"OLLAMA_ENABLED", "boolean"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestLoadOllamaInvalidBaseURL(t *testing.T) {
	setValidEnv(t)
	t.Setenv("OLLAMA_BASE_URL", "ollama:11434")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	for _, want := range []string{"OLLAMA_BASE_URL", "absolute URL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
}

func TestLoadOllamaEmptyModel(t *testing.T) {
	setValidEnv(t)
	t.Setenv("OLLAMA_MODEL", "   ")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "OLLAMA_MODEL") {
		t.Errorf("error %q does not name OLLAMA_MODEL", err)
	}
}

func TestLoadOllamaInvalidTimeout(t *testing.T) {
	// "3601" is one over the cap; "120000000000" is 120s pasted as
	// nanoseconds, which would overflow time.Duration to a negative
	// value and silently disable the HTTP timeout.
	for _, raw := range []string{"abc", "0", "-1", "3601", "120000000000"} {
		t.Run(raw, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("OLLAMA_TIMEOUT_SECONDS", raw)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() with OLLAMA_TIMEOUT_SECONDS=%q error = nil, want error", raw)
			}
			if !strings.Contains(err.Error(), "OLLAMA_TIMEOUT_SECONDS") {
				t.Errorf("error %q does not name OLLAMA_TIMEOUT_SECONDS", err)
			}
		})
	}
}

func TestLoadOllamaTimeoutUpperBoundAccepted(t *testing.T) {
	setValidEnv(t)
	t.Setenv("OLLAMA_TIMEOUT_SECONDS", "3600")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
	if cfg.Ollama.Timeout != 3600*time.Second {
		t.Errorf("Ollama.Timeout = %v, want 1h0m0s", cfg.Ollama.Timeout)
	}
}

func TestLoadOllamaInvalidTemperature(t *testing.T) {
	for _, raw := range []string{"abc", "NaN", "+Inf", "-0.1", "2.1"} {
		t.Run(raw, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("OLLAMA_TEMPERATURE", raw)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() with OLLAMA_TEMPERATURE=%q error = nil, want error", raw)
			}
			if !strings.Contains(err.Error(), "OLLAMA_TEMPERATURE") {
				t.Errorf("error %q does not name OLLAMA_TEMPERATURE", err)
			}
		})
	}
}

func TestLoadOllamaInvalidMaxTokens(t *testing.T) {
	for _, raw := range []string{"abc", "0", "-1"} {
		t.Run(raw, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("OLLAMA_MAX_TOKENS", raw)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() with OLLAMA_MAX_TOKENS=%q error = nil, want error", raw)
			}
			if !strings.Contains(err.Error(), "OLLAMA_MAX_TOKENS") {
				t.Errorf("error %q does not name OLLAMA_MAX_TOKENS", err)
			}
		})
	}
}

func TestLoadOllamaInvalidMaxOutputChars(t *testing.T) {
	for _, raw := range []string{"abc", "-1"} {
		t.Run(raw, func(t *testing.T) {
			setValidEnv(t)
			t.Setenv("OLLAMA_MAX_OUTPUT_CHARS", raw)

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() with OLLAMA_MAX_OUTPUT_CHARS=%q error = nil, want error", raw)
			}
			if !strings.Contains(err.Error(), "OLLAMA_MAX_OUTPUT_CHARS") {
				t.Errorf("error %q does not name OLLAMA_MAX_OUTPUT_CHARS", err)
			}
		})
	}
}

func TestLoadOllamaBlankSystemPromptUsesDefault(t *testing.T) {
	setValidEnv(t)
	t.Setenv("OLLAMA_SYSTEM_PROMPT", "   ")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}
	if cfg.Ollama.SystemPrompt != DefaultOllamaSystemPrompt {
		t.Errorf("Ollama.SystemPrompt = %q, want DefaultOllamaSystemPrompt", cfg.Ollama.SystemPrompt)
	}
}

func TestLoadOllamaDisabledStillValidates(t *testing.T) {
	setValidEnv(t)
	t.Setenv("OLLAMA_ENABLED", "false")
	t.Setenv("OLLAMA_TEMPERATURE", "abc")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "OLLAMA_TEMPERATURE") {
		t.Errorf("error %q does not name OLLAMA_TEMPERATURE", err)
	}
}

func TestLoadOllamaInvalidValuesAggregated(t *testing.T) {
	setValidEnv(t)
	t.Setenv("OLLAMA_TIMEOUT_SECONDS", "0")
	t.Setenv("OLLAMA_MAX_TOKENS", "abc")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	msg := err.Error()
	if !strings.HasPrefix(msg, "invalid configuration: ") {
		t.Errorf("error %q is not an aggregated configuration error", msg)
	}
	for _, name := range []string{"OLLAMA_TIMEOUT_SECONDS", "OLLAMA_MAX_TOKENS"} {
		if !strings.Contains(msg, name) {
			t.Errorf("error %q does not name %s", msg, name)
		}
	}
}
