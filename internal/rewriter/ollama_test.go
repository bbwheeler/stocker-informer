package rewriter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/example/stocker-informer/internal/config"
)

func newTestOllama(t *testing.T, handler http.HandlerFunc, mutate func(*config.OllamaConfig)) (*Ollama, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	cfg := config.OllamaConfig{
		Enabled:        true,
		BaseURL:        server.URL,
		Model:          "gemma4:26b",
		SystemPrompt:   config.DefaultOllamaSystemPrompt,
		Timeout:        2 * time.Second,
		Temperature:    0.7,
		MaxTokens:      1024,
		MaxOutputChars: 0,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return New(cfg), server
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, body string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write([]byte(body)); err != nil {
		t.Errorf("write response: %v", err)
	}
}

type decodedRequest struct {
	Model   string `json:"model"`
	Prompt  string `json:"prompt"`
	System  string `json:"system"`
	Stream  bool   `json:"stream"`
	Think   *bool  `json:"think"`
	Options struct {
		Temperature float64 `json:"temperature"`
		NumPredict  int     `json:"num_predict"`
	} `json:"options"`
}

func TestRewriteSendsGenerateRequest(t *testing.T) {
	const summary = "# AAPL\n\nRevenue up 12%"
	var (
		gotMethod      string
		gotPath        string
		gotContentType string
		gotRequest     decodedRequest
	)

	rw, _ := newTestOllama(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotContentType = r.Header.Get("Content-Type")
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSON(t, w, http.StatusOK, `{"response":"report","done":true,"done_reason":"stop"}`)
	}, nil)

	report, err := rw.Rewrite(context.Background(), summary)
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if report != "report" {
		t.Errorf("report = %q, want %q", report, "report")
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/api/generate" {
		t.Errorf("path = %q, want /api/generate", gotPath)
	}
	if gotContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotContentType)
	}
	if gotRequest.Model != "gemma4:26b" {
		t.Errorf("model = %q, want gemma4:26b", gotRequest.Model)
	}
	if gotRequest.System != config.DefaultOllamaSystemPrompt {
		t.Errorf("system = %q, want the default Cora-9 prompt", gotRequest.System)
	}
	if gotRequest.Stream {
		t.Error("stream = true, want false")
	}
	if gotRequest.Think == nil {
		t.Error("think is absent, want false")
	} else if *gotRequest.Think {
		t.Error("think = true, want false")
	}
	if gotRequest.Options.Temperature != 0.7 {
		t.Errorf("temperature = %v, want 0.7", gotRequest.Options.Temperature)
	}
	if gotRequest.Options.NumPredict != 1024 {
		t.Errorf("num_predict = %d, want 1024", gotRequest.Options.NumPredict)
	}
	if !strings.Contains(gotRequest.Prompt, "<summary>") || !strings.Contains(gotRequest.Prompt, "</summary>") {
		t.Errorf("prompt lacks <summary> delimiters: %q", gotRequest.Prompt)
	}
	if !strings.Contains(gotRequest.Prompt, summary) {
		t.Errorf("prompt does not contain the summary: %q", gotRequest.Prompt)
	}
}

func TestRewriteReturnsTrimmedReport(t *testing.T) {
	rw, _ := newTestOllama(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, `{"response":"  # Report\n","done":true}`)
	}, nil)

	report, err := rw.Rewrite(context.Background(), "summary")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if report != "# Report" {
		t.Errorf("report = %q, want %q", report, "# Report")
	}
}

func TestRewriteTrimsTrailingSlashBaseURL(t *testing.T) {
	var gotPath string
	rw, _ := newTestOllama(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		writeJSON(t, w, http.StatusOK, `{"response":"report","done":true}`)
	}, func(cfg *config.OllamaConfig) {
		cfg.BaseURL += "/"
	})

	if _, err := rw.Rewrite(context.Background(), "summary"); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if gotPath != "/api/generate" {
		t.Errorf("path = %q, want /api/generate", gotPath)
	}
}

func TestRewriteUsesDefaultSystemPromptWhenBlank(t *testing.T) {
	var gotRequest decodedRequest
	rw, _ := newTestOllama(t, func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writeJSON(t, w, http.StatusOK, `{"response":"report","done":true}`)
	}, func(cfg *config.OllamaConfig) {
		cfg.SystemPrompt = "   "
	})

	if _, err := rw.Rewrite(context.Background(), "summary"); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if gotRequest.System != config.DefaultOllamaSystemPrompt {
		t.Errorf("system = %q, want the default Cora-9 prompt", gotRequest.System)
	}
}

func TestRewriteRejectsNon2xx(t *testing.T) {
	rw, _ := newTestOllama(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusNotFound, `{"error":"model 'gemma4:26b' not found"}`)
	}, nil)

	_, err := rw.Rewrite(context.Background(), "SECRET-TICKER-DATA")
	if err == nil {
		t.Fatal("Rewrite succeeded, want an error")
	}
	for _, want := range []string{"404", "not found"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "SECRET-TICKER-DATA") {
		t.Errorf("error leaks the summary: %q", err)
	}
}

func TestRewriteRejectsMalformedJSON(t *testing.T) {
	rw, _ := newTestOllama(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("not-json")); err != nil {
			t.Errorf("write response: %v", err)
		}
	}, nil)

	_, err := rw.Rewrite(context.Background(), "summary")
	if err == nil {
		t.Fatal("Rewrite succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "decode Ollama response") {
		t.Errorf("error = %q, want it to mention decode Ollama response", err)
	}
}

func TestRewriteRejectsTruncatedBody(t *testing.T) {
	rw, _ := newTestOllama(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{"response":"partial","done":true`)); err != nil {
			t.Errorf("write response: %v", err)
		}
	}, nil)

	_, err := rw.Rewrite(context.Background(), "summary")
	if err == nil {
		t.Fatal("Rewrite succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "decode Ollama response") {
		t.Errorf("error = %q, want it to mention decode Ollama response", err)
	}
}

func TestRewriteRejectsIncompleteResponse(t *testing.T) {
	rw, _ := newTestOllama(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, `{"response":"partial","done":false}`)
	}, nil)

	_, err := rw.Rewrite(context.Background(), "summary")
	if err == nil {
		t.Fatal("Rewrite succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "did not complete") {
		t.Errorf("error = %q, want it to mention did not complete", err)
	}
}

func TestRewriteRejectsTruncatedOutput(t *testing.T) {
	rw, _ := newTestOllama(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, `{"response":"partial","done":true,"done_reason":"length"}`)
	}, nil)

	_, err := rw.Rewrite(context.Background(), "summary")
	if err == nil {
		t.Fatal("Rewrite succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "num_predict") {
		t.Errorf("error = %q, want it to mention num_predict", err)
	}
}

func TestRewriteRejectsEmptyOutput(t *testing.T) {
	rw, _ := newTestOllama(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, `{"response":"  \n\t","done":true}`)
	}, nil)

	_, err := rw.Rewrite(context.Background(), "summary")
	if err == nil {
		t.Fatal("Rewrite succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "empty output") {
		t.Errorf("error = %q, want it to mention empty output", err)
	}
}

func TestRewriteRejectsOverlongOutput(t *testing.T) {
	rw, _ := newTestOllama(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, `{"response":"12345678901","done":true}`)
	}, func(cfg *config.OllamaConfig) {
		cfg.MaxOutputChars = 10
	})

	_, err := rw.Rewrite(context.Background(), "summary")
	if err == nil {
		t.Fatal("Rewrite succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "OLLAMA_MAX_OUTPUT_CHARS") {
		t.Errorf("error = %q, want it to mention OLLAMA_MAX_OUTPUT_CHARS", err)
	}
}

func TestRewriteAcceptsOutputAtLimit(t *testing.T) {
	rw, _ := newTestOllama(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, `{"response":"1234567890","done":true}`)
	}, func(cfg *config.OllamaConfig) {
		cfg.MaxOutputChars = 10
	})

	report, err := rw.Rewrite(context.Background(), "summary")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if report != "1234567890" {
		t.Errorf("report = %q, want %q", report, "1234567890")
	}
}

func TestRewriteAllowsUnlimitedOutput(t *testing.T) {
	long := strings.Repeat("a", 5000)
	payload, err := json.Marshal(map[string]any{"response": long, "done": true})
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	rw, _ := newTestOllama(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, string(payload))
	}, func(cfg *config.OllamaConfig) {
		cfg.MaxOutputChars = 0
	})

	report, err := rw.Rewrite(context.Background(), "summary")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if report != long {
		t.Errorf("report length = %d, want 5000", len(report))
	}
}

func TestRewriteCountsRunesForLimit(t *testing.T) {
	rw, _ := newTestOllama(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, `{"response":"ééééé","done":true}`)
	}, func(cfg *config.OllamaConfig) {
		cfg.MaxOutputChars = 5
	})

	report, err := rw.Rewrite(context.Background(), "summary")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if report != "ééééé" {
		t.Errorf("report = %q, want %q", report, "ééééé")
	}
}

func TestRewriteHonorsContextCancellation(t *testing.T) {
	rw, _ := newTestOllama(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusOK, `{"response":"report","done":true}`)
	}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := rw.Rewrite(ctx, "summary")
	if err == nil {
		t.Fatal("Rewrite succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "call Ollama") {
		t.Errorf("error = %q, want it to mention call Ollama", err)
	}
}

func TestRewriteTimeout(t *testing.T) {
	blocked := make(chan struct{})
	rw, _ := newTestOllama(t, func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-blocked:
		}
	}, func(cfg *config.OllamaConfig) {
		cfg.Timeout = 50 * time.Millisecond
	})
	defer close(blocked)

	start := time.Now()
	_, err := rw.Rewrite(context.Background(), "summary")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Rewrite succeeded, want a timeout error")
	}
	if elapsed > 2*time.Second {
		t.Errorf("Rewrite took %v, want less than 2s", elapsed)
	}
	if !strings.Contains(err.Error(), "call Ollama") {
		t.Errorf("error = %q, want it to mention call Ollama", err)
	}
}

func TestRewriteErrorDoesNotLeakSummary(t *testing.T) {
	rw, _ := newTestOllama(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, http.StatusInternalServerError, `{"error":"boom"}`)
	}, nil)

	_, err := rw.Rewrite(context.Background(), "SECRET-TICKER-DATA")
	if err == nil {
		t.Fatal("Rewrite succeeded, want an error")
	}
	if strings.Contains(err.Error(), "SECRET-TICKER-DATA") {
		t.Errorf("error leaks the summary: %q", err)
	}
}
