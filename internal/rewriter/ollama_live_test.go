package rewriter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/example/stocker-informer/internal/config"
)

const (
	liveDefaultBaseURL = "http://192.168.1.71:11434"
	liveDefaultModel   = "gemma4:26b"
)

const liveSampleSummary = `# AAPL — Apple Inc.

Apple reported quarterly revenue of $94.9B, up 6% year over year. Services
revenue reached $24.2B, up 14%. Gross margin was 46.3%. Guidance implies
steady hardware demand into the next quarter.`

// TestOllamaLive calls the real Ollama host. It is skipped unless
// OLLAMA_LIVE_TEST=1, so the normal suite stays offline.
func TestOllamaLive(t *testing.T) {
	if os.Getenv("OLLAMA_LIVE_TEST") != "1" {
		t.Skip("set OLLAMA_LIVE_TEST=1 to call the real Ollama instance")
	}

	baseURL := os.Getenv("OLLAMA_BASE_URL")
	if baseURL == "" {
		baseURL = liveDefaultBaseURL
	}
	model := os.Getenv("OLLAMA_MODEL")
	if model == "" {
		model = liveDefaultModel
	}

	checkLiveModelPresent(t, baseURL, model)

	cfg := config.OllamaConfig{
		Enabled:        true,
		BaseURL:        baseURL,
		Model:          model,
		SystemPrompt:   config.DefaultOllamaSystemPrompt,
		Timeout:        5 * time.Minute,
		Temperature:    0.2,
		MaxTokens:      1024,
		MaxOutputChars: 0,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	report, err := New(cfg).Rewrite(ctx, liveSampleSummary)
	if err != nil {
		t.Fatalf("live rewrite via %s model %s: %v", baseURL, model, err)
	}

	// Plausibility checks: a non-empty report that is neither an echo of the
	// source nor a raw prompt dump, still names the company, and is long
	// enough to be more than a stub. No exact-text matching.
	trimmedInput := strings.TrimSpace(liveSampleSummary)
	switch {
	case strings.TrimSpace(report) == "":
		t.Fatal("live rewrite returned empty output")
	case report == trimmedInput:
		t.Error("live rewrite returned the input unchanged")
	case strings.Contains(report, "<summary>") || strings.Contains(report, "</summary>"):
		t.Error("live rewrite echoed the prompt delimiters")
	case !strings.Contains(strings.ToLower(report), "apple") && !strings.Contains(strings.ToLower(report), "aapl"):
		t.Errorf("live report does not mention the company: %q", livePreview(report, 0, 200))
	case utf8.RuneCountInString(report) < 80:
		t.Errorf("live report is only %d runes: %q", utf8.RuneCountInString(report), report)
	}

	t.Logf("input %d chars, report %d chars", utf8.RuneCountInString(liveSampleSummary), utf8.RuneCountInString(report))
	t.Logf("report preview: %q", livePreview(report, 120, 120))
}

// checkLiveModelPresent confirms the configured model is pulled. A transport
// error is left to Rewrite so the reported failure names the generate call;
// a reachable host missing the model fails here with the available names.
func checkLiveModelPresent(t *testing.T, baseURL, model string) {
	t.Helper()

	endpoint := strings.TrimRight(baseURL, "/") + "/api/tags"
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(endpoint)
	if err != nil {
		t.Logf("could not list models at %s: %v", endpoint, err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Logf("could not list models at %s: status %d, err %v", endpoint, resp.StatusCode, err)
		return
	}

	var payload struct {
		Models []struct {
			Name  string `json:"name"`
			Model string `json:"model"`
		} `json:"models"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Logf("could not decode models at %s: %v", endpoint, err)
		return
	}

	names := make([]string, 0, len(payload.Models))
	for _, m := range payload.Models {
		name := m.Name
		if name == "" {
			name = m.Model
		}
		names = append(names, name)
		if strings.EqualFold(name, model) || strings.EqualFold(m.Model, model) {
			return
		}
	}
	t.Fatalf("model %q is not pulled at %s; available models: %v", model, baseURL, names)
}

// preview renders at most n runes from each end of s, joined by an ellipsis
// when the middle is omitted.
func livePreview(s string, head, tail int) string {
	r := []rune(s)
	if len(r) <= head+tail {
		return s
	}
	if head == 0 {
		return fmt.Sprintf("… %s", string(r[len(r)-tail:]))
	}
	if tail == 0 {
		return fmt.Sprintf("%s …", string(r[:head]))
	}
	return fmt.Sprintf("%s … %s", string(r[:head]), string(r[len(r)-tail:]))
}
