package messenger

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/example/stocker-informer/internal/config"
)

const (
	testToken = "test-token"
	testPath  = "/api/v1/statuses"
)

type capturedRequest struct {
	path        string
	contentType string
	auth        string
	form        url.Values
}

// testServer is an httptest.Server that records requests and replies according
// to a caller-supplied function keyed on the 1-based request number.
type testServer struct {
	*httptest.Server

	mu       sync.Mutex
	requests []capturedRequest
	respond  func(attempt int) (int, string)
}

func newTestServer(t *testing.T, respond func(attempt int) (int, string)) *testServer {
	t.Helper()

	ts := &testServer{respond: respond}
	ts.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()

		ts.mu.Lock()
		ts.requests = append(ts.requests, capturedRequest{
			path:        r.URL.Path,
			contentType: r.Header.Get("Content-Type"),
			auth:        r.Header.Get("Authorization"),
			form:        cloneValues(r.PostForm),
		})
		attempt := len(ts.requests)
		ts.mu.Unlock()

		status, body := ts.respond(attempt)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(ts.Close)

	return ts
}

func created(_ int) (int, string) { return http.StatusCreated, `{"id":"01TEST"}` }

func cloneValues(v url.Values) url.Values {
	clone := make(url.Values, len(v))
	for key, values := range v {
		clone[key] = append([]string(nil), values...)
	}
	return clone
}

func (ts *testServer) captured() []capturedRequest {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return append([]capturedRequest(nil), ts.requests...)
}

func newTestPublisher(t *testing.T, instance string, audience config.Audience, recipient string) *GoToSocialPublisher {
	t.Helper()
	return NewGoToSocialPublisher(instance, "bot", testToken, audience, recipient, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestPublishAudienceForm(t *testing.T) {
	const recipient = "papa@wheeli.social"
	const text = "### AAPL up 2%\n\n*see [analysis](https://example.com)*"

	tests := []struct {
		name         string
		audience     config.Audience
		recipient    string
		wantStatus   string
		wantVisible  string
		wantLocal    string
		wantPrefix   string // when non-empty, status must start with it
		statusIsText bool   // when true, status must equal text exactly
	}{
		{
			name:         "global",
			audience:     config.AudienceGlobal,
			wantStatus:   text,
			wantVisible:  "public",
			wantLocal:    "false",
			statusIsText: true,
		},
		{
			name:         "local",
			audience:     config.AudienceLocal,
			wantStatus:   text,
			wantVisible:  "public",
			wantLocal:    "true",
			statusIsText: true,
		},
		{
			name:        "direct",
			audience:    config.AudienceDirect,
			recipient:   recipient,
			wantStatus:  "@" + recipient + " " + text,
			wantVisible: "direct",
			wantLocal:   "false",
			wantPrefix:  "@" + recipient + " ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newTestServer(t, created)
			publisher := newTestPublisher(t, server.URL, tt.audience, tt.recipient)

			if err := publisher.Publish(context.Background(), text); err != nil {
				t.Fatalf("Publish() error = %v", err)
			}

			got := server.captured()
			if len(got) != 1 {
				t.Fatalf("requests = %d, want 1", len(got))
			}

			if got[0].contentType != "application/x-www-form-urlencoded" {
				t.Errorf("request Content-Type = %q, want application/x-www-form-urlencoded", got[0].contentType)
			}
			if got[0].form.Get("content_type") != "text/markdown" {
				t.Errorf("content_type = %q, want text/markdown", got[0].form.Get("content_type"))
			}
			if status := got[0].form.Get("status"); status != tt.wantStatus {
				t.Errorf("status = %q, want %q", status, tt.wantStatus)
			}
			if tt.statusIsText && got[0].form.Get("status") != text {
				t.Errorf("status was modified; got %q, want unchanged %q", got[0].form.Get("status"), text)
			}
			if tt.wantPrefix != "" && !strings.HasPrefix(got[0].form.Get("status"), tt.wantPrefix) {
				t.Errorf("status = %q, want prefix %q", got[0].form.Get("status"), tt.wantPrefix)
			}
			if visibility := got[0].form.Get("visibility"); visibility != tt.wantVisible {
				t.Errorf("visibility = %q, want %q", visibility, tt.wantVisible)
			}
			if localOnly := got[0].form.Get("local_only"); localOnly != tt.wantLocal {
				t.Errorf("local_only = %q, want %q", localOnly, tt.wantLocal)
			}
		})
	}
}

func TestPublishMarkdownNotTruncated(t *testing.T) {
	body := "# Summary\n\n" +
		strings.Repeat("* item with **emphasis**\n", 40) +
		"[analysis](https://example.com/AAPL)\n\n" +
		strings.Repeat("word ", 80)
	if len(body) <= 500 {
		t.Fatalf("test body length = %d, want > 500", len(body))
	}

	server := newTestServer(t, created)
	publisher := newTestPublisher(t, server.URL, config.AudienceGlobal, "")

	if err := publisher.Publish(context.Background(), body); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	got := server.captured()
	if len(got) != 1 {
		t.Fatalf("requests = %d, want 1", len(got))
	}
	if status := got[0].form.Get("status"); status != body {
		t.Errorf("status length = %d, want %d (byte-for-byte not preserved)", len(status), len(body))
	}
}

func TestPublishRetriesAfterServerError(t *testing.T) {
	server := newTestServer(t, func(attempt int) (int, string) {
		if attempt == 1 {
			return http.StatusInternalServerError, `{"error":"boom"}`
		}
		return 201, `{"id":"01TEST"}`
	})
	publisher := newTestPublisher(t, server.URL, config.AudienceGlobal, "")

	if err := publisher.Publish(context.Background(), "hello"); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if got := len(server.captured()); got != 2 {
		t.Fatalf("requests = %d, want exactly 2", got)
	}
}

func TestPublishReturnsStatusID(t *testing.T) {
	server := newTestServer(t, created)
	publisher := newTestPublisher(t, server.URL, config.AudienceGlobal, "")

	id, err := publisher.publish(context.Background(), "hello")
	if err != nil {
		t.Fatalf("publish() error = %v", err)
	}
	if id != "01TEST" {
		t.Errorf("status ID = %q, want 01TEST", id)
	}
}

func TestPublishAcceptsTrailingSlashInstance(t *testing.T) {
	server := newTestServer(t, created)
	publisher := newTestPublisher(t, server.URL+"/", config.AudienceGlobal, "")

	if err := publisher.Publish(context.Background(), "hello"); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	got := server.captured()
	if len(got) != 1 {
		t.Fatalf("requests = %d, want 1", len(got))
	}
	if got[0].path != testPath {
		t.Errorf("request path = %q, want %q", got[0].path, testPath)
	}
}

func TestPublishSendsBearerToken(t *testing.T) {
	server := newTestServer(t, created)
	publisher := newTestPublisher(t, server.URL, config.AudienceGlobal, "")

	if err := publisher.Publish(context.Background(), "hello"); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	got := server.captured()
	if len(got) != 1 {
		t.Fatalf("requests = %d, want 1", len(got))
	}
	if want := "Bearer " + testToken; got[0].auth != want {
		t.Errorf("Authorization = %q, want %q", got[0].auth, want)
	}
}

func TestPublishToleratesUndecodableStatusID(t *testing.T) {
	server := newTestServer(t, func(int) (int, string) { return 201, "not json" })
	publisher := newTestPublisher(t, server.URL, config.AudienceGlobal, "")

	id, err := publisher.publish(context.Background(), "hello")
	if err != nil {
		t.Fatalf("publish() error = %v, want nil", err)
	}
	if id != "" {
		t.Errorf("status ID = %q, want empty", id)
	}
}
