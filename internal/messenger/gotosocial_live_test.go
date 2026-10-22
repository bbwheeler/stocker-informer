package messenger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/example/stocker-informer/internal/config"
)

// goToSocialLiveEnv gates the live test. Any value other than "1" keeps it
// skipped.
const goToSocialLiveEnv = "GOTOSOCIAL_LIVE_TEST"

const liveStatusesPath = "/api/v1/statuses/"

// liveStatus is the subset of GET /api/v1/statuses/{id} the live test checks.
type liveStatus struct {
	ID         string `json:"id"`
	Visibility string `json:"visibility"`
	LocalOnly  bool   `json:"local_only"`
	Mentions   []struct {
		Username string `json:"username"`
	} `json:"mentions"`
}

// TestGoToSocialLive publishes real statuses to verify the local and direct
// audiences end to end. It only runs when GOTOSOCIAL_LIVE_TEST=1 and never
// constructs a global publisher, so it cannot federate. Every created status is
// deleted in cleanup. The token is read from the environment and never logged.
func TestGoToSocialLive(t *testing.T) {
	if os.Getenv(goToSocialLiveEnv) != "1" {
		t.Skipf("%s is not 1; skipping live GoToSocial test", goToSocialLiveEnv)
	}

	instance := strings.TrimSpace(os.Getenv("GOTOSOCIAL_INSTANCE"))
	username := strings.TrimSpace(os.Getenv("GOTOSOCIAL_USER"))
	token := strings.TrimSpace(os.Getenv("GOTOSOCIAL_TOKEN"))
	rawRecipient := strings.TrimSpace(os.Getenv("GOTOSOCIAL_RECIPIENT"))

	var missing []string
	for _, required := range []struct{ name, value string }{
		{"GOTOSOCIAL_INSTANCE", instance},
		{"GOTOSOCIAL_USER", username},
		{"GOTOSOCIAL_TOKEN", token},
		{"GOTOSOCIAL_RECIPIENT", rawRecipient},
	} {
		if required.value == "" {
			missing = append(missing, required.name)
		}
	}
	if len(missing) > 0 {
		t.Skipf("live GoToSocial test requires %s", strings.Join(missing, ", "))
	}

	recipient, err := normalizeLiveRecipient(rawRecipient, instance)
	if err != nil {
		t.Fatalf("normalize GOTOSOCIAL_RECIPIENT: %v", err)
	}

	client := &http.Client{Timeout: 20 * time.Second}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	tests := []struct {
		name      string
		audience  config.Audience
		recipient string
	}{
		{name: "local", audience: config.AudienceLocal},
		{name: "direct", audience: config.AudienceDirect, recipient: recipient},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			publisher := NewGoToSocialPublisher(instance, username, token, tt.audience, tt.recipient, log)

			text := fmt.Sprintf(
				"**stocker-informer live test** `%s` (%s) — safe to delete",
				time.Now().UTC().Format(time.RFC3339Nano), tt.name,
			)

			statusID, err := publisher.publish(context.Background(), text)
			if err != nil {
				t.Fatalf("publish %s status: %v", tt.name, err)
			}
			if statusID == "" {
				t.Fatalf("publish %s status returned an empty status ID", tt.name)
			}

			t.Cleanup(func() { deleteLiveStatus(t, client, instance, token, statusID) })

			status := fetchLiveStatus(t, client, instance, token, statusID)
			t.Logf("status id=%s visibility=%s local_only=%t mentions=%v",
				status.ID, status.Visibility, status.LocalOnly, mentionUsernames(status))

			switch tt.audience {
			case config.AudienceLocal:
				if status.Visibility != "public" {
					t.Errorf("visibility = %q, want public", status.Visibility)
				}
				// GoToSocial omits local_only unless it is true.
				if !status.LocalOnly {
					t.Error("local_only = false, want true")
				}
			case config.AudienceDirect:
				if status.Visibility != "direct" {
					t.Errorf("visibility = %q, want direct", status.Visibility)
				}
				want := recipientUsername(recipient)
				if !hasMention(status, want) {
					t.Errorf("mentions = %v, want username %q", mentionUsernames(status), want)
				}
			default:
				t.Fatalf("live test must not publish audience %q", tt.audience)
			}
		})
	}
}

// normalizeLiveRecipient mirrors config's handle normalization closely enough
// for the test: strip one leading "@" and append the instance hostname when the
// handle has no domain.
func normalizeLiveRecipient(raw, instance string) (string, error) {
	handle := strings.TrimPrefix(strings.TrimSpace(raw), "@")
	if handle == "" || strings.Count(handle, "@") > 1 {
		return "", fmt.Errorf("invalid handle %q", raw)
	}
	if strings.Contains(handle, "@") {
		return handle, nil
	}

	u, err := url.Parse(instance)
	if err != nil || u.Hostname() == "" {
		return "", fmt.Errorf("cannot derive a domain from instance %q", instance)
	}
	return handle + "@" + u.Hostname(), nil
}

// recipientUsername returns the local part of a normalized fediverse handle.
func recipientUsername(recipient string) string {
	username, _, _ := strings.Cut(recipient, "@")
	return username
}

func hasMention(status liveStatus, username string) bool {
	for _, mention := range status.Mentions {
		if mention.Username == username {
			return true
		}
	}
	return false
}

func mentionUsernames(status liveStatus) []string {
	names := make([]string, 0, len(status.Mentions))
	for _, mention := range status.Mentions {
		names = append(names, mention.Username)
	}
	return names
}

// fetchLiveStatus reads a status back through the API to confirm the fields the
// instance actually stored, rather than the fields we submitted.
func fetchLiveStatus(t *testing.T, client *http.Client, instance, token, statusID string) liveStatus {
	t.Helper()

	resp, err := doLiveRequest(client, http.MethodGet, instance, liveStatusesPath+url.PathEscape(statusID), token)
	if err != nil {
		t.Fatalf("fetch status %s: %v", statusID, err)
	}
	defer func() { _ = resp.Body.Close() }()

	var status liveStatus
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&status); err != nil {
		t.Fatalf("decode status %s: %v", statusID, err)
	}
	return status
}

// deleteLiveStatus removes a status created by the test. Cleanup failures are
// logged and never fail the test, because leaking a status is not a code defect.
func deleteLiveStatus(t *testing.T, client *http.Client, instance, token, statusID string) {
	t.Helper()

	resp, err := doLiveRequest(client, http.MethodDelete, instance, liveStatusesPath+url.PathEscape(statusID), token)
	if err != nil {
		t.Logf("cleanup: delete status %s: %v", statusID, err)
		return
	}
	_ = resp.Body.Close()
	t.Logf("cleanup: deleted status %s", statusID)
}

// doLiveRequest sends an authenticated API request and returns a 2xx response.
// The returned response body is open and the caller must close it. Errors never
// include the bearer token.
func doLiveRequest(client *http.Client, method, instance, path, token string) (*http.Response, error) {
	req, err := http.NewRequest(method, strings.TrimSuffix(instance, "/")+path, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("API returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return resp, nil
}
