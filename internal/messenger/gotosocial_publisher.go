package messenger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/example/stocker-informer/internal/config"
)

const (
	httpTimeout        = 20 * time.Second
	maxPublishRetries  = 3
	publishBackoffBase = 500 * time.Millisecond
	publishMaxBackoff  = 30 * time.Second
	maxStatusIDBytes   = 64 << 10
)

// GoToSocialPublisher posts Markdown statuses to a GoToSocial instance. The
// audience (global, local, direct) selects the visibility form fields.
type GoToSocialPublisher struct {
	instance  string
	username  string
	token     string
	audience  config.Audience
	recipient string
	client    *http.Client
	log       *slog.Logger
}

// NewGoToSocialPublisher creates a publisher for the given instance and audience.
// For config.AudienceDirect the recipient must already be normalized by config.
func NewGoToSocialPublisher(instance, username, token string, audience config.Audience, recipient string, log *slog.Logger) *GoToSocialPublisher {
	transport := &http.Transport{
		MaxIdleConns:        10,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	return &GoToSocialPublisher{
		instance:  strings.TrimSuffix(instance, "/"),
		username:  username,
		token:     token,
		audience:  audience,
		recipient: recipient,
		client:    &http.Client{Timeout: httpTimeout, Transport: transport},
		log:       log,
	}
}

// Publish posts text and discards the created status ID. It keeps the
// messenger.Publisher contract.
func (p *GoToSocialPublisher) Publish(ctx context.Context, text string) error {
	_, err := p.publish(ctx, text)
	return err
}

// form maps the configured audience to GoToSocial status form fields. The
// status body is always sent as Markdown.
func (p *GoToSocialPublisher) form(text string) url.Values {
	form := url.Values{}
	form.Set("content_type", "text/markdown")

	switch p.audience {
	case config.AudienceLocal:
		form.Set("status", text)
		form.Set("visibility", "public")
		form.Set("local_only", "true")
	case config.AudienceDirect:
		form.Set("status", "@"+p.recipient+" "+text)
		form.Set("visibility", "direct")
		form.Set("local_only", "false")
	default: // config.AudienceGlobal and any unset value
		form.Set("status", text)
		form.Set("visibility", "public")
		form.Set("local_only", "false")
	}

	return form
}

// publish posts text and returns the created status ID so callers can delete
// the status. A missing or undecodable ID is not an error.
func (p *GoToSocialPublisher) publish(ctx context.Context, text string) (string, error) {
	endpoint := p.instance + "/api/v1/statuses"
	body := p.form(text).Encode()

	for attempt := 0; attempt < maxPublishRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
		if err != nil {
			return "", fmt.Errorf("create request: %w", err)
		}

		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", "Bearer "+p.token)

		resp, err := p.client.Do(req)
		if err != nil {
			lastErr := fmt.Errorf("send status attempt %d to GoToSocial: %w", attempt+1, err)
			p.log.Warn("publish attempt failed", "attempt", attempt+1, "max_attempts", maxPublishRetries, "error", lastErr)

			if attempt < maxPublishRetries-1 {
				time.Sleep(publishBackoff(attempt))
				continue
			}

			return "", lastErr
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			statusID := decodeStatusID(resp.Body)
			_ = resp.Body.Close()
			p.log.Info("status published on GoToSocial", "endpoint", endpoint)
			return statusID, nil
		}

		respBody, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		lastErr := fmt.Errorf("GoToSocial API returned status %d: %s", resp.StatusCode, string(respBody))
		p.log.Warn("publish attempt failed status code", "attempt", attempt+1, "max_attempts", maxPublishRetries, "status", resp.StatusCode, "error", lastErr)

		if attempt < maxPublishRetries-1 {
			time.Sleep(publishBackoff(attempt))
			continue
		}

		return "", lastErr
	}

	return "", fmt.Errorf("publish failed after %d attempt(s)", maxPublishRetries)
}

// publishBackoff returns the exponential backoff before the given retry.
func publishBackoff(attempt int) time.Duration {
	backoff := time.Duration(int64(publishBackoffBase) << uint(attempt))
	if backoff > publishMaxBackoff {
		backoff = publishMaxBackoff
	}
	return backoff
}

// decodeStatusID best-effort reads {"id":"..."} from a success response. A
// decode failure yields an empty ID rather than an error.
func decodeStatusID(r io.Reader) string {
	var payload struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(io.LimitReader(r, maxStatusIDBytes)).Decode(&payload); err != nil {
		return ""
	}
	return payload.ID
}
