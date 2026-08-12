package messenger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	httpTimeout      = 20 * time.Second
	maxPublishRetries   = 3
	publishBackoffBase  = 500 * time.Millisecond
	publishMaxBackoff   = 30 * time.Second
)

type GoToSocialPublisher struct {
	instance string
	username string
	token    string
	client   *http.Client
	log      *slog.Logger
}

func NewGoToSocialPublisher(instance, username, token string, log *slog.Logger) *GoToSocialPublisher {
	transport := &http.Transport{
		MaxIdleConns:        10,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	return &GoToSocialPublisher{
		instance: strings.TrimSuffix(instance, "/"),
		username: username,
		token:    token,
		client:   &http.Client{Timeout: httpTimeout, Transport: transport},
		log:      log,
	}
}

func (p *GoToSocialPublisher) Publish(ctx context.Context, text string) error {
	endpoint := p.instance + "/api/v1/statuses"

	form := url.Values{}
	form.Set("status", text)
	form.Set("visibility", "public")

	for attempt := 0; attempt < maxPublishRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}

		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", "Bearer "+p.token)

		resp, err := p.client.Do(req)
		if err != nil {
			lastErr := fmt.Errorf("send status attempt %d to GoToSocial: %w", attempt+1, err)
			p.log.Warn("publish attempt failed", "attempt", attempt+1, "max_attempts", maxPublishRetries, "error", lastErr)

			if attempt < maxPublishRetries-1 {
				backoff := time.Duration(int64(publishBackoffBase) << uint(attempt))
				if backoff > publishMaxBackoff {
					backoff = publishMaxBackoff
				}
				time.Sleep(backoff)
				continue
			}

			return lastErr
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			p.log.Info("Message published on GoToSocial", "endpoint", endpoint)
			return nil
		}

		body, _ := io.ReadAll(resp.Body)
		lastErr := fmt.Errorf("GoToSocial API returned status %d: %s", resp.StatusCode, string(body))
		p.log.Warn("publish attempt failed status code", "attempt", attempt+1, "max_attempts", maxPublishRetries, "status", resp.StatusCode, "error", lastErr)

		if attempt < maxPublishRetries-1 {
			backoff := time.Duration(int64(publishBackoffBase) << uint(attempt))
			if backoff > publishMaxBackoff {
				backoff = publishMaxBackoff
			}
			time.Sleep(backoff)
			continue
		}

		return lastErr
	}

	return fmt.Errorf("publish failed after %d attempt(s)", maxPublishRetries)
}
