package messenger

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const httpTimeout = 20 * time.Second

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

func (p *GoToSocialPublisher) Publish(ctx context.Context, msg Message) error {
	endpoint := p.instance + "/api/v1/statuses"

	form := url.Values{}
	form.Set("status", msg.Text)
	form.Set("visibility", "public")
	form.Set("in_reply_to_id", "")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Bearer "+p.token)

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("send status to GoToSocial: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GoToSocial API returned status %d", resp.StatusCode)
	}

	p.log.Info("Message published on GoToSocial", "endpoint", endpoint)
	return nil
}
