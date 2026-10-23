// Package summarystore reads a stock's summary markdown from a Garage/S3
// bucket.
//
// Objects live under the layout written by stocker-summarizer:
//
//	[S3_PATH_PREFIX/]{EXCHANGE}/{SYMBOL}/summary.md
//
// The canonical key has no leading slash, and it is the only key read.
package summarystore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/example/stocker-informer/internal/config"
)

// ErrNotFound reports that no summary object exists at the canonical key for
// the requested identity.
var ErrNotFound = errors.New("summarystore: summary not found")

// defaultRegion keeps construction offline: an empty region would make the
// SDK resolve one from the environment or IMDS.
const defaultRegion = "us-east-1"

// Store reads summaries from the object store.
type Store interface {
	ReadSummary(ctx context.Context, exchange, symbol string) ([]byte, error)
	Close() error
}

// API is the subset of the S3 client used here. It is the seam that keeps the
// tests off the network.
type API interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// Client is a Store backed by Garage/S3.
type Client struct {
	api     API
	bucket  string
	prefix  string
	timeout time.Duration
}

var _ Store = (*Client)(nil)

// New builds a Client from configuration. Construction stays offline: no
// bucket check and no request is made.
func New(cfg *config.S3Config) (*Client, error) {
	if cfg == nil {
		return nil, errors.New("summarystore: nil S3 config")
	}

	region := cfg.Region
	if region == "" {
		region = defaultRegion
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("summarystore: load aws config: %w", err)
	}

	api := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(strings.TrimRight(cfg.Endpoint, "/"))
		o.UsePathStyle = true
	})

	return NewWithAPI(cfg, api)
}

// NewWithAPI builds a Client around an injected API.
func NewWithAPI(cfg *config.S3Config, api API) (*Client, error) {
	if cfg == nil {
		return nil, errors.New("summarystore: nil S3 config")
	}
	if api == nil {
		return nil, errors.New("summarystore: nil S3 API")
	}
	return &Client{
		api:     api,
		bucket:  cfg.Bucket,
		prefix:  normalizePrefix(cfg.PathPrefix),
		timeout: cfg.Timeout,
	}, nil
}

// ReadSummary returns the summary bytes for an identity.
//
// It issues a single read at the canonical bucket-relative key. A definitive
// not-found satisfies errors.Is(err, ErrNotFound); every other error is
// returned immediately.
func (c *Client) ReadSummary(ctx context.Context, exchange, symbol string) ([]byte, error) {
	key, err := BuildKey(c.prefix, exchange, symbol)
	if err != nil {
		return nil, err
	}

	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	body, err := c.get(ctx, key)
	if err == nil {
		return body, nil
	}
	if isNotFound(err) {
		return nil, fmt.Errorf("summarystore: read %q: %w", key, ErrNotFound)
	}
	return nil, wrapReadError(ctx, key, err)
}

// Close releases the client. The SDK client holds no long-lived resources,
// so Close is a no-op and is safe to call more than once.
func (c *Client) Close() error { return nil }

// get reads one object. The body is always closed; a missing body is a
// storage error.
func (c *Client) get(ctx context.Context, key string) ([]byte, error) {
	out, err := c.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	if out == nil || out.Body == nil {
		return nil, fmt.Errorf("summarystore: get %q: response has no body", key)
	}
	defer func() { _ = out.Body.Close() }()

	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// wrapReadError wraps a storage error, preferring the context error when the
// read was cut short by the deadline or a cancellation.
func wrapReadError(ctx context.Context, key string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("summarystore: read %q: %w", key, ctxErr)
	}
	return fmt.Errorf("summarystore: read %q: %w", key, err)
}

// BuildKey returns the canonical object key for an identity:
//
//	[S3_PATH_PREFIX/]{EXCHANGE}/{SYMBOL}/summary.md
//
// The prefix is trimmed of slashes and whitespace and its empty segments are
// dropped; exchange and symbol are trimmed and upper-cased. The returned key
// never starts with a slash. An identity that is empty or contains "/" or
// ".." is rejected before any S3 call.
func BuildKey(prefix, exchange, symbol string) (string, error) {
	prefix = strings.TrimSpace(prefix)
	exchange = strings.TrimSpace(exchange)
	symbol = strings.TrimSpace(symbol)

	if exchange == "" {
		return "", errors.New("summarystore: exchange must not be empty")
	}
	if symbol == "" {
		return "", errors.New("summarystore: symbol must not be empty")
	}
	if err := checkSegment("exchange", exchange); err != nil {
		return "", err
	}
	if err := checkSegment("symbol", symbol); err != nil {
		return "", err
	}
	if strings.Contains(prefix, "..") {
		return "", errors.New(`summarystore: path prefix must not contain ".."`)
	}

	key := strings.ToUpper(exchange) + "/" + strings.ToUpper(symbol) + "/summary.md"
	if p := normalizePrefix(prefix); p != "" {
		key = p + "/" + key
	}
	return key, nil
}

// checkSegment rejects an identity segment that could escape the bucket
// prefix.
func checkSegment(name, value string) error {
	if strings.Contains(value, "/") {
		return fmt.Errorf("summarystore: %s must not contain %q", name, "/")
	}
	if strings.Contains(value, "..") {
		return fmt.Errorf("summarystore: %s must not contain %q", name, "..")
	}
	return nil
}

// normalizePrefix trims whitespace, drops empty segments and joins the rest
// with single slashes.
func normalizePrefix(prefix string) string {
	parts := strings.Split(prefix, "/")
	segments := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			segments = append(segments, part)
		}
	}
	return strings.Join(segments, "/")
}

// isNotFound reports whether err is a definitive object miss.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}

	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}

	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}

	var statusErr interface{ HTTPStatusCode() int }
	if errors.As(err, &statusErr) {
		return statusErr.HTTPStatusCode() == http.StatusNotFound
	}

	return false
}
