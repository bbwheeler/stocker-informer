package summarystore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/example/stocker-informer/internal/config"
)

// fakeAPI is a scripted s3.GetObject implementation. It records every input
// so tests can assert the exact keys and bucket that were requested.
type fakeAPI struct {
	inputs []*s3.GetObjectInput
	fn     func(ctx context.Context, key string, call int) (*s3.GetObjectOutput, error)
}

func (f *fakeAPI) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.inputs = append(f.inputs, in)
	key := aws.ToString(in.Key)
	return f.fn(ctx, key, len(f.inputs)-1)
}

func (f *fakeAPI) keys() []string {
	out := make([]string, 0, len(f.inputs))
	for _, in := range f.inputs {
		out = append(out, aws.ToString(in.Key))
	}
	return out
}

// objectOutput returns a successful response carrying body.
func objectOutput(body []byte) *s3.GetObjectOutput {
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}
}

// trackingBody records whether it was closed.
type trackingBody struct {
	reader io.Reader
	closed bool
}

func (b *trackingBody) Read(p []byte) (int, error) { return b.reader.Read(p) }

func (b *trackingBody) Close() error {
	b.closed = true
	return nil
}

// statusError is a fake error exposing an HTTP status code.
type statusError struct {
	code int
}

func (e *statusError) Error() string { return fmt.Sprintf("http status %d", e.code) }

func (e *statusError) HTTPStatusCode() int { return e.code }

// failingBody is a body whose read always fails.
type failingBody struct{}

func (b *failingBody) Read([]byte) (int, error) { return 0, errors.New("connection reset") }

func (b *failingBody) Close() error { return nil }

func newTestClient(t *testing.T, api API, prefix string, timeout time.Duration) *Client {
	t.Helper()
	c, err := NewWithAPI(&config.S3Config{
		Bucket:     "stocker",
		PathPrefix: prefix,
		Timeout:    timeout,
	}, api)
	if err != nil {
		t.Fatalf("NewWithAPI: %v", err)
	}
	return c
}

func TestBuildKey(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		exchange string
		symbol   string
		want     string
	}{
		{"canonical", "", "nasdaq", "aapl", "NASDAQ/AAPL/summary.md"},
		{"trim and upper case", "", " nasdaq ", " aapl ", "NASDAQ/AAPL/summary.md"},
		{"prefix with trailing slash", "corp/", "nasdaq", "aapl", "corp/NASDAQ/AAPL/summary.md"},
		{"prefix normalizes slashes", "/a//b/", "nasdaq", "aapl", "a/b/NASDAQ/AAPL/summary.md"},
		{"nested prefix", "corp/stocks", "nyse", "ibm", "corp/stocks/NYSE/IBM/summary.md"},
		{"slashes only prefix", "///", "nasdaq", "aapl", "NASDAQ/AAPL/summary.md"},
		{"dotted symbol", "", "tsx", "shop.to", "TSX/SHOP.TO/summary.md"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := BuildKey(tc.prefix, tc.exchange, tc.symbol)
			if err != nil {
				t.Fatalf("BuildKey(%q, %q, %q) error: %v", tc.prefix, tc.exchange, tc.symbol, err)
			}
			if got != tc.want {
				t.Errorf("BuildKey(%q, %q, %q) = %q, want %q", tc.prefix, tc.exchange, tc.symbol, got, tc.want)
			}
			if got != "" && got[0] == '/' {
				t.Errorf("BuildKey returned a leading slash: %q", got)
			}
		})
	}
}

func TestBuildKeyRejectsInvalidIdentity(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		exchange string
		symbol   string
	}{
		{"empty exchange", "", "", "aapl"},
		{"whitespace exchange", "", "   ", "aapl"},
		{"empty symbol", "", "nasdaq", ""},
		{"whitespace symbol", "", "nasdaq", "  "},
		{"slash in exchange", "", "nas/daq", "aapl"},
		{"slash in symbol", "", "nasdaq", "aa/pl"},
		{"dot dot in exchange", "", "na..daq", "aapl"},
		{"dot dot in symbol", "", "nasdaq", "aa..pl"},
		{"dot dot in prefix", "corp/..", "nasdaq", "aapl"},
		{"dot dot only in prefix", "..", "nasdaq", "aapl"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := BuildKey(tc.prefix, tc.exchange, tc.symbol)
			if err == nil {
				t.Fatalf("BuildKey(%q, %q, %q) = %q, want an error", tc.prefix, tc.exchange, tc.symbol, got)
			}
			if got != "" {
				t.Errorf("BuildKey returned %q alongside the error", got)
			}
		})
	}
}

func TestReadSummaryCanonical(t *testing.T) {
	body := []byte("# AAPL\n\nSome summary.\n")
	tracked := &trackingBody{reader: bytes.NewReader(body)}
	api := &fakeAPI{fn: func(_ context.Context, _ string, _ int) (*s3.GetObjectOutput, error) {
		return &s3.GetObjectOutput{Body: tracked}, nil
	}}
	c := newTestClient(t, api, "", time.Second)

	got, err := c.ReadSummary(context.Background(), "nasdaq", "aapl")
	if err != nil {
		t.Fatalf("ReadSummary: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("body = %q, want %q", got, body)
	}
	if keys := api.keys(); len(keys) != 1 || keys[0] != "NASDAQ/AAPL/summary.md" {
		t.Errorf("requested keys = %v, want [NASDAQ/AAPL/summary.md]", keys)
	}
	if bucket := aws.ToString(api.inputs[0].Bucket); bucket != "stocker" {
		t.Errorf("bucket = %q, want %q", bucket, "stocker")
	}
	if !tracked.closed {
		t.Error("response body was not closed")
	}
}

func TestReadSummaryNotFound(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		want   string
	}{
		{"no prefix", "", "NASDAQ/AAPL/summary.md"},
		{"prefixed", "corp/", "corp/NASDAQ/AAPL/summary.md"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			api := &fakeAPI{fn: func(_ context.Context, _ string, _ int) (*s3.GetObjectOutput, error) {
				return nil, &types.NoSuchKey{}
			}}
			c := newTestClient(t, api, tc.prefix, time.Second)

			_, err := c.ReadSummary(context.Background(), "nasdaq", "aapl")
			if err == nil {
				t.Fatal("ReadSummary returned no error")
			}
			if !errors.Is(err, ErrNotFound) {
				t.Errorf("error %v does not satisfy errors.Is(err, ErrNotFound)", err)
			}
			if keys := api.keys(); len(keys) != 1 || keys[0] != tc.want {
				t.Errorf("requested keys = %v, want exactly [%s]", keys, tc.want)
			}
		})
	}
}

func TestReadSummaryStorageErrorNoFallback(t *testing.T) {
	api := &fakeAPI{fn: func(_ context.Context, _ string, _ int) (*s3.GetObjectOutput, error) {
		return nil, &statusError{code: http.StatusInternalServerError}
	}}
	c := newTestClient(t, api, "", time.Second)

	_, err := c.ReadSummary(context.Background(), "nasdaq", "aapl")
	if err == nil {
		t.Fatal("ReadSummary returned no error")
	}
	if errors.Is(err, ErrNotFound) {
		t.Errorf("error %v must not satisfy errors.Is(err, ErrNotFound)", err)
	}
	if keys := api.keys(); len(keys) != 1 {
		t.Errorf("requested keys = %v, want a single canonical GET", keys)
	}
}

func TestReadSummaryReadError(t *testing.T) {
	api := &fakeAPI{fn: func(_ context.Context, _ string, _ int) (*s3.GetObjectOutput, error) {
		return &s3.GetObjectOutput{Body: &failingBody{}}, nil
	}}
	c := newTestClient(t, api, "", time.Second)

	_, err := c.ReadSummary(context.Background(), "nasdaq", "aapl")
	if err == nil {
		t.Fatal("ReadSummary returned no error")
	}
	if errors.Is(err, ErrNotFound) {
		t.Errorf("error %v must not satisfy errors.Is(err, ErrNotFound)", err)
	}
	if keys := api.keys(); len(keys) != 1 {
		t.Errorf("requested keys = %v, want a single canonical GET", keys)
	}
}

func TestReadSummaryNilBody(t *testing.T) {
	api := &fakeAPI{fn: func(_ context.Context, _ string, _ int) (*s3.GetObjectOutput, error) {
		return &s3.GetObjectOutput{}, nil
	}}
	c := newTestClient(t, api, "", time.Second)

	_, err := c.ReadSummary(context.Background(), "nasdaq", "aapl")
	if err == nil {
		t.Fatal("ReadSummary returned no error")
	}
	if errors.Is(err, ErrNotFound) {
		t.Errorf("error %v must not satisfy errors.Is(err, ErrNotFound)", err)
	}
	if keys := api.keys(); len(keys) != 1 {
		t.Errorf("requested keys = %v, want a single canonical GET", keys)
	}
}

func TestReadSummaryTimeout(t *testing.T) {
	api := &fakeAPI{fn: func(ctx context.Context, _ string, _ int) (*s3.GetObjectOutput, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	c := newTestClient(t, api, "", 10*time.Millisecond)

	start := time.Now()
	_, err := c.ReadSummary(context.Background(), "nasdaq", "aapl")
	if err == nil {
		t.Fatal("ReadSummary returned no error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error %v does not satisfy errors.Is(err, context.DeadlineExceeded)", err)
	}
	if !bytes.Contains([]byte(err.Error()), []byte("deadline")) {
		t.Errorf("error %q does not mention the deadline", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("ReadSummary took %v, want the read to be cut short by the timeout", elapsed)
	}
}

func TestIsNotFound(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"no such key", &types.NoSuchKey{}, true},
		{"not found", &types.NotFound{}, true},
		{"api error", &smithy.GenericAPIError{Code: "NoSuchKey"}, true},
		{"api not found", &smithy.GenericAPIError{Code: "NotFound"}, true},
		{"api access denied", &smithy.GenericAPIError{Code: "AccessDenied"}, false},
		{"http 404", &statusError{code: http.StatusNotFound}, true},
		{"http 403", &statusError{code: http.StatusForbidden}, false},
		{"http 500", &statusError{code: http.StatusInternalServerError}, false},
		{"plain error", errors.New("boom"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isNotFound(tc.err); got != tc.want {
				t.Errorf("isNotFound(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestReadSummaryRejectsInvalidIdentityBeforeAnyCall(t *testing.T) {
	api := &fakeAPI{fn: func(_ context.Context, _ string, _ int) (*s3.GetObjectOutput, error) {
		return objectOutput([]byte("unused")), nil
	}}
	c := newTestClient(t, api, "", time.Second)

	if _, err := c.ReadSummary(context.Background(), "", "aapl"); err == nil {
		t.Fatal("ReadSummary returned no error for an empty exchange")
	}
	if _, err := c.ReadSummary(context.Background(), "nasdaq", "aa/pl"); err == nil {
		t.Fatal("ReadSummary returned no error for a symbol containing a slash")
	}
	if keys := api.keys(); len(keys) != 0 {
		t.Errorf("requested keys = %v, want no S3 call", keys)
	}
}

func TestClose(t *testing.T) {
	api := &fakeAPI{fn: func(_ context.Context, _ string, _ int) (*s3.GetObjectOutput, error) {
		return objectOutput(nil), nil
	}}
	c := newTestClient(t, api, "", time.Second)

	if err := c.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestNewWithAPIValidatesArguments(t *testing.T) {
	api := &fakeAPI{fn: func(_ context.Context, _ string, _ int) (*s3.GetObjectOutput, error) {
		return objectOutput(nil), nil
	}}

	if _, err := NewWithAPI(nil, api); err == nil {
		t.Error("NewWithAPI(nil config) returned no error")
	}
	if _, err := NewWithAPI(&config.S3Config{Bucket: "stocker"}, nil); err == nil {
		t.Error("NewWithAPI(nil API) returned no error")
	}
	if _, err := New(nil); err == nil {
		t.Error("New(nil config) returned no error")
	}
}

func TestNewWiresClient(t *testing.T) {
	c, err := New(&config.S3Config{
		Endpoint:  "http://garage.example:3900/",
		Region:    "eu-west-1",
		Bucket:    "stocker",
		AccessKey: "AKIAEXAMPLE",
		SecretKey: "secret",
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	api, ok := c.api.(*s3.Client)
	if !ok {
		t.Fatalf("client API is %T, want *s3.Client", c.api)
	}
	opts := api.Options()
	if got := aws.ToString(opts.BaseEndpoint); got != "http://garage.example:3900" {
		t.Errorf("BaseEndpoint = %q, want %q", got, "http://garage.example:3900")
	}
	if !opts.UsePathStyle {
		t.Error("UsePathStyle = false, want true")
	}
	if opts.Region != "eu-west-1" {
		t.Errorf("Region = %q, want %q", opts.Region, "eu-west-1")
	}
	if c.bucket != "stocker" {
		t.Errorf("bucket = %q, want %q", c.bucket, "stocker")
	}
	if c.timeout != time.Second {
		t.Errorf("timeout = %v, want %v", c.timeout, time.Second)
	}

	creds, err := opts.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("retrieve credentials: %v", err)
	}
	if creds.AccessKeyID != "AKIAEXAMPLE" || creds.SecretAccessKey != "secret" {
		t.Errorf("credentials = %q/%q, want the configured static credentials", creds.AccessKeyID, creds.SecretAccessKey)
	}
}

func TestNewDefaultsEmptyRegion(t *testing.T) {
	c, err := New(&config.S3Config{
		Endpoint:  "http://garage.example:3900",
		Bucket:    "stocker",
		AccessKey: "AKIAEXAMPLE",
		SecretKey: "secret",
		Timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	api, ok := c.api.(*s3.Client)
	if !ok {
		t.Fatalf("client API is %T, want *s3.Client", c.api)
	}
	if got := api.Options().Region; got != "us-east-1" {
		t.Errorf("Region = %q, want %q", got, "us-east-1")
	}
}
