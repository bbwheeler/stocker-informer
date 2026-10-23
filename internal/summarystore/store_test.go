package summarystore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
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

// TestReadSummaryKeysAreBucketRelativeUnderEveryPrefix locks in the key
// contract for every S3_PATH_PREFIX spelling: each ReadSummary call issues
// exactly one GET, and every requested key is the hand-derived canonical
// bucket-relative key — never rooted, never doubled.
//
// The break it catches: rooting BuildKey or re-adding the rooted retry
// ("/" + key) would put a leading slash in a requested key or add a second
// GET on the miss leg.
func TestReadSummaryKeysAreBucketRelativeUnderEveryPrefix(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		want   string
	}{
		{"empty prefix", "", "NASDAQ/AAPL/summary.md"},
		{"rooted prefix", "/corp/", "corp/NASDAQ/AAPL/summary.md"},
		{"rooted doubled prefix", "//corp//stocks//", "corp/stocks/NASDAQ/AAPL/summary.md"},
	}
	const body = "# AAPL\n\nSome summary.\n"

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// call 0: canonical hit; call 1: canonical miss.
			api := &fakeAPI{fn: func(_ context.Context, _ string, call int) (*s3.GetObjectOutput, error) {
				if call == 0 {
					return objectOutput([]byte(body)), nil
				}
				return nil, &types.NoSuchKey{}
			}}
			c := newTestClient(t, api, tc.prefix, time.Second)

			got, err := c.ReadSummary(context.Background(), "nasdaq", "aapl")
			if err != nil {
				t.Fatalf("ReadSummary (hit): %v", err)
			}
			if string(got) != body {
				t.Errorf("body = %q, want %q", got, body)
			}
			if _, err := c.ReadSummary(context.Background(), "nasdaq", "aapl"); !errors.Is(err, ErrNotFound) {
				t.Errorf("ReadSummary (miss) error = %v, want errors.Is(err, ErrNotFound)", err)
			}

			keys := api.keys()
			if len(keys) != 2 {
				t.Fatalf("requested keys = %v, want exactly two single-GET calls", keys)
			}
			for i, key := range keys {
				if strings.HasPrefix(key, "/") {
					t.Errorf("GET %d key = %q, want a bucket-relative key with no leading %q", i+1, key, "/")
				}
				if key != tc.want {
					t.Errorf("GET %d key = %q, want %q", i+1, key, tc.want)
				}
			}
		})
	}
}

// keyResolveAPI is a minimal bucket fake whose GetObject resolves the
// requested key to a stored key through an injected function, modelling the
// key semantics of the two supported backends. It records every requested
// key so tests can assert call counts and exact spellings.
type keyResolveAPI struct {
	objects map[string][]byte
	resolve func(key string) string
	gets    []string
}

func (a *keyResolveAPI) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := aws.ToString(in.Key)
	a.gets = append(a.gets, key)
	body, ok := a.objects[a.resolve(key)]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	return objectOutput(body), nil
}

// TestReadSummaryBackendKeySemantics pins the key-spelling contract against
// both supported backends.
//
// The break it catches: reintroducing a rooted read (rooted BuildKey or the
// removed "/" + key fallback) makes the AWS case serve a rooted legacy
// object instead of reporting ErrNotFound, and puts a rooted key or a second
// GET on the wire in both cases.
func TestReadSummaryBackendKeySemantics(t *testing.T) {
	const (
		canonicalKey = "NASDAQ/AAPL/summary.md"
		rootedKey    = "/" + canonicalKey
		body         = "# AAPL\n"
	)

	// Garage (<= v2.4.1) derives the stored key from the URL path with the
	// leading slash trimmed, so rooted and unrooted request keys resolve to
	// the same object: the removed rooted retry was redundant here — when the
	// canonical GET 404s, the rooted retry addresses an identical, equally
	// absent object and can never return data.
	t.Run("garage trims leading slash", func(t *testing.T) {
		api := &keyResolveAPI{
			objects: map[string][]byte{canonicalKey: []byte(body)},
			resolve: func(key string) string { return strings.TrimLeft(key, "/") },
		}
		c := newTestClient(t, api, "", time.Second)

		got, err := c.ReadSummary(context.Background(), "nasdaq", "aapl")
		if err != nil {
			t.Fatalf("ReadSummary: %v", err)
		}
		if string(got) != body {
			t.Errorf("body = %q, want %q", got, body)
		}
		if len(api.gets) != 1 || api.gets[0] != canonicalKey {
			t.Errorf("requested keys = %v, want exactly [%s]", api.gets, canonicalKey)
		}

		// Characterization of the fake's Garage model: a rooted request
		// resolves to the same stored object as the canonical one, which is
		// why the fallback could never succeed when the canonical GET did not.
		out, err := api.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String("stocker"),
			Key:    aws.String(rootedKey),
		})
		if err != nil {
			t.Fatalf("rooted GetObject against the Garage model: %v", err)
		}
		probed, err := io.ReadAll(out.Body)
		_ = out.Body.Close()
		if err != nil {
			t.Fatalf("read rooted probe body: %v", err)
		}
		if string(probed) != body {
			t.Errorf("rooted probe body = %q, want %q (same object as the canonical key)", probed, body)
		}
		if len(api.gets) != 2 || api.gets[1] != rootedKey {
			t.Errorf("recorded keys = %v, want the rooted probe at %q", api.gets, rootedKey)
		}
	})

	// AWS S3 keeps the leading slash as a key byte, so a rooted object is a
	// distinct object the canonical read must not reach: rooted keys are
	// non-portable and must never be requested.
	t.Run("aws treats keys literally", func(t *testing.T) {
		api := &keyResolveAPI{
			objects: map[string][]byte{rootedKey: []byte(body)}, // rooted-only legacy object
			resolve: func(key string) string { return key },
		}
		c := newTestClient(t, api, "", time.Second)

		_, err := c.ReadSummary(context.Background(), "nasdaq", "aapl")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("ReadSummary error = %v, want errors.Is(err, ErrNotFound) for the rooted-only object", err)
		}
		if len(api.gets) != 1 || api.gets[0] != canonicalKey {
			t.Errorf("requested keys = %v, want exactly [%s]", api.gets, canonicalKey)
		}
	})
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
