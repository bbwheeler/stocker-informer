# stocker-informer

Go CLI daemon that consumes protobuf `stockstore.v1.Stock` summary signals from Kafka, reads each stock's `summary.md` from Garage/S3, rewrites it as a Cora-9 news report with a configurable Ollama instance (falling back to the original summary on any rewrite failure), and publishes the Markdown to GoToSocial.

## Key facts

- **Module**: `github.com/example/stocker-informer` | **Go**: 1.25
- **Build/run**: `go build ./cmd/server/` | **Test**: `go test ./...`
- **Lint/static checks**: `gofmt -l .`, `go vet ./...`
- **Kafka dependency**: `github.com/segmentio/kafka-go` v0.4.51
- **Other key dependencies**: AWS SDK for Go v2 (`config`, `credentials`, `service/s3`), `git.wheeli.ca/brian/stocker-store/proto/v1`, `google.golang.org/protobuf` v1.36.12
- **Rewrite dependency**: Ollama HTTP API via the standard library (`net/http`, `encoding/json`); no additional module

## Structure

```
cmd/server/main.go        # entry point / composition root
internal/config/          # env loading, defaults, validation, audience + recipient + Ollama settings
internal/kafka/           # Kafka consumer + protobuf Stock decoder + rewrite/publish dispatch
internal/rewriter/        # Ollama /api/generate adapter + Rewriter interface
internal/summarystore/    # Garage/S3 summary reader (canonical unrooted key only)
internal/messenger/       # GoToSocial publisher + Publisher interface
deploy/quadlet/           # single rootless quadlet + .env.podman.example template
docs/                     # implementation plans (stock-summary-gotosocial.md, plan-ollama-cora9-rewrite.md, deployment.md)
```

## Architecture

`main()` loads config → builds the S3 summary store → conditionally builds the Ollama rewriter → builds the audience-aware GoToSocial publisher → constructs the Kafka consumer. The consumer subscribes to the topic, decodes each record value as protobuf `stockstore.v1.Stock`, reads `[S3_PATH_PREFIX/]{EXCHANGE}/{SYMBOL}/summary.md` from Garage/S3, rewrites the trimmed summary as a Cora-9 news report when `OLLAMA_ENABLED=true` (publishing the original summary if rewriting fails), and POSTs the resulting Markdown to `{GOTOSOCIAL_INSTANCE}/api/v1/statuses` with the configured audience (`global`/`local`/`direct`). Every outcome logs and commits the offset.

## Environment variables

Required:

- `KAFKA_BOOTSTRAP_SERVERS` — comma-separated Kafka broker addresses
- `KAFKA_TOPIC` — topic carrying protobuf `Stock` signals (operationally `stock.summary.available.v1`)
- `KAFKA_CONSUMER_GROUP` — consumer group ID for partition offset tracking
- `GOTOSOCIAL_INSTANCE` — GoToSocial instance base URL (absolute, http/https)
- `GOTOSOCIAL_USER` — GoToSocial username (metadata only; auth is the Bearer token)
- `GOTOSOCIAL_TOKEN` — GoToSocial Bearer token (secret)
- `S3_BUCKET` — bucket holding an `{EXCHANGE}/{SYMBOL}/summary.md` layout
- `S3_ACCESS_KEY` — static S3 access key (secret)
- `S3_SECRET_KEY` — static S3 secret key (secret)

Optional:

- `GOTOSOCIAL_AUDIENCE` — `global` (default) | `local` | `direct`
- `GOTOSOCIAL_RECIPIENT` — fediverse handle; required iff audience is `direct`
- `S3_ENDPOINT` — default `http://192.168.1.31:3900`
- `S3_REGION` — default `us-east-1`
- `S3_PATH_PREFIX` — default empty
- `S3_TIMEOUT_SECONDS` — default `10`; must be an integer `> 0`
- `OLLAMA_ENABLED` — default `true`; Go boolean; `false` publishes the original summary and makes no Ollama request
- `OLLAMA_BASE_URL` — default `http://192.168.1.71:11434`; absolute http(s) URL; `/api/generate` is appended
- `OLLAMA_MODEL` — default `gemma4:26b`; must be non-empty and pulled on the Ollama host
- `OLLAMA_TIMEOUT_SECONDS` — default `120`; must be an integer in `[1, 3600]`
- `OLLAMA_SYSTEM_PROMPT` — default built-in Cora-9 prompt; blank uses the default
- `OLLAMA_TEMPERATURE` — default `0.7`; finite number in `[0, 2]`
- `OLLAMA_MAX_TOKENS` — default `1024`; sets `num_predict`; must be an integer `> 0`
- `OLLAMA_MAX_OUTPUT_CHARS` — default `0` (unlimited); must be an integer `>= 0`; over the cap falls back to the original

Invalid configuration aborts startup with `load config: invalid configuration: ...` and exit code `1`.

## Testing

- `go test ./...` runs all unit tests (no network; S3 uses an injected fake API, GoToSocial and Ollama use `httptest`).
- `go vet ./...` and `gofmt -l .` must be clean.
- The opt-in live GoToSocial test runs only when `GOTOSOCIAL_LIVE_TEST=1` and also requires `GOTOSOCIAL_INSTANCE`, `GOTOSOCIAL_USER`, `GOTOSOCIAL_TOKEN`, and `GOTOSOCIAL_RECIPIENT`.
- The opt-in live Ollama rewrite test runs only when `OLLAMA_LIVE_TEST=1`; it defaults to `http://192.168.1.71:11434` and `gemma4:26b`, overridable with `OLLAMA_BASE_URL` / `OLLAMA_MODEL`:
  `OLLAMA_LIVE_TEST=1 go test ./internal/rewriter -run TestOllamaLive -v`

No additional lint, format, or typecheck tooling beyond standard Go conventions.

## Tools

Credentials for the Forgejo instance git.wheeli.ca can be found in the parent directory (../credentials.md)
