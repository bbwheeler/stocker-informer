# stocker-informer

Go CLI daemon that consumes protobuf `stockstore.v1.Stock` summary signals from RabbitMQ, reads each stock's `summary.md` from Garage/S3, rewrites it as a Cora-9 news report with a configurable Ollama instance (falling back to the original summary on any rewrite failure), and publishes the Markdown to GoToSocial.

## Key facts

- **Module**: `github.com/example/stocker-informer` | **Go**: 1.25
- **Build/run**: `go build ./cmd/server/` | **Test**: `go test ./...`
- **Lint/static checks**: `gofmt -l .`, `go vet ./...`
- **RabbitMQ dependency**: `github.com/rabbitmq/amqp091-go` v1.16.0
- **Other key dependencies**: AWS SDK for Go v2 (`config`, `credentials`, `service/s3`), `git.wheeli.ca/brian/stocker-store/proto/v1`, `google.golang.org/protobuf` v1.36.12
- **Rewrite dependency**: Ollama HTTP API via the standard library (`net/http`, `encoding/json`); no additional module

## Structure

```
cmd/server/main.go        # entry point / composition root
internal/config/          # env loading, defaults, validation, RabbitMQ + audience + recipient + Ollama settings
internal/rabbitmq/        # supervised consumer, session, passive-topology probe, protobuf Stock decoder
internal/rewriter/        # Ollama /api/generate adapter + Rewriter interface
internal/summarystore/    # Garage/S3 summary reader (canonical unrooted key only)
internal/messenger/       # GoToSocial publisher + Publisher interface
deploy/quadlet/           # single rootless quadlet + .env.podman.example template
docs/                     # operator guide (deployment.md)
```

## Architecture

`main()` loads config → builds the S3 summary store → conditionally builds the Ollama rewriter → builds the audience-aware GoToSocial publisher → constructs the RabbitMQ consumer and readiness probe. The consumer declares the durable input exchange (`RABBITMQ_INPUT_EXCHANGE`, default `stock.summary`), its own durable non-exclusive queue (`RABBITMQ_QUEUE`, default `stocker-informer`), and the binding on every (re)connect, then consumes with manual acknowledgements (`autoAck=false`) and `Qos(RABBITMQ_PREFETCH, 0, false)`. It decodes each message body as protobuf `stockstore.v1.Stock`, reads `[S3_PATH_PREFIX/]{EXCHANGE}/{SYMBOL}/summary.md` from Garage/S3, rewrites the trimmed summary as a Cora-9 news report when `OLLAMA_ENABLED=true` (publishing the original summary if rewriting fails), and POSTs the resulting Markdown to `{GOTOSOCIAL_INSTANCE}/api/v1/statuses` with the configured audience (`global`/`local`/`direct`). `amqp091-go` does not reconnect on its own, so `Run` supervises the whole session with a deterministic `1s`→`30s` exponential backoff and retries broker failures forever. Every completed pipeline outcome — decode failure, missing/empty summary, rewrite fallback, publish success or failure — acks the delivery; a delivery interrupted by shutdown is nacked with requeue instead.

## Environment variables

Required (6):

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
- `RABBITMQ_URL` — default `amqp://<user>:<password>@192.168.1.48:5672/` (credentials redacted here; the built-in default embeds real shared-broker credentials); must be an absolute `amqp://`/`amqps://` URI with a host and, when a port is given, a numeric port in `1..65535`; secret-bearing, never logged raw (logs use `config.RedactedAMQPURL`)
- `RABBITMQ_INPUT_EXCHANGE` — default `stock.summary`; present-but-blank is an error; max 255 bytes
- `RABBITMQ_INPUT_EXCHANGE_TYPE` — default `topic`; one of `direct`|`topic`|`fanout`|`headers`; blank selects `topic`
- `RABBITMQ_INPUT_ROUTING_KEY` — default `stock.summary.available.v1`; present-but-blank is an error; max 255 bytes
- `RABBITMQ_QUEUE` — default `stocker-informer`; present-but-blank is an error; max 255 bytes
- `RABBITMQ_PREFETCH` — default `1`; integer in `[1, 65535]`
- `RABBITMQ_CONNECT_TIMEOUT_SECONDS` — default `10`; integer in `[1, 300]`

Invalid configuration aborts startup with `load config: invalid configuration: ...` and exit code `1`.

## Testing

- `go test ./...` runs all unit tests offline: S3 uses an injected fake API, GoToSocial and Ollama use `httptest`, and the RabbitMQ consumer/probe tests inject fake connections, channels, dialers, and sleep functions.
- `go vet ./...` and `gofmt -l .` must be clean.
- The opt-in live RabbitMQ consumer test runs only when `RABBITMQ_LIVE_TEST=1`; it uses unique timestamped topology and connects to `RABBITMQ_URL` when set, otherwise to the built-in default broker:
  `RABBITMQ_LIVE_TEST=1 go test ./internal/rabbitmq -run TestLiveConsumer -v`
- The opt-in live GoToSocial test runs only when `GOTOSOCIAL_LIVE_TEST=1` and also requires `GOTOSOCIAL_INSTANCE`, `GOTOSOCIAL_USER`, `GOTOSOCIAL_TOKEN`, and `GOTOSOCIAL_RECIPIENT`.
- The opt-in live Ollama rewrite test runs only when `OLLAMA_LIVE_TEST=1`; it defaults to `http://192.168.1.71:11434` and `gemma4:26b`, overridable with `OLLAMA_BASE_URL` / `OLLAMA_MODEL`:
  `OLLAMA_LIVE_TEST=1 go test ./internal/rewriter -run TestOllamaLive -v`

No additional lint, format, or typecheck tooling beyond standard Go conventions.

## Tools

Credentials for the Forgejo instance git.wheeli.ca can be found in the parent directory (../credentials.md)
