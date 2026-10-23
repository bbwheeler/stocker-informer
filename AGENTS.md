# stocker-informer

Go CLI daemon that consumes protobuf `stockstore.v1.Stock` summary signals from Kafka, reads each stock's `summary.md` from Garage/S3, and publishes the Markdown summary to GoToSocial.

## Key facts

- **Module**: `github.com/example/stocker-informer` | **Go**: 1.25
- **Build/run**: `go build ./cmd/server/` | **Test**: `go test ./...`
- **Lint/static checks**: `gofmt -l .`, `go vet ./...`
- **Kafka dependency**: `github.com/segmentio/kafka-go` v0.4.51
- **Other key dependencies**: AWS SDK for Go v2 (`config`, `credentials`, `service/s3`), `git.wheeli.ca/brian/stocker-store/proto/v1`, `google.golang.org/protobuf` v1.36.12

## Structure

```
cmd/server/main.go        # entry point / composition root
internal/config/          # env loading, defaults, validation, audience + recipient
internal/kafka/           # Kafka consumer + protobuf Stock decoder + dispatch
internal/summarystore/    # Garage/S3 summary reader (canonical unrooted key only)
internal/messenger/       # GoToSocial publisher + Publisher interface
deploy/quadlet/           # single rootless quadlet + .env.podman.example template
docs/                     # implementation plans (stock-summary-gotosocial.md, deployment.md)
```

## Architecture

`main()` loads config → builds the S3 summary store → builds the audience-aware GoToSocial publisher → constructs the Kafka consumer. The consumer subscribes to the topic, decodes each record value as protobuf `stockstore.v1.Stock`, reads `[S3_PATH_PREFIX/]{EXCHANGE}/{SYMBOL}/summary.md` from Garage/S3, and POSTs the Markdown body to `{GOTOSOCIAL_INSTANCE}/api/v1/statuses` with the configured audience (`global`/`local`/`direct`). Every outcome logs and commits the offset.

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

Invalid configuration aborts startup with `load config: invalid configuration: ...` and exit code `1`.

## Testing

- `go test ./...` runs all unit tests (no network; S3 uses an injected fake API, GoToSocial uses `httptest`).
- `go vet ./...` and `gofmt -l .` must be clean.
- The opt-in live GoToSocial test runs only when `GOTOSOCIAL_LIVE_TEST=1` and also requires `GOTOSOCIAL_INSTANCE`, `GOTOSOCIAL_USER`, `GOTOSOCIAL_TOKEN`, and `GOTOSOCIAL_RECIPIENT`.

No additional lint, format, or typecheck tooling beyond standard Go conventions.

## Tools

Credentials for the Forgejo instance git.wheeli.ca can be found in the parent directory (../credentials.md)
