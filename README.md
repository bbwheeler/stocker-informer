# stocker-informer

`stocker-informer` is a long-running Go daemon that subscribes to a Kafka topic of JSON stock events and republishes each one as a formatted, public post on a GoToSocial instance.

## Key features

- Kafka consumer-group subscription with partition offset tracking, so a group resumes where it left off across restarts.
- Per-event post formatting, truncated to stay within GoToSocial's 500-character status limit.
- In-process publish retries with up to 3 total attempts and exponential backoff.
- Structured JSON logging via `log/slog` to stdout.
- `/health` endpoint on `:8080`.
- Static, non-root distroless container image.
- Single rootless podman quadlet (systemd `--user`) deployment; image published to `git.wheeli.ca/brian` via `deploy/push.sh`.
- Graceful shutdown on `SIGINT` / `SIGTERM`.

## At a glance

| Item | Value |
| --- | --- |
| Language / Runtime | Go 1.25+ |
| Module | `github.com/example/stocker-informer` |
| Key dependency | `github.com/segmentio/kafka-go v0.4.47` |
| Configuration | 6 required environment variables |
| Build / publish | `go build ./cmd/server/` / `./deploy/push.sh` (→ `podman build` + `podman push` to `git.wheeli.ca/brian/stocker-informer:latest`) |
| Container base | `golang:1.25-bookworm` build → `gcr.io/distroless/static-debian12` runtime, user `nobody`/`65534` |
| Registry image | `git.wheeli.ca/brian/stocker-informer:latest` |
| Logging | JSON to stdout |
| Health | `/health` on `:8080` |
| Exit codes | `0` clean shutdown; `1` fatal error |

## Quick start

The shortest happy path to run the service natively:

```bash
git clone https://git.wheeli.ca/brian/stocker-informer
cd stocker-informer

# go.sum is not committed, so resolve module hashes locally first
go mod tidy

go build -o stocker-informer ./cmd/server/

# export the six required environment variables (see "Environment Variables")
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=stock-events
export KAFKA_CONSUMER_GROUP=stocker-informer
export GOTOSOCIAL_INSTANCE=https://social.example.com
export GOTOSOCIAL_USER=your-user
export GOTOSOCIAL_TOKEN=your-token

./stocker-informer
```

The binary runs until stopped. Press `Ctrl-C` (`SIGINT`); it logs `shutting down` and exits with code `0`.

## Prerequisites

- **Local build/run:** Go 1.25 or newer.
- A reachable **Kafka** broker with the stock-events topic.
- A reachable **GoToSocial** instance with a valid auth token.
- **Container / quadlet deployment:** `podman` (or `docker`) with systemd-user quadlet support; quadlet units live in `/etc/containers/systemd/`.
- `deploy/push.sh` requires an existing `podman login` to `git.wheeli.ca` (no root/sudo).
- The service is deployed **rootlessly** as a user systemd quadlet (`systemctl --user`) — no root/sudo at deploy time.
- The operator hands-writes `~/.config/stocker-informer/.env.podman` (the env file is **not** shipped in this repo).

## Installation

### Clone the source

```bash
git clone https://git.wheeli.ca/brian/stocker-informer
```

> **Note:** `go.sum` is not committed to git. Run `go mod download` / `go mod tidy` once locally before building the container image — the build resolves module hashes, and a missing `go.sum` in the build context can make the container build fail. (See `deploy/push.sh` for the registry build+push.)

Deployment is a single rootless quadlet plus a registry push — see **Publishing to the registry** (`deploy/push.sh`) and **Running the service → systemd / Podman quadlet**.

## Building

### Native binary

```bash
go build ./cmd/server/
# or with an explicit binary name
go build -o stocker-informer ./cmd/server/
```

> The runtime version/commit/build-time are reported as `dev` / `none` / `unknown` because the shipped build scripts do not set `-ldflags` to inject real values.

### Container image

```bash
podman build -t stocker-informer:latest -f Containerfile .
# docker also works
```

The build is two-stage:

- **Build stage:** `FROM golang:1.25-bookworm`, copies the source and runs `CGO_ENABLED=0 go build -o /out/stocker-informer ./cmd/server`.
- **Runtime stage:** `FROM gcr.io/distroless/static-debian12`, copies the binary to `/stocker-informer`, runs as `USER 65534:65534` (`nobody`), with `ENTRYPOINT ["/stocker-informer"]`.

CGO is disabled, the runtime base is static distroless, and the container runs as a non-root user (`65534`).

## Configuration

The service is configured entirely by the six environment variables in the next section. All six are required; there are no optional variables and no meaningful defaults (an empty value causes a config validation error). Config loading is the first action at startup, and a failure aborts the process with exit code `1`.

## Environment Variables

Exactly six environment variables are required. Each is validated as non-empty; an empty or missing value aborts startup with exit code `1`.

| Variable | Description | Default |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | **Required.** Comma-separated Kafka broker addresses, e.g. `localhost:9092` or `broker1:9092,broker2:9092`. Split on `,`, trimmed, empties dropped (see `Config.KafkaServers()`). | `""` (empty → required error) |
| `KAFKA_TOPIC` | **Required.** Name of the Kafka topic that carries the JSON stock events. | `""` |
| `KAFKA_CONSUMER_GROUP` | **Required.** Consumer-group ID; used for partition offset tracking so a group resumes where it left off across restarts. | `""` |
| `GOTOSOCIAL_INSTANCE` | **Required.** Base URL of the GoToSocial instance (scheme + host, e.g. `https://social.example.com`). The publisher POSTs to `{GOTOSOCIAL_INSTANCE}/api/v1/statuses`. | `""` |
| `GOTOSOCIAL_USER` | **Required.** GoToSocial username. Required by config validation, but the publish request authenticates via the Bearer token; this username is stored/surfaced in config but is **not** currently referenced by the publisher. | `""` |
| `GOTOSOCIAL_TOKEN` | **Required.** GoToSocial auth token, sent as `Authorization: Bearer <token>`. This is what actually authenticates the post. | `""` |

### Validation order and error messages

The three Kafka variables are checked in order `KAFKA_BOOTSTRAP_SERVERS` → `KAFKA_TOPIC` → `KAFKA_CONSUMER_GROUP`. The **first** empty one triggers a message naming that specific variable, e.g. `KAFKA_BOOTSTRAP_SERVERS is required` (analogous messages for the other two).

If any of the three GoToSocial variables is empty, a single combined message is returned:

```
GOTOSOCIAL_INSTANCE, GOTOSOCIAL_USER, and GOTOSOCIAL_TOKEN are all required
```

`main()` wraps the load error as `load config: <message>` and exits with code `1`, e.g.:

```
{"level":"ERROR","msg":"fatal error","error":"load config: KAFKA_TOPIC is required"}
```

### Example env file

You author this file yourself (it is **not** shipped in this repo) at `~/.config/stocker-informer/.env.podman`. Sample:

```
# Environment variables for Podman deployment (stocker-informer).
# Fill in GOTOSOCIAL_* before running.

# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=stock-events
KAFKA_CONSUMER_GROUP=stocker-informer

# --- GoToSocial (all required) ---
GOTOSOCIAL_INSTANCE=
GOTOSOCIAL_USER=
GOTOSOCIAL_TOKEN=
```

Fill in the GoToSocial trio before the service will start; the Kafka value `localhost:9092` is a development-only placeholder.

## Kafka input schema

### Payload fields

The JSON payload expected on the Kafka topic:

| JSON field | Go type | Required? | Notes |
| --- | --- | --- | --- |
| `symbol` | `string` | **Required** (non-empty, enforced by the decoder) | Shown in the post title line. |
| `company_name` | `string` | optional | Shown in italics on the second line (second line only emitted if `company_name` or `exchange` is non-empty). |
| `exchange` | `string` | optional | Part of the `(exchange/currency)` metadata. |
| `currency` | `string` | optional | Part of the `(exchange/currency)` metadata. |
| `price` | `float64` | required for meaningful output | Rendered as `Price: $X.XX` (two decimals). |
| `change_percent` | `float64` | optional | Rendered as `(+Y.Y%)`. |
| `sentiment_score` | `float64` | optional | Only rendered if greater than 0. |
| `timestamp` | `time.Time` (RFC3339) | — | Decoded but not currently rendered. |

### Sample event and rendered post

A fully-populated event:

```json
{
  "symbol": "AAPL",
  "company_name": "Apple Inc.",
  "exchange": "NASDAQ",
  "currency": "USD",
  "price": 213.42,
  "change_percent": 1.7,
  "sentiment_score": 0.62,
  "timestamp": "2026-09-02T10:00:00Z"
}
```

Formatted GoToSocial output for that event:

```
# Stock Update - AAPL
*Apple Inc.* (NASDAQ/USD)
Price: $213.42 (+1.7%) | Sentiment: 0.62
```

The formatter truncates overly long posts with a trailing `…` to stay within GoToSocial's 500-character status limit.

### Decode behaviour

- Non-JSON payloads, and payloads with a missing/empty `symbol`, are rejected, logged as an `Error`, and the offset is committed so the message is skipped rather than redelivered.
- If the formatted text ends up empty, it is logged as a `Warn` and skipped (offset committed).

## Running the service

### Local binary

Export the six required environment variables, then run the built binary. It emits structured JSON logs to stdout; the first line is:

```
{"level":"INFO","msg":"starting stocker-informer","version":"dev","commit":"none","build_time":"unknown"}
```

It runs until `SIGINT` / `SIGTERM`, then logs `{"level":"INFO","msg":"shutting down"}` and exits with code `0`.

### Container

```bash
podman run -it --rm \
  --env-file "$HOME/.config/stocker-informer/.env.podman" \
  stocker-informer:latest
```

Hardened form (matches the quadlet; `--read-only` keeps the root FS read-only, `--tmpfs` provides a small scratch `/tmp`):

```bash
podman run -it --rm \
  --env-file "$HOME/.config/stocker-informer/.env.podman" \
  --read-only \
  --tmpfs=/tmp:rw,noexec,nosuid,size=64m \
  stocker-informer:latest
```

### systemd / Podman quadlet (rootless)

A **single** unit lives in `deploy/quadlet/`: `stocker-informer.container`. It consumes the registry image (`AutoUpdate=registry` re-pulls a new `:latest` on auto-update) and runs it read-only with a scratch `/tmp`:

- `[Unit]` `After=network-online.target`.
- `[Container]` `Image=git.wheeli.ca/brian/stocker-informer:latest`, `EnvironmentFile=%h/.config/stocker-informer/.env.podman`, `PodmanArgs=--read-only`, `PodmanArgs=--tmpfs=/tmp:rw,noexec,nosuid,size=64m`, `AutoUpdate=registry`.
- `[Service]` `Restart=always`, `RestartSec=10`.
- `[Install]` `WantedBy=default.target` (starts on user login).

There is **no** separate build unit and **no** root install script: the image is built and published by `deploy/push.sh` (below).

Setup (rootless, no sudo; the env file is **not** shipped — you create it):

```bash
# 1. Author the config (fill in GOTOSOCIAL_* + KAFKA_* before starting)
mkdir -p ~/.config/stocker-informer
nano ~/.config/stocker-informer/.env.podman   # see "Environment Variables" / "Example env file"

# 2. Register the single quadlet with systemd --user
ln -s "$PWD/deploy/quadlet/stocker-informer.container" \
      "$HOME/.config/systemd/user/stocker-informer.container"
systemctl --user daemon-reload

# 3. Enable and start
systemctl --user enable --now stocker-informer

# 4. Verify
systemctl --user status stocker-informer
podman logs stocker-informer
```

## Publishing to the registry

`deploy/push.sh` (no root required) builds the image from the repo-root `Containerfile` and pushes the `:latest` tag to the `git.wheeli.ca/brian` registry:

```bash
./deploy/push.sh
```

It runs, equivalently:

- `podman build -t git.wheeli.ca/brian/stocker-informer:latest <repo-root>`
- `podman push git.wheeli.ca/brian/stocker-informer:latest`

It requires an existing `podman login` to `git.wheeli.ca` beforehand. The pushed `:latest` is exactly the image the quadlet consumes (see **Running the service → systemd / Podman quadlet**).

## Quickstart (Quadlets, end-to-end)

The shortest path to a running, rootless service:

1. **Write the env file** (the file is not shipped — you author it):
   `mkdir -p ~/.config/stocker-informer` and create `~/.config/stocker-informer/.env.podman` with the six required vars (see **Environment Variables**); fill `GOTOSOCIAL_INSTANCE` / `GOTOSOCIAL_USER` / `GOTOSOCIAL_TOKEN` and `KAFKA_*`.

2. **Build + push the image** to the registry (no root):
   `./deploy/push.sh`

3. **Register & enable the single quadlet**:
   `ln -s "$PWD/deploy/quadlet/stocker-informer.container" "$HOME/.config/systemd/user/stocker-informer.container"` then `systemctl --user daemon-reload` then `systemctl --user enable --now stocker-informer`

4. **Verify**:
   `systemctl --user status stocker-informer` and `podman logs stocker-informer`

## Operations

### Logging

- `log/slog` JSON handler writing to stdout.
- Levels emitted: `Info` / `Warn` / `Error`.
- No log-level flag.
- `GOTOSOCIAL_TOKEN` is never logged.
- No log rotation configured (stdout / journald, operator-owned).

Representative log lines: `starting stocker-informer` (with version/commit/build_time), `kafka consumer started`, `event published on GoToSocial` (with symbol and offset), `Message published on GoToSocial`, `exceeded max consecutive Kafka read failures`, `abandoning commit after max retries`, `shutting down`.

### Kafka consumer behaviour

- Consumer `GroupID=<KAFKA_CONSUMER_GROUP>`.
- `StartOffset=kafka.FirstOffset` — a fresh group starts at the beginning of the log.
- `MaxBytes=10_000_000`, `ReadBatchTimeout=5s`.
- On a `ReadMessage` error: exponential backoff starting at `500 ms`, doubling, capped at `30 s`; if `10` or more consecutive read failures occur it logs an `Error` (`exceeded max consecutive Kafka read failures`) but keeps retrying. A successful read resets the consecutive error count and backoff.

### GoToSocial publisher behaviour

- `POST {GOTOSOCIAL_INSTANCE}/api/v1/statuses`.
- Headers: `Content-Type: application/x-www-form-urlencoded`, `Authorization: Bearer <GOTOSOCIAL_TOKEN>`.
- Form body: `status=<formatted text>` and `visibility=public`.
- Up to `3` attempts with exponential backoff (500 ms doubled per attempt, capped at 30 s); `20 s` request timeout; the response body is drained on non-2xx.

### Offset & failure semantics

- Offsets are committed after every message — including decode failure, empty formatted text, and publish failure.
- A **failed** GoToSocial publish is committed and dropped; only the publisher's 3 total attempts apply, and a failed publish is **not** redelivered by the consumer.
- Offset commit (`commitWithRetries`) attempts up to `3` times (`100 ms` between tries); if all fail it logs `abandoning commit after max retries` and continues.

### Health endpoint

- `GET /health` on `:8080`.
- Returns `200` with body `{"status":"ok","version":"..."}` when ready, else `503` with `{"status":"starting","version":"..."}`.

Caveats:

- The `ready` flag is set to `true` at startup and never cleared (optimistic readiness).
- The quadlet unit as deployed does **not** publish port `8080` to the host — the endpoint is reachable only inside the container's network namespace.

### Signals & shutdown

- Honors context cancellation on `SIGINT` / `SIGTERM`.
- Closes the Kafka reader and returns on shutdown.
- A health HTTP server `ListenAndServe` failure (e.g. `:8080` already in use) is logged as a `Warn` (`health check server error`) but does **not** stop the consumer.

### How to verify it's working

1. `podman logs stocker-informer` (or the process stdout) shows `starting stocker-informer` then `kafka consumer started`.
2. Produce a test stock event (e.g. the AAPL sample) on `KAFKA_TOPIC`; the log shows `event published on GoToSocial`.
3. Confirm the post appears on the GoToSocial instance for the token's account.
4. Quick negative test: run the binary with one env var deliberately left empty — it should fail fast with a `load config: ...` JSON fatal-error line and exit `1`.
5. Restart check: stop/start and confirm the consumer resumes at the last committed offset for the same `KAFKA_CONSUMER_GROUP`.

## Repository structure

```
stocker-informer/
├── AGENTS.md
├── Containerfile
├── review.md
├── go.mod
├── README.md
├── docs/deployment.md
├── cmd/server/main.go
├── internal/config/config.go
├── internal/kafka/consumer.go
├── internal/messenger/messenger.go
├── internal/messenger/gotosocial_publisher.go
├── internal/messenger/stock_event_formatter.go
└── deploy/
    ├── push.sh
    └── quadlet/
        └── stocker-informer.container
```

## Known limitations

- `go.sum` is not committed; run `go mod download` / `go mod tidy` once locally before building the container image (via `deploy/push.sh` or `podman build`), or the container build can fail on unresolved module hashes.
- The Go module path is a placeholder (`github.com/example/stocker-informer`), not the canonical repo identity.
- No automated tests are present yet; `go test ./...` is a no-op.
- The `/health` readiness flag is set to `true` at startup and never cleared (optimistic), and `:8080` is not published to the host by the quadlet unit as deployed.
- A failed GoToSocial publish is committed and dropped (not redelivered) after the publisher's 3 total attempts.
- `GOTOSOCIAL_USER` is required by config validation but is not currently referenced by the publish request (authentication is Bearer-token based).
- No log rotation is configured (JSON logging → stdout/journald, operator-owned).
- The operator-authored `~/.config/stocker-informer/.env.podman` sample shows `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` (development-only); it is not shipped in this repo and must be changed for real deployments.

## Getting help

- Repository: `https://git.wheeli.ca/brian/stocker-informer`
- File issues via the Forgejo instance at `git.wheeli.ca`.
- No license is currently specified in the repository.
