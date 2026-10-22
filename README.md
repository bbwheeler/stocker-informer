# stocker-informer

`stocker-informer` is a long-running Go daemon that consumes protobuf stock summary signals from Kafka, reads each stock's Markdown `summary.md` from Garage/S3, and publishes the full summary to a GoToSocial instance.

The producer (`stocker-summarizer`) writes the summary object and emits a minimal signal carrying only the stock's symbol and exchange. `stocker-informer` turns that signal into a published status with a configurable audience.

## Key features

- Kafka consumer-group subscription to protobuf `stockstore.v1.Stock` signals, with partition offset tracking so a group resumes where it left off across restarts.
- Summary reads from Garage/S3 using the canonical key `[S3_PATH_PREFIX/]{EXCHANGE}/{SYMBOL}/summary.md`, with a one-shot rooted-key (`/{key}`) fallback for objects written by the older summarizer layout.
- Full summaries (up to ~1000 words) are published without truncation.
- Configurable GoToSocial audience: `global`, `local`, or `direct`.
- Markdown statuses sent with `content_type=text/markdown`.
- In-process publish retries with up to 3 total attempts and exponential backoff.
- Structured JSON logging via `log/slog` to stdout.
- `/health` endpoint on `:8080`.
- Static, non-root distroless container image.
- Single rootless podman quadlet (systemd `--user`) deployment; image published to `git.wheeli.ca/brian` via `deploy/push.sh`.
- Graceful shutdown on `SIGINT` / `SIGTERM`.

## At a glance

| Item | Value |
| --- | --- |
| Language / Runtime | Go 1.25 |
| Module | `github.com/example/stocker-informer` |
| Key dependencies | `github.com/segmentio/kafka-go v0.4.51`; AWS SDK for Go v2 (`config`, `credentials`, `service/s3`); `git.wheeli.ca/brian/stocker-store/proto/v1`; `google.golang.org/protobuf v1.36.12` |
| Configuration | 9 required environment variables + 6 optional |
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

go build -o stocker-informer ./cmd/server/

# Kafka
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=stock.summary.available.v1
export KAFKA_CONSUMER_GROUP=stocker-informer

# Garage / S3 (bucket already contains summaries)
export S3_ENDPOINT=http://192.168.1.31:3900
export S3_BUCKET=stocker-summaries
export S3_ACCESS_KEY=your-access-key
export S3_SECRET_KEY=your-secret-key

# GoToSocial
export GOTOSOCIAL_INSTANCE=https://social.example.com
export GOTOSOCIAL_USER=your-user
export GOTOSOCIAL_TOKEN=your-token
export GOTOSOCIAL_AUDIENCE=global

./stocker-informer
```

The binary runs until stopped. Press `Ctrl-C` (`SIGINT`); it logs `shutting down` and exits with code `0`.

## Prerequisites

- **Local build/run:** Go 1.25 or newer.
- A reachable **Kafka** broker with a topic carrying protobuf `stockstore.v1.Stock` signals (operationally `stock.summary.available.v1`).
- A reachable **Garage/S3** bucket that contains `summary.md` objects under the documented layout, plus static credentials with read access.
- A reachable **GoToSocial** instance with a valid auth token.
- **Container / quadlet deployment:** `podman` (the quadlet engine; docker cannot run quadlets) with systemd-user quadlet support; quadlet units live in the user's `~/.config/containers/systemd/` and are registered/enabled via `~/.config/systemd/user/` (`systemctl --user`).
- `deploy/push.sh` requires an existing `podman login` to `git.wheeli.ca` (no root/sudo).
- The service is deployed **rootlessly** as a user systemd quadlet (`systemctl --user`) — no root/sudo at deploy time.
- The operator hands-writes `~/.config/stocker-informer/.env.podman` (the env file is **not** shipped in this repo). `deploy/quadlet/.env.podman.example` is a placeholder-only template.

## Installation

### Clone the source

```bash
git clone https://git.wheeli.ca/brian/stocker-informer
```

`go.sum` is committed, so a fresh clone builds without resolving module hashes first. Deployment is a single rootless quadlet plus a registry push — see **Publishing to the registry** (`deploy/push.sh`) and **Running the service → systemd / Podman quadlet**.

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

- **Build stage:** `FROM golang:1.25-bookworm`, copies `go.mod go.sum`, sets `GOPRIVATE=git.wheeli.ca` (the `stocker-store` module is fetched directly from `git.wheeli.ca`), runs `go mod download`, copies the source, then runs `CGO_ENABLED=0 go build -o /out/stocker-informer ./cmd/server`.
- **Runtime stage:** `FROM gcr.io/distroless/static-debian12`, copies the binary to `/stocker-informer`, runs as `USER 65534:65534` (`nobody`), with `ENTRYPOINT ["/stocker-informer"]`.

CGO is disabled, the runtime base is static distroless, and the container runs as a non-root user (`65534`). No credentials are baked into the image; the builder needs network and git access to `git.wheeli.ca` to resolve the private module.

## Configuration

The service is configured entirely by environment variables: nine are required and six have defaults. `config.Load()` reads every variable, applies defaults, validates all of them, and returns a single aggregated error naming each offender. A failure aborts the process with exit code `1`, for example:

```
{"level":"ERROR","msg":"fatal error","error":"load config: invalid configuration: KAFKA_TOPIC is required and must be non-empty; S3_BUCKET is required and must be non-empty"}
```

Secret values (`GOTOSOCIAL_TOKEN`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`) are never included in errors or logs.

## Environment Variables

| Variable | Required / default | Secret | Meaning |
| --- | --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | **Required** | no | Comma-separated broker addresses, e.g. `localhost:9092` or `broker1:9092,broker2:9092`. Split on `,`, trimmed, empty entries dropped; at least one non-empty broker is required. |
| `KAFKA_TOPIC` | **Required** | no | Topic carrying protobuf `stockstore.v1.Stock` signals. Operationally `stock.summary.available.v1`. |
| `KAFKA_CONSUMER_GROUP` | **Required** | no | Consumer-group ID used for partition offset tracking. |
| `GOTOSOCIAL_INSTANCE` | **Required** | no | Absolute `http(s)` base URL of the instance (scheme + host), e.g. `https://wheeli.social`. The publisher POSTs to `{GOTOSOCIAL_INSTANCE}/api/v1/statuses`. |
| `GOTOSOCIAL_USER` | **Required** | no | Account name. Stored and logged only as metadata; the publish request authenticates with the Bearer token. |
| `GOTOSOCIAL_TOKEN` | **Required** | **yes** | Bearer token for `/api/v1/statuses`, sent as `Authorization: Bearer <token>`. Never logged. |
| `GOTOSOCIAL_AUDIENCE` | optional, default `global` | no | `global` \| `local` \| `direct` (case-insensitive, trimmed). |
| `GOTOSOCIAL_RECIPIENT` | required iff `GOTOSOCIAL_AUDIENCE=direct`; ignored otherwise | no | Fediverse handle, e.g. `papa@wheeli.social`. One leading `@` is stripped; a handle without `@domain` borrows the hostname of `GOTOSOCIAL_INSTANCE`. |
| `S3_ENDPOINT` | optional, default `http://192.168.1.31:3900` | no | Garage/S3 base endpoint. Must be an absolute URL with scheme and host; path-style addressing is always used. |
| `S3_REGION` | optional, default `us-east-1` | no | AWS region passed to the SDK. |
| `S3_BUCKET` | **Required** | no | Bucket containing the summary objects. |
| `S3_ACCESS_KEY` | **Required** | **yes** | Static S3 access key. Never logged. |
| `S3_SECRET_KEY` | **Required** | **yes** | Static S3 secret key. Never logged. |
| `S3_PATH_PREFIX` | optional, default empty | no | Key prefix placed before `{EXCHANGE}/{SYMBOL}/summary.md`. Leading/trailing slashes are trimmed and empty segments dropped. |
| `S3_TIMEOUT_SECONDS` | optional, default `10` | no | Per-summary-read timeout in seconds (covers the canonical read plus any fallback). Must parse as an integer and be `> 0`. |

### Validation notes

- All nine required variables are checked together; every empty one is named in the aggregated error rather than only the first.
- `GOTOSOCIAL_INSTANCE` and `S3_ENDPOINT` must both parse as absolute URLs with an `http`/`https` scheme and a host.
- `GOTOSOCIAL_AUDIENCE` is trimmed and lower-cased; anything outside `global|local|direct` is rejected.
- `S3_TIMEOUT_SECONDS` values that are non-integers or `<= 0` are rejected.

### Example env file

A placeholder-only template ships at `deploy/quadlet/.env.podman.example`. Copy it to `~/.config/stocker-informer/.env.podman` (the path the quadlet reads) and replace every `REPLACE_ME`:

```bash
mkdir -p ~/.config/stocker-informer
cp deploy/quadlet/.env.podman.example ~/.config/stocker-informer/.env.podman
chmod 600 ~/.config/stocker-informer/.env.podman
# then edit in the real Kafka brokers, S3 bucket and keys, and GoToSocial token
```

The env file itself is **not** shipped — the operator owns it and it must never be committed with real credentials (`.gitignore` ignores `.env` / `.env.*`, while `*.example` files stay trackable).

## Kafka input schema

### Record value

Each record value is a protobuf-encoded `stockstore.v1.Stock`, imported from `git.wheeli.ca/brian/stocker-store/proto/v1`. The relevant schema:

```proto
message Stock {
  string symbol = 1;
  string exchange = 2;
  repeated ScoreEntry scores = 3;
}

message ScoreEntry {
  string category = 1;
  double value = 2;
  google.protobuf.Timestamp updated_at = 3;
}
```

- Only `symbol` and `exchange` are used. Both are trimmed and must be non-empty; the exchange is required because it forms the S3 path.
- `scores`, the record key, and record headers are ignored.

### Producer and topic

- Producer: `stocker-summarizer`.
- Topic (operational): `stock.summary.available.v1`, configured through `KAFKA_TOPIC`.

### Decode behaviour

- A value that is not valid protobuf, or that trims to an empty `symbol` or `exchange`, is logged as an `Error` (`decode stock failed`) and the offset is committed so the message is skipped rather than redelivered.
- The summary body is never logged.

## Summary source

### Object key

```text
[S3_PATH_PREFIX/]{EXCHANGE}/{SYMBOL}/summary.md
```

- `EXCHANGE` and `SYMBOL` are trimmed and upper-cased.
- The canonical key never has a leading slash, e.g. `NASDAQ/AAPL/summary.md` (or `corp/NASDAQ/AAPL/summary.md` with `S3_PATH_PREFIX=corp`).
- A rooted compatibility key (`/NASDAQ/AAPL/summary.md`) is read **only** after the canonical read is a definitive not-found. This covers objects written by the older summarizer layout.
- Any other storage error (for example a 500 or a timeout) is returned immediately and does **not** trigger the fallback.
- Identities with `/` or `..` in a segment, or a prefix containing `..`, are rejected before any S3 call.

### Missing and empty summaries

- A summary that is missing at both keys is a warning (`summary not found`) and a skip, not a retry.
- A summary that trims to empty is a warning (`summary is empty, skipping publish`) and a skip.

### Publication

- The summary body is trimmed of surrounding whitespace and published verbatim as `text/markdown`.
- No title, header, or other decoration is added, and the body is not truncated.
- If the instance's status limit is exceeded, GoToSocial returns a non-2xx, the publish is retried, and then the message is dropped (see **Known limitations**).

### Sample

`NASDAQ/AAPL/summary.md`:

```markdown
# AAPL — Apple Inc.

Apple beat consensus on services revenue, and guidance implies steady demand
into the next quarter.

- Revenue: $94.9B (+6% YoY)
- Services: $24.2B (+14% YoY)
- Gross margin: 46.3%

**Takeaway:** hardware demand is stable; margin expansion depends on services mix.
```

With `GOTOSOCIAL_AUDIENCE=global`, the resulting public status is exactly that Markdown (no added title line, no truncation). With `local`, the same body is posted as a public status restricted to the local timeline. With `direct`, the body is prefixed with `@<recipient> ` and posted as a direct status.

## Audience configuration

`GOTOSOCIAL_AUDIENCE` maps to explicit GoToSocial form fields. The publisher always sends `content_type=text/markdown`.

| Audience | `visibility` | `local_only` | `status` |
| --- | --- | --- | --- |
| `global` (default) | `public` | `false` | `<summary>` |
| `local` | `public` | `true` | `<summary>` |
| `direct` | `direct` | `false` | `@<recipient> <summary>` |

### Direct recipient normalization

`GOTOSOCIAL_RECIPIENT` is required only for `direct`. It is normalized by config:

- Surrounding whitespace is trimmed and one leading `@` is stripped.
- A handle without an `@domain` gets the hostname (no port) of `GOTOSOCIAL_INSTANCE` appended.
- A handle containing more than one `@` is rejected.

Examples with `GOTOSOCIAL_INSTANCE=https://wheeli.social`:

| Input | Normalized | Resulting status prefix |
| --- | --- | --- |
| `papa` | `papa@wheeli.social` | `@papa@wheeli.social ` |
| `@papa@wheeli.social` | `papa@wheeli.social` | `@papa@wheeli.social ` |
| `papa@example.org` | `papa@example.org` | `@papa@example.org ` |

For `global` and `local`, `GOTOSOCIAL_RECIPIENT` is ignored and stored as empty.

## Running the service

### Local binary

Export the environment variables, then run the built binary. It emits structured JSON logs to stdout; the first line is:

```
{"level":"INFO","msg":"starting stocker-informer","version":"dev","commit":"none","build_time":"unknown"}
```

It then logs `configuration loaded` (with `kafka_topic`, `s3_endpoint`, `s3_bucket`, `audience`) and `kafka consumer started`. It runs until `SIGINT` / `SIGTERM`, then logs `shutting down` and exits with code `0`.

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
# 1. Author the config (fill in Kafka, S3, and GoToSocial values before starting)
mkdir -p ~/.config/stocker-informer
cp deploy/quadlet/.env.podman.example ~/.config/stocker-informer/.env.podman
chmod 600 ~/.config/stocker-informer/.env.podman
nano ~/.config/stocker-informer/.env.podman

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
   `mkdir -p ~/.config/stocker-informer` and create `~/.config/stocker-informer/.env.podman` from `deploy/quadlet/.env.podman.example` (see **Environment Variables**); fill in the Kafka, S3, and GoToSocial values.

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
- `GOTOSOCIAL_TOKEN`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, and the summary body are never logged.
- No log rotation configured (stdout / journald, operator-owned).

Representative log lines: `starting stocker-informer`, `configuration loaded`, `kafka consumer started`, `decode stock failed`, `summary not found`, `read summary failed`, `summary is empty, skipping publish`, `publish failed`, `summary published on GoToSocial`, `exceeded max consecutive Kafka read failures`, `abandoning commit after max retries`, `shutting down`.

### Kafka consumer behaviour

- Consumer `GroupID=<KAFKA_CONSUMER_GROUP>`, topic `<KAFKA_TOPIC>`.
- `StartOffset=kafka.FirstOffset` — a fresh group starts at the beginning of the log.
- `MaxBytes=10_000_000`, `ReadBatchTimeout=5s`.
- Each record value is decoded as protobuf `stockstore.v1.Stock`, the summary is read from S3, and the summary is published.
- On a `ReadMessage` error: exponential backoff starting at `500 ms`, doubling, capped at `30 s`; if `10` or more consecutive read failures occur it logs an `Error` (`exceeded max consecutive Kafka read failures`) but keeps retrying. A successful read resets the consecutive error count and backoff.

### GoToSocial publisher behaviour

- `POST {GOTOSOCIAL_INSTANCE}/api/v1/statuses`.
- Headers: `Content-Type: application/x-www-form-urlencoded`, `Authorization: Bearer <GOTOSOCIAL_TOKEN>`.
- Form body: `status`, `visibility`, `local_only`, and `content_type=text/markdown` per the **Audience configuration** table.
- Up to `3` attempts with exponential backoff (500 ms doubling per attempt, capped at 30 s); `20 s` request timeout; the response body is drained on non-2xx.

### Offset & failure semantics

Offsets are committed after every message, in every outcome:

| Outcome | Log level | Offset |
| --- | --- | --- |
| Decode failure | `Error` (`decode stock failed`) | committed |
| Summary not found at canonical and rooted keys | `Warn` (`summary not found`) | committed |
| S3 storage error (non-not-found) | `Error` (`read summary failed`) | committed |
| Summary trims to empty | `Warn` (`summary is empty, skipping publish`) | committed |
| Publish failure after 3 attempts | `Error` (`publish failed`) | committed |
| Publish success | `Info` (`summary published on GoToSocial`, with `symbol`, `exchange`, `offset`) | committed |

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

1. `podman logs stocker-informer` (or the process stdout) shows `starting stocker-informer` then `configuration loaded` then `kafka consumer started`.
2. Write a `NASDAQ/AAPL/summary.md` to the bucket and produce a protobuf `Stock{symbol:"AAPL",exchange:"NASDAQ"}` on `KAFKA_TOPIC`; the log shows `summary published on GoToSocial`.
3. Confirm the status appears on the GoToSocial instance according to the configured audience.
4. Quick negative test: run the binary with one env var deliberately left empty — it should fail fast with a `load config: invalid configuration: ...` JSON fatal-error line and exit `1`.
5. S3 path check: with a missing summary, the log shows `summary not found` and the consumer continues (offset committed).
6. Restart check: stop/start and confirm the consumer resumes at the last committed offset for the same `KAFKA_CONSUMER_GROUP`.

## Repository structure

```
stocker-informer/
├── .gitignore
├── AGENTS.md
├── Containerfile
├── review.md
├── go.mod
├── go.sum
├── README.md
├── docs/
│   ├── deployment.md
│   └── stock-summary-gotosocial.md
├── cmd/server/main.go
├── internal/config/config.go
├── internal/kafka/consumer.go
├── internal/messenger/messenger.go
├── internal/messenger/gotosocial_publisher.go
├── internal/summarystore/store.go
└── deploy/
    ├── push.sh
    └── quadlet/
        ├── .env.podman.example
        └── stocker-informer.container
```

## Known limitations

- The Go module path is a placeholder (`github.com/example/stocker-informer`), not the canonical repo identity.
- The `/health` readiness flag is set to `true` at startup and never cleared (optimistic), and `:8080` is not published to the host by the quadlet unit as deployed.
- A failed GoToSocial publish is committed and dropped (not redelivered) after the publisher's 3 total attempts.
- `GOTOSOCIAL_USER` is required by config validation but is not currently referenced by the publish request (authentication is Bearer-token based).
- No log rotation is configured (JSON logging → stdout/journald, operator-owned).
- No chunking or threading: a summary larger than the instance's status limit fails validation at GoToSocial, is retried 3 times, and is then dropped.
- S3 storage errors are not retried; the message is logged, the offset is committed, and the summary is skipped.
- The rooted-key fallback runs only after a definitive canonical miss; a transient error on the canonical key does not try the rooted key.
- The container build needs network and git access to `git.wheeli.ca` to resolve the private `stocker-store` module; no credentials are baked into the image.
- The operator-authored `~/.config/stocker-informer/.env.podman` is not shipped in this repo; `deploy/quadlet/.env.podman.example` contains placeholders only and must be edited for real deployments.

## Getting help

- Repository: `https://git.wheeli.ca/brian/stocker-informer`
- File issues via the Forgejo instance at `git.wheeli.ca`.
- No license is currently specified in the repository.
