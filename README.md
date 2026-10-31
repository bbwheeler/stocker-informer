# stocker-informer

`stocker-informer` is a long-running Go daemon that consumes protobuf stock summary signals from Kafka, reads each stock's Markdown `summary.md` from Garage/S3, rewrites it as a Cora-9 news report with a configurable Ollama instance, and publishes the resulting Markdown to a GoToSocial instance. If the rewrite fails, the original summary is published instead.

The producer (`stocker-summarizer`) writes the summary object and emits a minimal signal carrying only the stock's symbol and exchange. `stocker-informer` turns that signal into a published status — the Cora-9 report, or the original summary on fallback — with a configurable audience.

## Key features

- Kafka consumer-group subscription to protobuf `stockstore.v1.Stock` signals, with partition offset tracking so a group resumes where it left off across restarts.
- Summary reads from Garage/S3 use the canonical bucket-relative key `[S3_PATH_PREFIX/]{EXCHANGE}/{SYMBOL}/summary.md` only; a miss is one GET and returns `ErrNotFound`.
- Each non-empty summary is rewritten as a Cora-9 news report via Ollama (`POST {OLLAMA_BASE_URL}/api/generate`) before publication; the persona, model, and sampling are configurable.
- Any rewrite failure falls back to publishing the original summary, so an Ollama outage never stops or drops a publication.
- Full summaries (up to ~1000 words) are published without truncation when rewriting is disabled or falls back.
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
| Key dependencies | `github.com/segmentio/kafka-go v0.4.51`; AWS SDK for Go v2 (`config`, `credentials`, `service/s3`); `git.wheeli.ca/brian/stocker-store/proto/v1`; `google.golang.org/protobuf v1.36.12`; the Ollama HTTP API via the standard library (no additional module) |
| Configuration | 9 required environment variables + 14 optional |
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

# Ollama / Cora-9 rewrite (optional; these are the defaults)
export OLLAMA_ENABLED=true
export OLLAMA_BASE_URL=http://192.168.1.71:11434
export OLLAMA_MODEL=gemma4:26b

./stocker-informer
```

The binary runs until stopped. Press `Ctrl-C` (`SIGINT`); it logs `shutting down` and exits with code `0`.

## Prerequisites

- **Local build/run:** Go 1.25 or newer.
- A reachable **Kafka** broker with a topic carrying protobuf `stockstore.v1.Stock` signals (operationally `stock.summary.available.v1`).
- A reachable **Garage/S3** bucket that contains `summary.md` objects under the documented layout, plus static credentials with read access.
- A reachable **GoToSocial** instance with a valid auth token.
- A reachable **Ollama** instance (default `http://192.168.1.71:11434`) with the configured `OLLAMA_MODEL` already pulled (`ollama list`). If it is unreachable, the original summary is still published.
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

The service is configured entirely by environment variables: nine are required and fourteen have defaults. `config.Load()` reads every variable, applies defaults, validates all of them, and returns a single aggregated error naming each offender. A failure aborts the process with exit code `1`, for example:

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
| `S3_TIMEOUT_SECONDS` | optional, default `10` | no | Per-summary-read timeout in seconds (covers the summary read). Must parse as an integer and be `> 0`. |
| `OLLAMA_ENABLED` | optional, default `true` | no | Go boolean (`true`/`false`/`1`/`0`). When false, no Ollama call is made and the original summary is published. |
| `OLLAMA_BASE_URL` | optional, default `http://192.168.1.71:11434` | no | Ollama server root. Must be an absolute `http(s)` URL with scheme and host. A trailing `/` is trimmed and `/api/generate` is appended; do not include `/api`. |
| `OLLAMA_MODEL` | optional, default `gemma4:26b` | no | Model name passed to Ollama; must be non-empty and already pulled on the Ollama host. |
| `OLLAMA_TIMEOUT_SECONDS` | optional, default `120` | no | Per-rewrite HTTP timeout in seconds. Must parse as an integer and be `> 0`; covers connect, request, generation, and response read. |
| `OLLAMA_SYSTEM_PROMPT` | optional, default built-in Cora-9 prompt | no | Replaces the persona/system instruction. Leading and trailing whitespace is trimmed; blank means the built-in prompt. |
| `OLLAMA_TEMPERATURE` | optional, default `0.7` | no | Sampling temperature. Must be a finite number in `[0, 2]`; `0` is allowed. |
| `OLLAMA_MAX_TOKENS` | optional, default `1024` | no | Value sent as Ollama `options.num_predict`. Must parse as an integer and be `> 0`. |
| `OLLAMA_MAX_OUTPUT_CHARS` | optional, default `0` (unlimited) | no | Maximum acceptable report length in Unicode runes after trimming. Must parse as an integer and be `>= 0`. Output over the cap falls back to the original. |

### Validation notes

- All nine required variables are checked together; every empty one is named in the aggregated error rather than only the first.
- `GOTOSOCIAL_INSTANCE`, `S3_ENDPOINT`, and `OLLAMA_BASE_URL` must parse as absolute URLs with an `http`/`https` scheme and a host.
- `GOTOSOCIAL_AUDIENCE` is trimmed and lower-cased; anything outside `global|local|direct` is rejected.
- `S3_TIMEOUT_SECONDS` values that are non-integers or `<= 0` are rejected.
- `OLLAMA_ENABLED` must parse as a Go boolean; invalid values are rejected.
- `OLLAMA_MODEL` must be non-empty.
- `OLLAMA_TIMEOUT_SECONDS` and `OLLAMA_MAX_TOKENS` must be integers `> 0`; `OLLAMA_MAX_OUTPUT_CHARS` must be an integer `>= 0` (`0` means unlimited).
- `OLLAMA_TEMPERATURE` must be a finite number in `[0, 2]` (NaN and ±Inf are rejected).
- `OLLAMA_*` values are validated even when `OLLAMA_ENABLED=false`, so a typo surfaces at startup instead of when rewriting is re-enabled.

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
- The canonical key is the only key read; a missing summary returns `ErrNotFound`.
- Any other storage error (for example a 500 or a timeout) is returned immediately.
- Identities with `/` or `..` in a segment, or a prefix containing `..`, are rejected before any S3 call.

### Missing and empty summaries

- A summary that is missing at the canonical key is a warning (`summary not found`) and a skip, not a retry.
- A summary that trims to empty is a warning (`summary is empty, skipping publish`) and a skip.

### Publication

- The summary body is trimmed of surrounding whitespace. When `OLLAMA_ENABLED=true` it is rewritten as a Cora-9 news report before publication; see **Cora-9 rewrite (Ollama)**.
- The published text is the Cora-9 report when rewriting succeeds; otherwise it is the trimmed original summary.
- The published body is sent as `text/markdown` without truncation. `stocker-informer` adds no title, header, or other decoration (the model may format the report itself).
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

With `GOTOSOCIAL_AUDIENCE=global` and rewriting disabled (or on fallback), the resulting public status is exactly that Markdown (no added title line, no truncation). With rewriting enabled (the default), the status is the Cora-9 report generated from it. With `local`, the published body is posted as a public status restricted to the local timeline. With `direct`, the body is prefixed with `@<recipient> ` and posted as a direct status.

## Cora-9 rewrite (Ollama)

After a non-empty summary is read and trimmed, and before it is published, the consumer asks a configurable Ollama instance to rewrite it as a news report in the voice of **Cora-9** — a cheerful, friendly, intelligent, and demure robot woman who is a financial expert and news reporter. Missing, empty, and unreadable summaries never reach Ollama; rewriting is skipped and the existing skip/commit behavior applies.

### Request shape

- `POST {OLLAMA_BASE_URL}/api/generate` with `"stream": false` and `"think": false`.
- A `system` field carries the Cora-9 persona; the source summary is delimited with `<summary>` / `</summary>` in the `prompt` field and is treated as data, not instructions.
- `options.temperature` comes from `OLLAMA_TEMPERATURE`; `options.num_predict` comes from `OLLAMA_MAX_TOKENS`.
- Thinking is disabled because `gemma4:26b` otherwise spends the whole `num_predict` budget reasoning and leaves `response` empty; reasoning text is not publishable.
- The adapter uses only the Go standard library (`net/http`, `encoding/json`); there is no new module dependency.

The built-in persona preserves facts, figures, dates, and caveats, forbids inventing prices/events/guidance/quotes, stays warm and professional, and returns only the report. Setting `OLLAMA_SYSTEM_PROMPT` replaces it.

### Defaults

| Setting | Default |
| --- | --- |
| `OLLAMA_ENABLED` | `true` |
| `OLLAMA_BASE_URL` | `http://192.168.1.71:11434` |
| `OLLAMA_MODEL` | `gemma4:26b` |
| `OLLAMA_TIMEOUT_SECONDS` | `120` |
| `OLLAMA_TEMPERATURE` | `0.7` |
| `OLLAMA_MAX_TOKENS` | `1024` |
| `OLLAMA_MAX_OUTPUT_CHARS` | `0` (unlimited) |
| `OLLAMA_SYSTEM_PROMPT` | built-in Cora-9 prompt |

### Failure semantics

Any rewrite failure logs a warning and publishes the original summary. The offset is committed exactly once, as before; rewriting never skips a publication and never changes offset semantics.

| Condition | Published text | Log |
| --- | --- | --- |
| `OLLAMA_ENABLED=false` | original summary | `Info` published, `rewritten=false` |
| Success, non-empty, within cap | Cora-9 report | `Info` published, `rewritten=true` |
| Connection refused / DNS / TLS error | original summary | `Warn` rewrite failed + `Info` published, `rewritten=false` |
| Timeout (`OLLAMA_TIMEOUT_SECONDS`) | original summary | `Warn` rewrite failed + `Info` published, `rewritten=false` |
| Non-2xx (for example model not pulled → 404) | original summary | `Warn` rewrite failed + `Info` published, `rewritten=false` |
| Malformed JSON / truncated body | original summary | `Warn` rewrite failed + `Info` published, `rewritten=false` |
| `done:false` | original summary | `Warn` rewrite failed + `Info` published, `rewritten=false` |
| `done_reason:"length"` | original summary | `Warn` rewrite failed + `Info` published, `rewritten=false` |
| Blank `response` | original summary | `Warn` rewrite returned empty output + `Info` published, `rewritten=false` |
| Report longer than `OLLAMA_MAX_OUTPUT_CHARS` | original summary | `Warn` rewrite failed + `Info` published, `rewritten=false` |
| Rewrite succeeds, GoToSocial publish fails | — | `Error` publish failed (existing 3-attempt behavior) |

### Length strategy

- `OLLAMA_MAX_TOKENS` bounds generation via `num_predict`.
- If Ollama stops with `done_reason:"length"`, the output hit the token cap and is treated as a failure; the original summary is published.
- `OLLAMA_MAX_OUTPUT_CHARS` is an optional hard cap counted in Unicode runes after trimming. `0` means unlimited. Over the cap is a failure; output is never truncated mid-document.

### Privacy

The summary is sent over plain HTTP to `OLLAMA_BASE_URL`, so the host must be on a trusted network. No credentials are sent. The summary, the prompt, and the rewritten report are never logged; only lengths, identity fields, offsets, and error text are logged.

### Operational rollback

Set `OLLAMA_ENABLED=false` and restart (no code or image change) to publish original summaries unchanged. Validation of the other `OLLAMA_*` values still runs.

### Smoke test

```bash
curl -s http://192.168.1.71:11434/api/generate \
  -H 'Content-Type: application/json' \
  -d '{"model":"gemma4:26b","prompt":"Say hello in one sentence.","stream":false,"think":false}'
```

## Audience configuration

`GOTOSOCIAL_AUDIENCE` maps to explicit GoToSocial form fields. The publisher always sends `content_type=text/markdown`.

| Audience | `visibility` | `local_only` | `status` |
| --- | --- | --- | --- |
| `global` (default) | `public` | `false` | `<report or summary>` |
| `local` | `public` | `true` | `<report or summary>` |
| `direct` | `direct` | `false` | `@<recipient> <report or summary>` |

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

It then logs `configuration loaded` (with `kafka_topic`, `s3_endpoint`, `s3_bucket`, `audience`, `ollama_enabled`, `ollama_base_url`, and `ollama_model`) and `kafka consumer started`. It runs until `SIGINT` / `SIGTERM`, then logs `shutting down` and exits with code `0`.

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
   `mkdir -p ~/.config/stocker-informer` and create `~/.config/stocker-informer/.env.podman` from `deploy/quadlet/.env.podman.example` (see **Environment Variables**); fill in the Kafka, S3, and GoToSocial values. The `OLLAMA_*` defaults work as shipped; adjust them only if the Ollama host or model differs.

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
- `GOTOSOCIAL_TOKEN`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, the summary body, the rewritten report, and the Ollama system prompt are never logged.
- No log rotation configured (stdout / journald, operator-owned).

Representative log lines: `starting stocker-informer`, `configuration loaded`, `kafka consumer started`, `decode stock failed`, `summary not found`, `read summary failed`, `summary is empty, skipping publish`, `rewrite failed, publishing original summary`, `rewrite returned empty output, publishing original summary`, `publish failed`, `summary published on GoToSocial`, `exceeded max consecutive Kafka read failures`, `abandoning commit after max retries`, `shutting down`. The success line carries `rewritten=true|false`, plus `summary_chars` and `report_chars` when rewritten.

### Kafka consumer behaviour

- Consumer `GroupID=<KAFKA_CONSUMER_GROUP>`, topic `<KAFKA_TOPIC>`.
- `StartOffset=kafka.FirstOffset` — a fresh group starts at the beginning of the log.
- `MaxBytes=10_000_000`, `ReadBatchTimeout=5s`.
- Each record value is decoded as protobuf `stockstore.v1.Stock`, the summary is read from S3, rewritten as a Cora-9 report when enabled (falling back to the original on any rewrite error), and the resulting text is published.
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
| Summary not found | `Warn` (`summary not found`) | committed |
| S3 storage error (non-not-found) | `Error` (`read summary failed`) | committed |
| Summary trims to empty | `Warn` (`summary is empty, skipping publish`) | committed |
| Rewrite failure (or blank output) | `Warn` (`rewrite failed, publishing original summary`) | committed; original summary published |
| Publish failure after 3 attempts | `Error` (`publish failed`) | committed |
| Publish success | `Info` (`summary published on GoToSocial`, with `symbol`, `exchange`, `offset`, `rewritten`) | committed |

- A rewrite failure never skips the publish: the original summary is published and the message is committed exactly once.
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
2. Write a `NASDAQ/AAPL/summary.md` to the bucket and produce a protobuf `Stock{symbol:"AAPL",exchange:"NASDAQ"}` on `KAFKA_TOPIC`; the log shows `summary published on GoToSocial` with `rewritten=true`, and the status reads as a Cora-9 news report.
3. Rewrite fallback check: stop Ollama (or point `OLLAMA_BASE_URL` at an unused port); the same signal logs `rewrite failed, publishing original summary`, the original summary appears, and the success line shows `rewritten=false`. Set `OLLAMA_ENABLED=false` to confirm the same publish without a warning.
4. Confirm the status appears on the GoToSocial instance according to the configured audience.
5. Quick negative test: run the binary with one env var deliberately left empty — it should fail fast with a `load config: invalid configuration: ...` JSON fatal-error line and exit `1`.
6. S3 path check: with a missing summary, the log shows `summary not found` and the consumer continues (offset committed).
7. Restart check: stop/start and confirm the consumer resumes at the last committed offset for the same `KAFKA_CONSUMER_GROUP`.

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
│   ├── plan-ollama-cora9-rewrite.md
│   └── stock-summary-gotosocial.md
├── cmd/server/main.go
├── internal/config/config.go
├── internal/kafka/consumer.go
├── internal/messenger/messenger.go
├── internal/messenger/gotosocial_publisher.go
├── internal/rewriter/ollama.go
├── internal/rewriter/rewriter.go
├── internal/summarystore/store.go
└── deploy/
    ├── push.sh
    └── quadlet/
        ├── .env.podman.example
        └── stocker-informer.container
```

## Testing

```bash
gofmt -l .              # expected: no output
go build ./cmd/server/
go vet ./...
go test ./... -count=1
```

- Unit tests are offline: S3 uses an injected fake API, and GoToSocial and Ollama use `httptest`.
- The opt-in GoToSocial live test runs only when `GOTOSOCIAL_LIVE_TEST=1` and requires `GOTOSOCIAL_INSTANCE`, `GOTOSOCIAL_USER`, `GOTOSOCIAL_TOKEN`, and `GOTOSOCIAL_RECIPIENT`.
- The opt-in Ollama live test is gated by `OLLAMA_LIVE_TEST=1`, defaults to `http://192.168.1.71:11434` and `gemma4:26b`, and honours `OLLAMA_BASE_URL` / `OLLAMA_MODEL` when set (see `docs/plan-ollama-cora9-rewrite.md` §7.2):

  ```bash
  OLLAMA_LIVE_TEST=1 go test ./internal/rewriter -run TestOllamaLive -v
  ```

## Known limitations

- The Go module path is a placeholder (`github.com/example/stocker-informer`), not the canonical repo identity.
- The `/health` readiness flag is set to `true` at startup and never cleared (optimistic), and `:8080` is not published to the host by the quadlet unit as deployed.
- A failed GoToSocial publish is committed and dropped (not redelivered) after the publisher's 3 total attempts.
- `GOTOSOCIAL_USER` is required by config validation but is not currently referenced by the publish request (authentication is Bearer-token based).
- No log rotation is configured (JSON logging → stdout/journald, operator-owned).
- Rewriting adds up to `OLLAMA_TIMEOUT_SECONDS` latency per message; the consumer processes messages serially.
- Rewrite failures are not retried; the summary is published unrewritten.
- Model output is non-deterministic and may still omit or misstate details despite the prompt; the original summary is the source of truth.
- The summary is sent to Ollama over plain HTTP; the Ollama host must be trusted.
- No chunking or threading: a summary or report larger than the instance's status limit fails validation at GoToSocial, is retried 3 times, and is then dropped.
- Prompt-injection mitigations (persona rules plus `<summary>` delimiters) are best-effort, not a guarantee.
- S3 storage errors are not retried; the message is logged, the offset is committed, and the summary is skipped.
- Summary reads issue a single GET at the canonical key, so a missing summary costs one S3 round trip.
- The container build needs network and git access to `git.wheeli.ca` to resolve the private `stocker-store` module; no credentials are baked into the image.
- The operator-authored `~/.config/stocker-informer/.env.podman` is not shipped in this repo; `deploy/quadlet/.env.podman.example` contains placeholders only and must be edited for real deployments.

## Getting help

- Repository: `https://git.wheeli.ca/brian/stocker-informer`
- File issues via the Forgejo instance at `git.wheeli.ca`.
- No license is currently specified in the repository.
