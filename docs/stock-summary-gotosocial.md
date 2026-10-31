# Stock Summary → GoToSocial Implementation Plan

**Goal:** Replace the JSON `StockEvent` pipeline with a protobuf `stockstore.v1.Stock` consumer that reads a stock's `summary.md` from Garage/S3 and publishes the full Markdown summary to GoToSocial with a configurable audience (`global`, `local`, or `direct`).

**Architecture:** `stocker-summarizer` writes `summary.md` to Garage/S3 and emits a minimal protobuf `Stock` (`symbol` + `exchange`) on `stock.summary.available.v1`. `stocker-informer` decodes that record, reads `[S3_PATH_PREFIX/]{EXCHANGE}/{SYMBOL}/summary.md`, and POSTs the summary as `text/markdown` to `/api/v1/statuses`. The composition root in `cmd/server/main.go` wires config, the S3 summary store, the audience-aware publisher, and the Kafka consumer.

**Tech Stack:** Go 1.25, `github.com/segmentio/kafka-go` v0.4.47, `git.wheeli.ca/brian/stocker-store/proto/v1` (`v0.0.0-20261010025329-1a26328c2930`), AWS SDK for Go v2 (S3), `google.golang.org/protobuf` v1.36.12, `log/slog`, `net/http`, distroless container.

**Spec:** This document. It is both the design record and the step-by-step implementation plan for the change requested on 2026-10-22.

> **Update (2026-10-31):** the pipeline now rewrites each summary as a Cora-9
> news report with Ollama before publication, falling back to the original
> summary on any rewrite failure. See
> `docs/plan-ollama-cora9-rewrite.md`. The rest of this document remains the
> design record for the summary → GoToSocial pipeline.

## Global Constraints

- **Module:** `github.com/example/stocker-informer`. **Go:** `1.25.0`. No new frameworks or linters; standard `gofmt` / `go vet` / `go test`.
- **Kafka input topic (recommended):** `stock.summary.available.v1`. The config name stays `KAFKA_TOPIC`.
- **Kafka record value:** protobuf `stockstore.v1.Stock`; key and headers are ignored.
- **Summary object key:** `[S3_PATH_PREFIX/]{EXCHANGE}/{SYMBOL}/summary.md`, canonical form with no leading slash.
- **Summary body:** Markdown, up to ~1000 words. Never truncate it.
- **GoToSocial:** always send `content_type=text/markdown`; audience mapping is defined in §3.
- **Failure semantics (unchanged):** every message is committed after handling, including decode failure, missing summary, storage error, empty summary, and publish failure. No redelivery beyond the publisher's 3 attempts.
- **Secrets:** `GOTOSOCIAL_TOKEN`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY` are never logged and never committed. No token from the task brief may appear in any file.
- **Dependency versions:** `github.com/aws/aws-sdk-go-v2 v1.47.2`, `.../config v1.33.8`, `.../credentials v1.20.8`, `.../service/s3 v1.114.2`, `github.com/aws/smithy-go v1.28.4`, `google.golang.org/protobuf v1.36.12`, `git.wheeli.ca/brian/stocker-store v0.0.0-20261010025329-1a26328c2930`. Keep `github.com/segmentio/kafka-go v0.4.47` unless `go mod tidy` requires otherwise.
- **Deployment mechanics unchanged:** single rootless quadlet, `deploy/push.sh`, image `git.wheeli.ca/brian/stocker-informer:latest`, env file `~/.config/stocker-informer/.env.podman`.

---

## 1. Goal, Architecture, and Data Flow

### 1.1 Goal

A summary is now the unit of publication. A stock event no longer carries price/sentiment; it signals that a Markdown summary exists. The daemon's job is:

1. Consume the signal.
2. Identify the stock (`exchange`, `symbol`).
3. Read its summary from Garage/S3.
4. Publish the summary to GoToSocial with an operator-selected audience.

### 1.2 Data flow

```text
stocker-summarizer
  ├─ writes s3://$S3_BUCKET/[prefix/]NASDAQ/AAPL/summary.md   (text/markdown)
  └─ produces protobuf Stock{symbol:"AAPL", exchange:"NASDAQ"}
        on Kafka topic stock.summary.available.v1

stocker-informer
  ├─ kafka.Consumer reads record value
  ├─ proto.Unmarshal → stockstorev1.Stock
  ├─ validate symbol + exchange non-empty
  ├─ summarystore.ReadSummary(ctx, "NASDAQ", "AAPL")
  │    └─ GET  NASDAQ/AAPL/summary.md          (canonical, single GET)
  └─ messenger.GoToSocialPublisher.Publish(ctx, summaryMarkdown)
       POST {GOTOSOCIAL_INSTANCE}/api/v1/statuses
       form: status, visibility, local_only, content_type=text/markdown
```

The record key and headers are ignored. Missing summaries are a warning and a skip, not a retry.

### 1.3 Package layout

| Package | Responsibility | Imports allowed |
| --- | --- | --- |
| `internal/config` | Read env, apply defaults, validate, normalize recipient. Owns `Audience` and `S3Config`. Leaf package. | stdlib only |
| `internal/summarystore` | Garage/S3 adapter: key building, canonical read, timeout, `ErrNotFound`. | `internal/config`, AWS SDK, smithy |
| `internal/messenger` | GoToSocial HTTP adapter and `Publisher` interface. Audience → form fields. | `internal/config`, stdlib |
| `internal/kafka` | Consumer loop, protobuf decode, summary dispatch, offset commit. | `internal/messenger`, `internal/summarystore`, `stocker-store/proto/v1`, kafka-go |
| `cmd/server` | Composition root: config → store → publisher → consumer + health server. | all of the above |

Dependency direction is one-way: `config` ← `summarystore` / `messenger` ← `kafka` ← `main`. No cycles.

---

## 2. Decision Log

### D1 — Kafka input is the real `stocker-store` protobuf

**Decision:** Import `stockstorev1 "git.wheeli.ca/brian/stocker-store/proto/v1"` and `proto.Unmarshal` the record value into `stockstorev1.Stock`. Pin `v0.0.0-20261010025329-1a26328c2930`, the version already used by `stocker-summary-evaluator`.

**Why:** The sibling services already produce this exact contract (`stocker-summarizer` emits a minimal `Stock` carrying only `Symbol` + `Exchange`). Sharing the generated type avoids a hand-written mirror struct and keeps the wire contract single-sourced. JSON is gone.

**Validation:** `symbol` and `exchange` are trimmed and both required. The exchange is needed to build the S3 path; a stock without it cannot be resolved.

### D2 — S3 layout: canonical bucket-relative keys only

**Decision:** Match the sibling implementation's key layout:

- key `[normalized S3_PATH_PREFIX/]{EXCHANGE}/{SYMBOL}/summary.md`
- exchange and symbol trimmed and upper-cased
- the canonical key is bucket-relative and never starts with `/`
- `ReadSummary` issues exactly one `GetObject` at that key per call
- a definitive not-found returns an error wrapping `ErrNotFound`; any other storage error is returned immediately
- empty exchange/symbol, `/`, or `..` in either segment is rejected before S3 I/O; the prefix is also rejected if it contains `..`

**Why canonical-only:** Treating a 500/timeout as "not found" would silently mask an outage. A single unrooted key is also the only representation that behaves the same on both supported backends.

**History:** `stocker-summarizer` once emitted rooted keys (`/NASDAQ/AAPL/summary.md`), so this reader carried a one-shot rooted fallback to straddle both spellings. The summarizer now writes bucket-relative keys (see `stocker-summarizer/docs/plan-garage-key-prefix.md`), so the fallback has nothing legitimate left to catch:

- **Garage ≤ v2.4.1:** the S3 router derives the key from the URL path and trims a leading `/`. `GET /bucket//NASDAQ/AAPL/summary.md` therefore addresses the same stored key as the canonical request — when the canonical GET 404s the retry cannot succeed and only doubles round trips on every miss.
- **AWS S3:** a leading slash is a literal key byte, so `/NASDAQ/AAPL/summary.md` is a different object. A rooted retry can serve stale legacy data, mask a producer regression, and make behavior backend-dependent.

AWS buckets that really do hold literal rooted objects (inventory them with `aws s3api list-objects-v2 --prefix "/"`) need a one-time copy to unrooted keys; the default Garage deployment needs no migration because old rooted writes were stored trimmed.

### D3 — AWS SDK v2 with static credentials and path-style addressing

**Decision:** Build the client offline with `awsconfig.LoadDefaultConfig`, a static credentials provider, `BaseEndpoint = S3_ENDPOINT`, and `UsePathStyle = true`. No bucket probe at startup. Apply `S3_TIMEOUT_SECONDS` as a per-`ReadSummary` context deadline covering the summary read.

**Why:** Garage is S3-compatible and requires path-style addressing at a custom endpoint. Offline construction keeps startup independent of Garage availability; a fail-fast startup on a transient S3 outage would cause restart loops. The timeout bounds a hung read so the consumer loop cannot stall indefinitely.

### D4 — Three operator-facing audiences, mapped to explicit form fields

**Decision:** `GOTOSOCIAL_AUDIENCE` is `global` (default), `local`, or `direct`:

| Audience | `visibility` | `local_only` | Status prefix |
| --- | --- | --- | --- |
| `global` | `public` | `false` | none |
| `local` | `public` | `true` | none |
| `direct` | `direct` | `false` | `@<recipient> ` |

`GOTOSOCIAL_RECIPIENT` is required only for `direct`. It is normalized in config: trim whitespace, strip one leading `@`, and if the result has no `@domain`, append the host of `GOTOSOCIAL_INSTANCE` (hostname only, no port). A handle with more than one `@` is rejected. The publisher always sends `content_type=text/markdown`.

**Why:** One setting covers the three real operator intents without exposing the raw API. Explicitly sending `local_only=false` (rather than omitting it) makes the global/local distinction unambiguous and testable. Mentioning the recipient at the start is required for a `direct` status to have a recipient.

### D5 — Remove the 500-character truncation and the event formatter

**Decision:** Publish the summary body verbatim (after trimming surrounding whitespace) as `text/markdown`. Delete `internal/messenger/stock_event_formatter.go`, `StockEvent`, `Formatter`, and `truncateAtUTF8Runes`. Keep the `Publisher` interface and the publisher's retry/backoff logic.

**Why:** A summary is up to ~1000 words; truncating at 500 characters would discard most of the product. The target GoToSocial instance has a high status limit. The formatter and `StockEvent` model have no remaining input once Kafka carries a protobuf `Stock`.

**Trade-off:** If a summary exceeds the instance's status limit, GoToSocial returns a non-2xx and the existing retry/drop semantics apply. Chunking/threading is out of scope and listed as a known limitation.

### D6 — `go.sum` is committed and the container build caches modules

**Decision:** Commit `go.sum`, change the Containerfile to `COPY go.mod go.sum ./`, set `ENV GOPRIVATE=git.wheeli.ca`, and run `go mod download` before `COPY . .`, matching `stocker-summarizer/Containerfile`.

**Why:** The module graph is now much larger and includes a private module fetched directly from `git.wheeli.ca`. A committed `go.sum` makes builds reproducible and lets the dependency layer cache independently of source changes. `GOPRIVATE` prevents the Go toolchain from trying to reach the public checksum database for the private module. Credentials are never baked into the image; if the module is private, the builder must supply git credentials at build time.

### D7 — Failure semantics stay skip-and-commit, with level distinction

**Decision:** A decode failure, missing summary, storage error, empty summary, or publish failure logs and commits the offset, then continues. Missing summary logs `Warn`; all other failures log `Error`. The summary body is never logged.

**Why:** A missing summary is an expected race (signal consumed before the object is visible) and should not look like an outage. The existing consumer already treats Kafka as an at-most-once pipeline after the publisher's retries; changing that is out of scope and would require a dead-letter strategy.

### D8 — Live GoToSocial verification is opt-in, local/direct only, and cleans up

**Decision:** Add `internal/messenger/gotosocial_live_test.go`, skipped unless `GOTOSOCIAL_LIVE_TEST=1`. It requires `GOTOSOCIAL_INSTANCE`, `GOTOSOCIAL_USER`, `GOTOSOCIAL_TOKEN`, and `GOTOSOCIAL_RECIPIENT`, and runs only the `local` and `direct` audiences. It never constructs a `global` publisher. The internal `publish` method returns the created status ID so the test can `DELETE` its own statuses.

**Why:** A real instance is the only way to validate `local_only` and `direct` behavior end-to-end. Restricting to non-global audiences guarantees tests never federate. Cleaning up avoids polluting the local timeline and the recipient's inbox. The token is read from the environment and never written to the repository.

### D9 — Keep `GOTOSOCIAL_USER` but document that it is informational

**Decision:** Keep `GOTOSOCIAL_USER` required for compatibility and operator clarity, while authentication remains Bearer-token based.

**Why:** Removing it is a breaking config change unrelated to this feature. It remains in the existing known limitations.

---

## 3. Configuration

`Load()` reads all variables, applies defaults, validates everything, and returns one aggregated error naming each offending variable. `main()` wraps it as `load config: ...` and exits `1`.

### 3.1 Variables

| Variable | Required / default | Secret | Meaning |
| --- | --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | **Required** | no | Comma-separated broker addresses; split on `,`, trimmed, empty entries dropped. At least one non-empty broker is required. |
| `KAFKA_TOPIC` | **Required** | no | Topic carrying protobuf `stockstore.v1.Stock` summary signals. Operationally `stock.summary.available.v1`. |
| `KAFKA_CONSUMER_GROUP` | **Required** | no | Consumer-group ID for partition offset tracking. |
| `GOTOSOCIAL_INSTANCE` | **Required** | no | Absolute `http(s)` base URL, e.g. `https://wheeli.social`. Must include scheme and host. |
| `GOTOSOCIAL_USER` | **Required** | no | Account name. Stored and logged only as metadata; auth is the Bearer token. |
| `GOTOSOCIAL_TOKEN` | **Required** | **yes** | Bearer token for `/api/v1/statuses`. Never logged. |
| `GOTOSOCIAL_AUDIENCE` | optional, default `global` | no | `global` \| `local` \| `direct` (case-insensitive, trimmed). |
| `GOTOSOCIAL_RECIPIENT` | required iff audience=`direct`; ignored otherwise | no | Fediverse handle, e.g. `papa@wheeli.social`. Leading `@` stripped. If no `@domain`, the host of `GOTOSOCIAL_INSTANCE` is appended. |
| `S3_ENDPOINT` | optional, default `http://192.168.1.31:3900` | no | Garage/S3 base endpoint; must be an absolute URL. |
| `S3_REGION` | optional, default `us-east-1` | no | AWS region passed to the SDK. |
| `S3_BUCKET` | **Required** | no | Bucket containing summary objects. |
| `S3_ACCESS_KEY` | **Required** | **yes** | Static S3 access key. Never logged. |
| `S3_SECRET_KEY` | **Required** | **yes** | Static S3 secret key. Never logged. |
| `S3_PATH_PREFIX` | optional, default empty | no | Key prefix before `{EXCHANGE}/{SYMBOL}/summary.md`. Leading/trailing slashes trimmed; empty segments dropped. |
| `S3_TIMEOUT_SECONDS` | optional, default `10` | no | Per-summary-read timeout in seconds; must parse as an integer and be `> 0`. |

### 3.2 Audience form mapping

| Audience | `visibility` | `local_only` | `content_type` | `status` |
| --- | --- | --- | --- | --- |
| `global` | `public` | `false` | `text/markdown` | `<summary>` |
| `local` | `public` | `true` | `text/markdown` | `<summary>` |
| `direct` | `direct` | `false` | `text/markdown` | `@<recipient> <summary>` |

### 3.3 Validation messages

Each message names the variable, for example:

- `KAFKA_TOPIC is required and must be non-empty`
- `KAFKA_BOOTSTRAP_SERVERS must contain at least one non-empty broker`
- `GOTOSOCIAL_INSTANCE must be an absolute URL with scheme and host (got "...")`
- `GOTOSOCIAL_AUDIENCE must be one of global|local|direct (got "...")`
- `GOTOSOCIAL_RECIPIENT is required when GOTOSOCIAL_AUDIENCE=direct`
- `GOTOSOCIAL_RECIPIENT must be a fediverse handle (got "...")`
- `S3_ENDPOINT must be an absolute URL with scheme and host (got "...")`
- `S3_TIMEOUT_SECONDS must be an integer (got "...")`
- `S3_TIMEOUT_SECONDS must be > 0 (got 0)`

Aggregated form: `invalid configuration: <err1>; <err2>`.

---

## 4. Target File Inventory

### 4.1 Add

| Path | Purpose |
| --- | --- |
| `go.sum` | Committed module hashes. |
| `.gitignore` | Ignore local `.env` files while keeping `*.example` files trackable. |
| `internal/summarystore/store.go` | S3-backed summary reader, key builder, `ErrNotFound`. |
| `internal/summarystore/store_test.go` | Fake-S3 tests for keys, single canonical GET, traversal, errors, timeout. |
| `internal/config/config_test.go` | Env validation, defaults, audience, recipient normalization. |
| `internal/messenger/gotosocial_publisher_test.go` | `httptest.Server` tests for form fields, markdown, retry, status ID. |
| `internal/messenger/gotosocial_live_test.go` | Opt-in live `local` / `direct` test with cleanup. |
| `internal/kafka/consumer_test.go` | Protobuf decode and dispatch/skip tests. |
| `deploy/quadlet/.env.podman.example` | Operator template with placeholders; no real secrets. |
| `docs/stock-summary-gotosocial.md` | This document. |

### 4.2 Modify

| Path | Change |
| --- | --- |
| `go.mod` | Add stocker-store, AWS SDK v2, smithy, protobuf. Keep kafka-go. |
| `Containerfile` | Cache `go.mod go.sum`, `GOPRIVATE`, `go mod download` before source copy. |
| `internal/config/config.go` | Add `Audience`, `S3Config`, recipient normalization, new validation. |
| `internal/messenger/gotosocial_publisher.go` | Audience-aware form fields, Markdown content type, internal `publish` returning status ID. |
| `internal/messenger/messenger.go` | Keep only the `Publisher` interface. |
| `internal/kafka/consumer.go` | Protobuf decode, summary read, dispatch via `handleMessage`, `SummaryReader` seam. |
| `cmd/server/main.go` | Construct summary store, audience-aware publisher, new consumer signature, handle `Run` error. |
| `README.md` | Full rewrite of feature/config/input/ops/structure/limitations sections. |
| `AGENTS.md` | New architecture, structure, deps, env vars, test commands. |
| `docs/deployment.md` | Mark superseded configuration/Containerfile statements; link to this plan. |

### 4.3 Delete

| Path | Reason |
| --- | --- |
| `internal/messenger/stock_event_formatter.go` | JSON event model and 500-char truncation are obsolete. |

### 4.4 Unchanged

- `deploy/push.sh`
- `deploy/quadlet/stocker-informer.container`
- `review.md` (historical audit snapshot)

---

## 5. Implementation Tasks

Each task ends in a compiling tree with passing tests. Run `gofmt -w` on touched Go files before committing.

### Task 1: Dependencies, `go.sum`, Containerfile, and secret hygiene

**Depends on:** nothing.

**Files:**
- Modify: `go.mod`
- Create: `go.sum`
- Modify: `Containerfile`
- Create: `.gitignore`

**Expected behaviour:**
- `go.mod` gains the AWS SDK, smithy, and existing kafka-go requirements at the `§Global Constraints` versions and keeps `go 1.25.0`.
- `go.sum` is generated and tracked.
- The Containerfile copies `go.mod go.sum` first, sets `GOPRIVATE=git.wheeli.ca`, runs `go mod download`, then copies the source and builds `./cmd/server`.
- `.gitignore` ignores local env files but not examples.

`stocker-store` and `google.golang.org/protobuf` are added in Task 5, when code first imports them. Adding them here would not survive `go mod tidy` in Task 3 because nothing imports them yet; Task 5 pins them explicitly with `go get`.

**Steps:**

1. Edit the `require` block in `go.mod` to:

   ```go
   require (
       github.com/aws/aws-sdk-go-v2 v1.47.2
       github.com/aws/aws-sdk-go-v2/config v1.33.8
       github.com/aws/aws-sdk-go-v2/credentials v1.20.8
       github.com/aws/aws-sdk-go-v2/service/s3 v1.114.2
       github.com/aws/smithy-go v1.28.4
       github.com/segmentio/kafka-go v0.4.47
   )
   ```

   Keep the existing indirect `klauspost/compress` and `pierrec/lz4` block; `go mod tidy` in Tasks 3 and 5 will expand indirect requirements.

2. Generate sums without pruning the new direct requirements:

   ```bash
   go mod download all
   ```

3. Verify the pinned versions:

   ```bash
   go list -m all | grep -E 'aws-sdk-go-v2|smithy-go|kafka-go'
   ```

   Expected: the versions above, no newer majors.

4. Replace `Containerfile` with:

   ```dockerfile
   # build stage
   FROM docker.io/golang:1.25-bookworm AS build
   WORKDIR /src
   COPY go.mod go.sum ./
   # stocker-informer depends on the private module
   # git.wheeli.ca/brian/stocker-store (fetched direct)
   ENV GOPRIVATE=git.wheeli.ca
   RUN go mod download
   COPY . .
   RUN CGO_ENABLED=0 GOOS=linux go build -o /out/stocker-informer ./cmd/server

   # runtime stage
   FROM gcr.io/distroless/static-debian12
   COPY --from=build /out/stocker-informer /stocker-informer
   USER 65534:65534
   ENTRYPOINT ["/stocker-informer"]
   ```

5. Create `.gitignore`:

   ```gitignore
   # Operator env files may contain secrets; never commit them.
   .env
   .env.*
   !.env.example
   !.env.*.example
   ```

6. Verify ignore rules:

   ```bash
   git check-ignore -v .env.podman
   git check-ignore -v deploy/quadlet/.env.podman.example || echo "example is trackable"
   ```

7. Build and verify modules:

   ```bash
   go build ./...
   go mod verify
   ```

   Expected: exit 0. If `go build` reports missing sums for a transitive package, run `go mod tidy` once the first importing task lands (Task 3), not here.

8. Commit:

   ```bash
   git add go.mod go.sum Containerfile .gitignore
   git commit -m "build: add S3 deps, commit go.sum, cache modules in Containerfile"
   ```

---

### Task 2: Configuration for S3 and GoToSocial audience

**Depends on:** Task 1.

**Files:**
- Modify: `internal/config/config.go`
- Create: `internal/config/config_test.go`

**Interfaces produced:**
```go
type Audience string

const (
    AudienceGlobal Audience = "global"
    AudienceLocal  Audience = "local"
    AudienceDirect Audience = "direct"
)

type S3Config struct {
    Endpoint   string
    Region     string
    Bucket     string
    AccessKey  string
    SecretKey  string
    PathPrefix string
    Timeout    time.Duration
}

type Config struct {
    KafkaBootstrapServers string
    KafkaTopic            string
    KafkaConsumerGroup    string

    GotoSocialInstance  string
    GotoSocialUser      string
    GotoSocialToken     string
    GotoSocialAudience  Audience
    GotoSocialRecipient string

    S3 S3Config
}

func Load() (*Config, error)
```

**Expected behaviour:**
- Required variables: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_CONSUMER_GROUP`, `GOTOSOCIAL_INSTANCE`, `GOTOSOCIAL_USER`, `GOTOSOCIAL_TOKEN`, `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`.
- Defaults: `GOTOSOCIAL_AUDIENCE=global`, `S3_ENDPOINT=http://192.168.1.31:3900`, `S3_REGION=us-east-1`, `S3_PATH_PREFIX=""`, `S3_TIMEOUT_SECONDS=10`.
- `GOTOSOCIAL_INSTANCE` and `S3_ENDPOINT` must parse as absolute URLs with scheme and host.
- Audience is trimmed/lower-cased; anything outside the three values is an error.
- Direct requires a non-empty recipient; a recipient with no `@` gets `@<instance-hostname>` appended; one leading `@` is stripped; more than one `@` is an error. When the audience is not `direct`, the recipient is ignored and stored as empty.
- `S3_PATH_PREFIX` is trimmed of `/`; `S3_TIMEOUT_SECONDS` must parse and be `> 0`.
- Errors are aggregated and name the offending variable; no secret value appears in an error.
- `Config.KafkaServers()` keeps its split/trim/drop-empty behavior; an empty result is now a validation error.

**Steps:**

1. Write `config_test.go` with a `setValidEnv(t)` helper that sets every required variable and explicitly sets optional variables to `""` so the test is independent of the caller's environment. Use `t.Setenv` (no `t.Parallel` in these tests).

   Required test cases:

   | Test | Setup | Assertion |
   | --- | --- | --- |
   | `TestLoadDefaults` | valid env | audience `global`, endpoint `http://192.168.1.31:3900`, region `us-east-1`, prefix ``, timeout `10s`, recipient `""` |
   | `TestLoadMissingRequired` | table, blank one required var at a time (9 cases) | error contains that variable name and `required` |
   | `TestLoadInvalidAudience` | `GOTOSOCIAL_AUDIENCE=friends` | error contains `GOTOSOCIAL_AUDIENCE` and `global|local|direct` |
   | `TestLoadDirectRequiresRecipient` | audience `direct`, recipient empty | error contains `GOTOSOCIAL_RECIPIENT` |
   | `TestLoadRecipientNormalization` | table: `papa@wheeli.social`, `@papa@wheeli.social`, `papa` with `https://wheeli.social`, `papa` with `https://wheeli.social:8443`, `  papa@wheeli.social  ` | expected `papa@wheeli.social` for the first four |
   | `TestLoadInvalidRecipient` | `a@b@c` | error contains `GOTOSOCIAL_RECIPIENT` |
   | `TestLoadInvalidAudienceURL` | `GOTOSOCIAL_INSTANCE=not-a-url` | error contains `GOTOSOCIAL_INSTANCE` and `absolute URL` |
   | `TestLoadInvalidS3Endpoint` | `S3_ENDPOINT=garage:3900` | error contains `S3_ENDPOINT` |
   | `TestLoadS3Timeout` | `abc`, `0`, `-1` | errors contain `S3_TIMEOUT_SECONDS` |
   | `TestLoadBrokers` | `"a:1, b:2,,"` → `["a:1","b:2"]`; `","` → error | as stated |
   | `TestLoadAggregatesErrors` | blank `KAFKA_TOPIC` and `S3_BUCKET` together | one error contains both names |

2. Run to confirm failure:

   ```bash
   go test ./internal/config -v
   ```

   Expected: compile failure (`undefined: Audience`, `S3Config`, etc.).

3. Implement `config.go`:
   - Add `defaultS3Endpoint = "http://192.168.1.31:3900"`.
   - `Load()` builds `errs []string`, appends messages via `add`, and returns `fmt.Errorf("invalid configuration: %s", strings.Join(errs, "; "))` when non-empty.
   - Use `getEnv` for optional defaults and `strings.TrimSpace` for all values.
   - Parse the instance URL once with `url.Parse`; use `u.Hostname()` for recipient expansion and `u.IsAbs()`/`u.Host != ""` for validation.
   - Implement `normalizeRecipient(raw, instance string) (string, error)` as described. Return `"", nil` for an empty raw value.
   - Implement `parseTimeout` using `strconv.Atoi`.
   - Implement `validateURL(name, raw string) error`.
   - Keep `getEnv` and `KafkaServers`.

4. Run:

   ```bash
   gofmt -w internal/config/config.go internal/config/config_test.go
   go test ./internal/config -v
   go vet ./internal/config
   ```

   Expected: all pass; no test makes a network call.

5. Commit:

   ```bash
   git add internal/config/config.go internal/config/config_test.go
   git commit -m "feat(config): add S3 settings and GoToSocial audience validation"
   ```

---

### Task 3: Garage/S3 summary store

**Depends on:** Task 2.

**Files:**
- Create: `internal/summarystore/store.go`
- Create: `internal/summarystore/store_test.go`

**Interfaces produced:**
```go
var ErrNotFound = errors.New("summarystore: summary not found")

type Store interface {
    ReadSummary(ctx context.Context, exchange, symbol string) ([]byte, error)
    Close() error
}

type API interface {
    GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

func New(cfg *config.S3Config) (*Client, error)
func NewWithAPI(cfg *config.S3Config, api API) (*Client, error)
func BuildKey(prefix, exchange, symbol string) (string, error)
func (c *Client) ReadSummary(ctx context.Context, exchange, symbol string) ([]byte, error)
func (c *Client) Close() error
```

**Expected behaviour:**
- `New` loads AWS config with the region (fallback `us-east-1` if empty), static credentials, `BaseEndpoint` trimmed of trailing `/`, and `UsePathStyle = true`. It makes no network request.
- `BuildKey` trims, upper-cases, normalizes the prefix, rejects empty segments, `/` in a segment, `..` in a segment or prefix, and returns a key with no leading slash.
- `ReadSummary`:
  1. Build the key.
  2. Apply `cfg.Timeout` as a context deadline when `> 0`.
  3. `GET` the canonical key — the only GET issued.
  4. On success, return the bytes.
  5. On a definitive not-found, return an error wrapping `ErrNotFound`.
  6. On any non-not-found error, return the wrapped error.
- Bodies are always closed; a nil response/body is an error.
- `isNotFound` recognizes `*types.NoSuchKey`, `*types.NotFound`, smithy error codes `NoSuchKey`/`NotFound`, and `HTTPStatusCode() == 404`.
- Context deadline/cancellation is preferred in the wrapped error.

**Steps:**

1. Write `store_test.go` with a scripted `fakeAPI` that records every `GetObjectInput` and returns a keyed response. Copy the fake pattern from `stocker-summary-evaluator/internal/summarystore/store_test.go`; no network.

   Required test cases:

   | Test | Assertion |
   | --- | --- |
   | `TestBuildKey` table | canonical, trim/upper-case, prefix normalization (`corp/`, `/a//b/`), empty exchange, empty symbol, `AA/PL`, `A..`, prefix `..` |
   | `TestReadSummaryCanonical` | one call, bucket and key `NASDAQ/AAPL/summary.md`, body returned, body closed |
   | `TestReadSummaryNotFound` | one call at `NASDAQ/AAPL/summary.md`, `errors.Is(err, ErrNotFound)` |
   | `TestReadSummaryKeysAreBucketRelativeUnderEveryPrefix` | empty, rooted, and doubled prefix spellings each record one bucket-relative key |
   | `TestReadSummaryBackendKeySemantics` | Garage trims a leading `/` (same object); an AWS-style verbatim key does not reach a rooted-only object |
   | `TestReadSummaryStorageErrorNoFallback` | canonical 500, exactly one call, error not `ErrNotFound` |
   | `TestReadSummaryReadError` | body read fails, error returned |
   | `TestReadSummaryNilBody` | nil body, error returned |
   | `TestReadSummaryTimeout` | fake API waits on `ctx.Done()`, timeout `10ms`, error mentions context/deadline |
   | `TestClose` | returns nil |

2. Run to confirm failure:

   ```bash
   go test ./internal/summarystore -v
   ```

   Expected: compile failure (`undefined: BuildKey`, etc.).

3. Implement `store.go` by adapting the sibling implementation in `/home/brian/workspace/stocker-summary-evaluator/internal/summarystore/store.go`:
   - Change the package doc to `stocker-informer`.
   - Import `github.com/example/stocker-informer/internal/config`.
   - Keep the same error strings and behavior; do not add retries or bucket checks.

4. Tidy and run:

   ```bash
   go mod tidy
   gofmt -w internal/summarystore/store.go internal/summarystore/store_test.go
   go test ./internal/summarystore -v
   go vet ./internal/summarystore
   ```

   Expected: all pass. `go mod tidy` now adds the AWS transitive requirements and package sums to `go.sum`.

5. Commit (include `go.mod`/`go.sum` if tidy changed them):

   ```bash
   git add internal/summarystore go.mod go.sum
   git commit -m "feat(summarystore): read summary markdown from Garage/S3"
   ```

---

### Task 4: Audience-aware GoToSocial publisher

**Depends on:** Task 2.

**Files:**
- Modify: `internal/messenger/gotosocial_publisher.go`
- Create: `internal/messenger/gotosocial_publisher_test.go`
- Modify: `cmd/server/main.go` (constructor arguments only)

**Interfaces produced:**
```go
func NewGoToSocialPublisher(instance, username, token string, audience config.Audience, recipient string, log *slog.Logger) *GoToSocialPublisher

// Publish keeps the messenger.Publisher contract.
func (p *GoToSocialPublisher) Publish(ctx context.Context, text string) error

// publish returns the created status ID so the live test can delete it.
func (p *GoToSocialPublisher) publish(ctx context.Context, text string) (statusID string, err error)
```

**Expected behaviour:**
- Form body always contains `content_type=text/markdown` and `status`.
- Audience mapping is exactly §3.2.
- For `direct`, `status` is `"@" + recipient + " " + text`; the recipient is already normalized by config.
- For `global`, do not add a mention or modify the text.
- Request headers remain `Content-Type: application/x-www-form-urlencoded` and `Authorization: Bearer <token>`.
- Retry/backoff is unchanged: up to 3 attempts, 500 ms base doubling, capped at 30 s, 20 s HTTP timeout.
- On 2xx, best-effort decode `{"id":"..."}` and return it. A decode failure does not fail the publish; the ID is simply empty.
- Non-2xx responses drain the body and return an error as today.
- Close each response body at the end of its attempt instead of `defer`-ing inside the retry loop.
- Status text and token are never logged.

**Steps:**

1. Write `gotosocial_publisher_test.go` using `httptest.NewServer`:
   - Handler records `r.PostForm`, `Authorization`, and `Content-Type`, then responds `201` with `{"id":"01TEST"}`.
   - `newTestPublisher(t, server.URL, audience, recipient)` helper.

   Required test cases:

   | Test | Assertion |
   | --- | --- |
   | `TestPublishAudienceForm` table: global / local / direct | `status`, `visibility`, `local_only`, `content_type` exactly per §3.2; direct status starts `@papa@wheeli.social `; global/local status equals input |
   | `TestPublishMarkdownNotTruncated` | a >500-character Markdown body with `#`, `*`, links is sent byte-for-byte (global) |
   | `TestPublishRetriesAfterServerError` | handler returns 500 on call 1, 201 on call 2; exactly 2 requests; `Publish` returns nil |
   | `TestPublishReturnsStatusID` | internal `publish` returns `01TEST` |
   | `TestPublishAcceptsTrailingSlashInstance` | constructor with `server.URL + "/"`, POST still hits `/api/v1/statuses` |
   | `TestPublishSendsBearerToken` | `Authorization` equals `Bearer test-token` |

   Do not assert log output; do not use a real network.

2. Run to confirm failure:

   ```bash
   go test ./internal/messenger -run 'TestPublish' -v
   ```

   Expected: compile failure (new constructor arguments).

3. Implement the publisher:
   - Add `audience config.Audience` and `recipient string` fields.
   - Split `Publish` into a thin wrapper over `publish`.
   - Build the form in one helper, e.g. `func (p *GoToSocialPublisher) form(text string) url.Values`, so the audience mapping is in one place.
   - Parse the success response ID with `json.NewDecoder(io.LimitReader(resp.Body, 64<<10))`.
   - Keep the existing retry loop and error text.

4. Update the constructor call in `cmd/server/main.go` (leave the formatter/consumer wiring untouched in this task):

   ```go
   publisher := messenger.NewGoToSocialPublisher(
       cfg.GotoSocialInstance,
       cfg.GotoSocialUser,
       cfg.GotoSocialToken,
       cfg.GotoSocialAudience,
       cfg.GotoSocialRecipient,
       log,
   )
   ```

5. Run:

   ```bash
   gofmt -w internal/messenger/gotosocial_publisher.go internal/messenger/gotosocial_publisher_test.go cmd/server/main.go
   go test ./internal/messenger -run 'TestPublish' -v
   go build ./...
   go vet ./...
   ```

   Expected: all pass; `cmd/server` still compiles with the old formatter/consumer.

6. Commit:

   ```bash
   git add internal/messenger/gotosocial_publisher.go internal/messenger/gotosocial_publisher_test.go cmd/server/main.go
   git commit -m "feat(messenger): publish markdown with configurable GoToSocial audience"
   ```

---

### Task 5: Protobuf consumer, summary dispatch, main wiring, remove dead code

**Depends on:** Tasks 1, 2, 3, 4.

**Files:**
- Modify: `internal/kafka/consumer.go`
- Create: `internal/kafka/consumer_test.go`
- Modify: `internal/messenger/messenger.go`
- Delete: `internal/messenger/stock_event_formatter.go`
- Modify: `cmd/server/main.go`

**Interfaces produced:**
```go
type SummaryReader interface {
    ReadSummary(ctx context.Context, exchange, symbol string) ([]byte, error)
}

func New(brokers []string, topic, consumerGroup string, summaries SummaryReader, publisher messenger.Publisher, log *slog.Logger) *Consumer

type stockIdentity struct {
    Symbol   string
    Exchange string
}

func decodeStock(data []byte) (stockIdentity, error)

func (c *Consumer) handleMessage(ctx context.Context, msg kafka.Message, commit func(kafka.Message))
```

**Expected behaviour:**
- `decodeStock` uses `proto.Unmarshal(data, &stockstorev1.Stock{})`.
- Symbol and exchange are trimmed; both must be non-empty. Errors name the missing field.
- `handleMessage`:
  1. Decode. On error: log `Error`, commit, return.
  2. `ReadSummary(ctx, exchange, symbol)`. On `errors.Is(err, summarystore.ErrNotFound)`: log `Warn` (no body), commit, return. On any other error: log `Error`, commit, return.
  3. `strings.TrimSpace(string(summary))`. If empty: log `Warn`, commit, return.
  4. `publisher.Publish(ctx, text)`. On error: log `Error`, commit, return.
  5. Commit, then log `Info` with `symbol`, `exchange`, and `offset`.
- `Run` keeps the reader configuration and backoff loop; it replaces inline dispatch with:
  ```go
  c.handleMessage(ctx, msg, func(m kafka.Message) {
      c.commitWithRetries(ctx, reader, m)
  })
  ```
- Log messages never include the summary body, token, or S3 credentials.
- `messenger.go` keeps only:
  ```go
  package messenger

  import "context"

  type Publisher interface {
      Publish(ctx context.Context, text string) error
  }
  ```
- `stock_event_formatter.go` is deleted; no `StockEvent`, `Formatter`, `truncateAtUTF8Runes`, or `maxGotosocialTextLength` remains.
- `main` constructs the store and new consumer, defers `store.Close()`, handles `consumer.Run` errors, and logs non-secret configuration metadata.

**Steps:**

1. Write `consumer_test.go`:
   - `fakeSummaries` records `exchange`/`symbol`, returns a configurable body/error.
   - `fakePublisher` records texts, returns a configurable error.
   - `recordedCommits` counts commit calls.
   - Construct the consumer with `slog.New(slog.NewJSONHandler(io.Discard, nil))`.
   - Use `stockstorev1.Stock` + `proto.Marshal` to build values.

   Required test cases:

   | Test | Assertion |
   | --- | --- |
   | `TestDecodeStock` | `aapl`/`nasdaq` decoded as `aapl`/`nasdaq`; scores ignored |
   | `TestDecodeStockTrims` | `" AAPL "` / `" NASDAQ "` → `AAPL` / `NASDAQ` |
   | `TestDecodeStockRejectsEmptySymbol` | error contains `symbol` |
   | `TestDecodeStockRejectsEmptyExchange` | error contains `exchange` |
   | `TestDecodeStockRejectsMalformed` | random bytes error |
   | `TestHandleMessagePublishesSummary` | store receives `NASDAQ`/`AAPL`, publisher gets exact Markdown, one commit |
   | `TestHandleMessageSummaryNotFoundSkips` | store returns `ErrNotFound`, zero publishes, one commit |
   | `TestHandleMessageStorageErrorSkips` | store returns `errors.New("boom")`, zero publishes, one commit |
   | `TestHandleMessageEmptySummarySkips` | store returns whitespace, zero publishes, one commit |
   | `TestHandleMessagePublishErrorCommits` | publisher error, one commit |
   | `TestHandleMessageDecodeErrorCommits` | malformed value, zero store calls, one commit |
   | `TestHandleMessagePassesTrimmedIdentity` | store receives trimmed values |

2. Run to confirm failure:

   ```bash
   go test ./internal/kafka -v
   ```

   Expected: compile failure against the old `Formatter` signature.

3. Implement `consumer.go` as described. Import:
   ```go
   stockstorev1 "git.wheeli.ca/brian/stocker-store/proto/v1"
   "google.golang.org/protobuf/proto"
   "errors"
   "strings"
   ```

4. Update `main.go`:
   ```go
   store, err := summarystore.New(&cfg.S3)
   if err != nil {
       return fmt.Errorf("create summary store: %w", err)
   }
   defer func() { _ = store.Close() }()

   publisher := messenger.NewGoToSocialPublisher(
       cfg.GotoSocialInstance, cfg.GotoSocialUser, cfg.GotoSocialToken,
       cfg.GotoSocialAudience, cfg.GotoSocialRecipient, log,
   )

   consumer := kafka.New(
       cfg.KafkaServers(), cfg.KafkaTopic, cfg.KafkaConsumerGroup,
       store, publisher, log,
   )
   ```
   Remove the `formatter` variable and the `StockEventFormatter` construction. Log:
   ```go
   log.Info("configuration loaded",
       "kafka_topic", cfg.KafkaTopic,
       "s3_endpoint", cfg.S3.Endpoint,
       "s3_bucket", cfg.S3.Bucket,
       "audience", cfg.GotoSocialAudience,
   )
   ```
   Return `fmt.Errorf("run consumer: %w", err)` if `consumer.Run(ctx)` fails. Add `defer stop()` for the signal context.

5. Delete the dead formatter and strip `messenger.go`:
   ```bash
   git rm internal/messenger/stock_event_formatter.go
   ```

6. Add the protobuf dependencies at their pinned versions, then tidy:

   ```bash
   go get git.wheeli.ca/brian/stocker-store@v0.0.0-20261010025329-1a26328c2930
   go get google.golang.org/protobuf@v1.36.12
   go mod tidy
   ```

7. Verify no dead references and run everything:
   ```bash
   grep -rn 'StockEvent\|Formatter\|truncateAtUTF8Runes\|maxGotosocialTextLength' --include='*.go' . || echo "no dead references"
   gofmt -w internal/kafka/consumer.go internal/kafka/consumer_test.go internal/messenger/messenger.go cmd/server/main.go
   go build ./...
   go vet ./...
   go test ./...
   ```

   Expected: no matches, all pass, `go.mod`/`go.sum` consistent. Include them in the commit if tidy changed them.

8. Commit:

   ```bash
   git add internal/kafka internal/messenger cmd/server go.mod go.sum
   git commit -m "feat(kafka): consume Stock protobuf and publish summaries; remove formatter"
   ```

---

### Task 6: Documentation and operator template

**Depends on:** Tasks 1–5.

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `docs/deployment.md`
- Create: `deploy/quadlet/.env.podman.example`

**Expected behaviour:**

#### README.md

Rewrite these sections so they describe the actual system:

- **Title/intro:** protobuf stock summaries from Kafka → Garage/S3 `summary.md` → GoToSocial Markdown status.
- **Key features:** protobuf consumer-group subscription; single canonical S3 summary read; full 1000-word summaries (no 500-char truncation); `global`/`local`/`direct` audience; Markdown content type; publish retries; JSON logs; `/health`; distroless image; single rootless quadlet; graceful shutdown.
- **At a glance:** Go 1.25, module, dependencies (`kafka-go`, AWS SDK v2 S3, `stocker-store/proto/v1`, protobuf), configuration count (9 required + 6 optional), build/publish, container base, registry image, logging, health, exit codes.
- **Quick start:** `go build`, then export Kafka + S3 + GoToSocial values. Remove the `go mod tidy`/`go.sum` caveat.
- **Prerequisites:** Kafka, a Garage/S3 bucket containing summaries, GoToSocial, podman for quadlets.
- **Installation/Building:** `go.sum` is committed; the Containerfile caches `go.mod go.sum`, sets `GOPRIVATE=git.wheeli.ca`, and runs `go mod download`.
- **Configuration:** point at the table below; state that invalid config aborts with `load config: invalid configuration: ...` and exit `1`.
- **Environment Variables:** full table with required/default, secret, meaning for all 15 variables. Call out `S3_ENDPOINT` default `http://192.168.1.31:3900`, `S3_TIMEOUT_SECONDS` must be `> 0`, and `GOTOSOCIAL_RECIPIENT` semantics.
- **Kafka input schema:**
  - Value is a protobuf `stockstore.v1.Stock` from `git.wheeli.ca/brian/stocker-store/proto/v1`.
  - Show the relevant schema:
    ```proto
    message Stock {
      string symbol = 1;
      string exchange = 2;
      repeated ScoreEntry scores = 3;
    }
    ```
  - Only `symbol` and `exchange` are used; both must be non-empty; `scores`, key, and headers are ignored.
  - Topic: `stock.summary.available.v1`; state the producer is `stocker-summarizer`.
  - Decode failures are logged and the offset is committed.
- **Summary source:** path `[S3_PATH_PREFIX/]{EXCHANGE}/{SYMBOL}/summary.md`; uppercase/trim; canonical bucket-relative key is the only key read (one GET, a miss returns `ErrNotFound`); other storage errors are returned immediately; missing summary is a warning and skip; summary published verbatim as `text/markdown`; no truncation; no extra title or header is added to the summary. Include a short sample `summary.md` and the resulting status.
- **Audience configuration:** mapping table from §3.2 and a direct-recipient normalization example (`papa` + `https://wheeli.social` → `papa@wheeli.social`).
- **Running the service / Operations:** update consumer-behaviour and publisher-behaviour subsections:
  - Consumer decodes protobuf, reads S3, publishes summary.
  - Publisher form body includes `visibility`, `local_only`, `content_type`.
  - Offset semantics include decode failure, missing summary, storage error, empty summary, publish failure.
  - Representative log lines: `summary published on GoToSocial` (symbol, exchange, offset), `summary not found`, `read summary failed`, `decode stock failed`.
- **Repository structure:** include `go.sum`, `.gitignore`, `internal/summarystore/`, no `stock_event_formatter.go`, and this plan.
- **Known limitations:** replace obsolete bullets:
  - module path placeholder
  - optimistic `/health` readiness and unpublished `:8080`
  - failed publish committed/dropped after 3 attempts
  - `GOTOSOCIAL_USER` informational
  - no log rotation
  - no chunking/threading: an over-limit summary fails and is dropped
  - S3 storage errors are not retried; the message is logged, committed, and skipped
  - container build needs network and git access to `git.wheeli.ca` (no credentials baked in)

#### AGENTS.md

- Description: consumes protobuf `Stock` signals, reads `summary.md` from S3, publishes Markdown to GoToSocial.
- Structure: add `internal/summarystore/`; remove formatter; keep `internal/messenger/` as publisher only.
- Architecture: config → S3 store + GTS publisher → Kafka consumer → decode → read → publish.
- Dependencies: kafka-go, AWS SDK v2 S3, stocker-store proto, protobuf.
- Env vars: all 15, with required/default notes.
- Tests: `go test ./...`, `go vet ./...`, optional `GOTOSOCIAL_LIVE_TEST=1`.
- Leave the existing **Tools** section (credentials pointer) unchanged.

#### docs/deployment.md

Do not rewrite the historical task list. Add a clearly marked update near the top:

```markdown
> **Configuration update (2026-10-22):** the service now consumes protobuf
> `stockstore.v1.Stock`, reads `summary.md` from Garage/S3, and publishes
> Markdown with a configurable GoToSocial audience. Environment variables,
> dependencies, and the Containerfile changed; see
> `docs/stock-summary-gotosocial.md` and the README. Deployment mechanics
> (single rootless quadlet, `deploy/push.sh`, image name,
> `~/.config/stocker-informer/.env.podman`) are unchanged.
```

Also update the "six required env vars" global-constraint line and the "Do not modify Containerfile" line to point at the new plan instead of restating stale facts. Leave the rest of the document intact.

#### deploy/quadlet/.env.podman.example

Create a placeholder-only template:

```dotenv
# stocker-informer — operator environment template.
# Copy to ~/.config/stocker-informer/.env.podman and chmod 600.
# NEVER commit real credentials.

# ── Kafka ───────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS=192.168.1.71:9092,192.168.1.75:9092
KAFKA_TOPIC=stock.summary.available.v1
KAFKA_CONSUMER_GROUP=stocker-informer

# ── Garage / S3 ─────────────────────────────────────────────────────────────
S3_ENDPOINT=http://192.168.1.31:3900
S3_REGION=us-east-1
S3_BUCKET=REPLACE_ME
S3_ACCESS_KEY=REPLACE_ME
S3_SECRET_KEY=REPLACE_ME
S3_PATH_PREFIX=
S3_TIMEOUT_SECONDS=10

# ── GoToSocial ──────────────────────────────────────────────────────────────
GOTOSOCIAL_INSTANCE=https://wheeli.social
GOTOSOCIAL_USER=bot
GOTOSOCIAL_TOKEN=REPLACE_ME
# global | local | direct
GOTOSOCIAL_AUDIENCE=global
# Required only for direct; full handle or just the username.
# GOTOSOCIAL_RECIPIENT=papa@wheeli.social
```

**Steps:**

1. Apply the README, AGENTS, and deployment-doc edits.
2. Add the example env file.
3. Sweep for stale text:

   ```bash
   grep -nE 'JSON stock|StockEvent|stock event|500-character|500 chars|stock-events|go.sum is not committed|formatter' README.md AGENTS.md
   grep -n 'stock.summary.available.v1' README.md
   grep -nE 'S3_BUCKET|S3_TIMEOUT_SECONDS|GOTOSOCIAL_AUDIENCE|GOTOSOCIAL_RECIPIENT' README.md AGENTS.md
   grep -n 'REPLACE_ME' deploy/quadlet/.env.podman.example
   ```

   Expected: no stale JSON/500-char/formatter references in README or AGENTS; topic and all new variables present; the example contains only placeholders.

4. Confirm no secret-shaped values were added:

   ```bash
   grep -rnE '[A-Z0-9]{40,}' deploy/quadlet/.env.podman.example README.md AGENTS.md docs/stock-summary-gotosocial.md || echo "no secret-shaped values"
   ```

5. Commit:

   ```bash
   git add README.md AGENTS.md docs/deployment.md deploy/quadlet/.env.podman.example
   git commit -m "docs: document stock summary pipeline and new configuration"
   ```

---

### Task 7: Opt-in live GoToSocial test and final verification gate

**Depends on:** Tasks 1–6.

**Files:**
- Create: `internal/messenger/gotosocial_live_test.go`

**Expected behaviour:**
- Normal test file (no build tag); skips unless `GOTOSOCIAL_LIVE_TEST=1`.
- Skips with a message naming missing env vars if any of `GOTOSOCIAL_INSTANCE`, `GOTOSOCIAL_USER`, `GOTOSOCIAL_TOKEN`, `GOTOSOCIAL_RECIPIENT` is empty.
- Runs exactly two subtests: `local` (`config.AudienceLocal`) and `direct` (`config.AudienceDirect`). No `global` publisher is ever constructed.
- Each subtest publishes unique Markdown text:
  ```go
  text := fmt.Sprintf("**stocker-informer live test** `%s` — safe to delete", time.Now().UTC().Format(time.RFC3339))
  ```
- Uses `p.publish(ctx, text)` to get the status ID, asserts it is non-empty.
- Fetches `GET /api/v1/statuses/{id}` with the Bearer token and asserts:
  - `local`: `visibility == "public"` and `local_only == true` (GoToSocial omits `local_only` unless it is true).
  - `direct`: `visibility == "direct"` and the `mentions` array contains the recipient's username (split the normalized recipient on `@`; do not match rendered HTML).
- `t.Cleanup` sends `DELETE /api/v1/statuses/{id}`; a cleanup failure is logged with `t.Logf`, not treated as a test failure.
- Uses a 20-second HTTP client; no token is printed on failure.

**Steps:**

1. Write the live test as described. Use `config.Audience` values, not string literals, for the subtests.
2. Run the default suite to confirm the live test skips:

   ```bash
   go test ./internal/messenger -run 'TestGoToSocialLive' -v
   ```

   Expected: `--- SKIP` and no network request.

3. Run the full gate:

   ```bash
   gofmt -l .            # expected: no output
   go build ./...
   go vet ./...
   go test ./...
   go test ./... -race
   go mod verify
   ```

4. Build the container from a clean cache and verify the dependency layer works:

   ```bash
   podman build --no-cache -t stocker-informer:verify .
   # docker build --no-cache -t stocker-informer:verify .  # if podman is unavailable
   ```

5. Verify fail-fast config in the container (no env supplied):

   ```bash
   podman run --rm stocker-informer:verify
   ```

   Expected: exit code `1` and a JSON log containing `load config: invalid configuration:` and the name of a missing variable. It must not reach the Kafka connection.

6. Sweep the repository for stale symbols and secrets:

   ```bash
   grep -rn 'StockEvent\|StockEventFormatter\|truncateAtUTF8Runes\|maxGotosocialTextLength' --include='*.go' . || echo "no dead code"
   grep -rn '500-character\|500 chars\|JSON stock' README.md AGENTS.md || echo "no stale docs"
   git status --porcelain
   ```

7. Commit:

   ```bash
   git add internal/messenger/gotosocial_live_test.go
   git commit -m "test(messenger): add opt-in live GoToSocial audience test"
   ```

---

## 6. How to Verify

### 6.1 Unit and static checks

```bash
gofmt -l .                 # no output
go build ./...
go vet ./...
go test ./...
go test ./... -race
go mod verify
```

All must pass. No unit test may touch the network: the summary store uses `NewWithAPI`, the publisher uses `httptest.Server`, and the consumer uses fakes.

### 6.2 Container build

```bash
podman build --no-cache -t stocker-informer:verify .
podman run --rm stocker-informer:verify
```

The build must succeed with only `go.mod`/`go.sum` present in the dependency layer. The run must exit `1` with a config error naming a missing variable and must not log any secret.

### 6.3 Live GoToSocial test (non-global only)

Export the real values in your shell. The token is provided out-of-band; do not paste it into any file:

```bash
export GOTOSOCIAL_LIVE_TEST=1
export GOTOSOCIAL_INSTANCE=https://wheeli.social
export GOTOSOCIAL_USER=bot
export GOTOSOCIAL_TOKEN='<token from the operator>'
export GOTOSOCIAL_RECIPIENT=papa@wheeli.social

go test ./internal/messenger -run 'TestGoToSocialLive' -v
```

Expected:

- `local` subtest: status ID returned, `visibility=public`, `local_only=true`.
- `direct` subtest: status ID returned, `visibility=direct`, `mentions` includes `papa`.
- Both statuses are deleted in cleanup.
- No `global` post is created.

If the live run is skipped, `GOTOSOCIAL_LIVE_TEST` is not exactly `1` or one of the four variables is empty.

### 6.4 Manual end-to-end check (Kafka + S3 + GoToSocial)

1. Confirm the summary object exists:

   ```bash
   aws --endpoint-url "$S3_ENDPOINT" s3 ls "s3://$S3_BUCKET/NASDAQ/AAPL/summary.md"
   ```

2. Produce a protobuf signal. Using the `stocker-store` proto:

   ```bash
   printf 'symbol: "AAPL"\nexchange: "NASDAQ"\n' | \
     protoc -I /home/brian/workspace/stocker-store -I /usr/include \
       --encode=stockstore.v1.Stock \
       /home/brian/workspace/stocker-store/proto/v1/stock_store.proto | \
     kcat -P -b "$KAFKA_BOOTSTRAP_SERVERS" -t stock.summary.available.v1
   ```

3. Watch the daemon logs for:

   ```text
   configuration loaded
   kafka consumer started
   summary published on GoToSocial   symbol=AAPL exchange=NASDAQ offset=<n>
   ```

4. Confirm the post on the instance matches the summary Markdown and the configured audience. For `direct`, confirm the mention is at the start.

5. Negative checks:
   - Produce a `Stock{symbol:"", exchange:"NASDAQ"}` → `decode stock failed`, offset committed.
   - Produce a stock with no S3 object → `summary not found`, offset committed, no post.
   - Set an invalid `GOTOSOCIAL_AUDIENCE` → startup exits `1` naming the variable.

### 6.5 Documentation checks

```bash
grep -n 'stock.summary.available.v1' README.md
grep -nE 'S3_BUCKET|S3_ACCESS_KEY|S3_SECRET_KEY|S3_TIMEOUT_SECONDS|GOTOSOCIAL_AUDIENCE|GOTOSOCIAL_RECIPIENT' README.md AGENTS.md
grep -rn '500-character\|500 chars\|StockEvent\|JSON stock' README.md AGENTS.md docs/deployment.md || echo "no stale docs"
```

`review.md` is a historical audit and is excluded from stale-reference sweeps.

---

## 7. Secrets Policy

- Never commit `GOTOSOCIAL_TOKEN`, `S3_ACCESS_KEY`, or `S3_SECRET_KEY` values.
- The live token supplied for verification is used only as an exported shell variable and must not appear in tests, docs, commit messages, or logs.
- `deploy/quadlet/.env.podman.example` contains only `REPLACE_ME` placeholders.
- `.gitignore` ignores `.env` and `.env.*` while allowing `*.example`.
- Startup logs may include `s3_endpoint`, `s3_bucket`, `kafka_topic`, and `audience`; they must not include credentials or status bodies.
