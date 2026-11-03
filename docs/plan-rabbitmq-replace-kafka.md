# Replace the Legacy Message Broker with RabbitMQ — Design and Implementation Plan

**Status:** proposed
**Scope:** all production code, tests, configuration, deploy assets, and documentation in `github.com/example/stocker-informer`
**Migration artifact:** this file is temporary. Its filename and body contain the retired broker's name. Step 7 removes it after the durable behaviour has been folded into `README.md`, `AGENTS.md`, and `docs/deployment.md`. Git history preserves the plan.

---

## 1. Context and goal

`stocker-informer` currently consumes `stockstore.v1.Stock` protobuf signals from a Kafka topic through `github.com/segmentio/kafka-go`. The rest of the stocker fleet (`stocker-summarizer`, `stocker-news`) already publishes and consumes over RabbitMQ with `github.com/rabbitmq/amqp091-go v1.16.0`. Running two broker technologies costs an extra broker, an extra client dependency, an extra failure mode, and an extra runbook.

The product owner has asked for RabbitMQ to replace Kafka **entirely**. This is a transport swap, not a pipeline redesign: summary reads from Garage/S3, Cora-9 rewriting through Ollama, GoToSocial publication, audiences, `/health`, and the existing failure fallbacks stay exactly as they are.

### 1.1 Acceptance criterion

After the migration is implemented, this command must produce **no output**:

```bash
grep -rin kafka . --exclude-dir=.git
```

The criterion is case-insensitive and applies to:

- Go source and tests,
- `go.mod`, `go.sum`, and module references,
- Markdown, comments, and shell snippets,
- `deploy/` assets and environment templates,
- **filenames and directory names** (`internal/kafka/` must not exist; no tracked path may contain `kafka` in any case).

Because this plan's own filename contains the retired name, Step 7 deletes this plan. The current operator and architecture documentation lives in `README.md`, `AGENTS.md`, and `docs/deployment.md`.

### 1.2 Non-goals

- No change to `internal/summarystore`, `internal/rewriter`, or `internal/messenger` interfaces or behaviour.
- No change to the GoToSocial payload, audience mapping, or publish retries.
- No change to `/health` response bodies, status codes, or stickiness.
- No exactly-once delivery guarantee. The current pipeline is at-most-once after publication and remains so; see §2.3.
- No RabbitMQ producer. This service only consumes (plus passive topology checks in the readiness probe).

---

## 2. Design

### 2.1 Dependency

Add one direct dependency:

```
github.com/rabbitmq/amqp091-go v1.16.0
```

`go mod tidy` after the legacy consumer is deleted must also remove the broker-specific transitive modules `github.com/klauspost/compress` and `github.com/pierrec/lz4/v4` (and the legacy client itself). `amqp091-go` has no non-standard-library dependency of its own.

### 2.2 Topology contract

The consumer owns the input exchange, queue, and binding, and declares them on every (re)connect. This is the same contract as `stocker-summarizer` and `stocker-news`.

| Element | Value | Declaration flags |
| --- | --- | --- |
| Exchange | `RABBITMQ_INPUT_EXCHANGE`, default `stock.summary` | type `RABBITMQ_INPUT_EXCHANGE_TYPE` (default `topic`), `durable=true`, `autoDelete=false`, `internal=false`, `noWait=false`, `args=nil` |
| Queue | `RABBITMQ_QUEUE`, default `stocker-informer` | `durable=true`, `autoDelete=false`, `exclusive=false`, `noWait=false`, `args=nil` |
| Binding | queue ← `RABBITMQ_INPUT_ROUTING_KEY` → exchange | default routing key `stock.summary.available.v1` |
| QoS | `RABBITMQ_PREFETCH`, default `1` | `Qos(prefetch, 0, false)` (per-consumer, not global) |
| Consumer | tag `stocker-informer-<pid>` | `Consume(queue, tag, autoAck=false, exclusive=false, noLocal=false, noWait=false, nil)` |

Message body is `proto.Marshal(stockstore.v1.Stock)`. Only `Symbol` and `Exchange` are used; the same trimming and non-empty validation as today applies. The exchange name `stock.summary` and routing key `stock.summary.available.v1` match what `stocker-summarizer` publishes.

The queue is durable and non-exclusive. Multiple replicas may therefore compete on the same queue without losing messages; `RABBITMQ_PREFETCH=1` and the synchronous pipeline still bound each replica to one in-flight message.

### 2.3 Delivery and acknowledgement semantics

The legacy consumer committed every outcome: decode failure, missing summary, empty summary, rewrite fallback, publish success, and publish failure. **Preserve that: ack every completed outcome.** A poison message or a permanently failing stock must not loop forever or block the queue.

Per delivery:

1. Receive `amqp.Delivery` (`autoAck=false`).
2. Decode protobuf. On error: log `decode stock failed`, ack, continue.
3. `ReadSummary`. On `summarystore.ErrNotFound`: log `summary not found`, ack. On any other error: log `read summary failed`, ack.
4. Trim summary. Empty: log `summary is empty, skipping publish`, ack.
5. If a rewriter is configured, rewrite. On error or blank output: log the existing warning and publish the original summary (no change).
6. `Publish`. On error: log `publish failed`, ack.
7. Ack. On success log `summary published on GoToSocial`.

Ack failures are logged at `Warn` as `ack delivery failed`. Never retry an ack on a replacement channel: AMQP delivery tags are channel-scoped, so a tag from a dead channel is meaningless on a new one. If the session died before the ack, RabbitMQ requeues the unacked delivery. That produces **at-least-once delivery with possible duplicates**, while publication remains **at-most-once after the ack**. Concretely: a connection loss after GoToSocial accepted a status but before the ack lands can republish that stock. There is no idempotency key in the GoToSocial call, so this is accepted and documented.

**Shutdown during delivery.** If the run context is cancelled while a delivery is being processed (S3, Ollama, or GoToSocial is in flight), do not ack. Attempt `Nack(tag, false, true)` to requeue while the channel is still open, then return. If the channel is already dead, the settlement returns the session-closed error, is logged, and RabbitMQ has already requeued the delivery. This is the one deliberate departure from "ack every outcome": an interrupted outcome is not a completed outcome, and losing a stock on shutdown would be worse than a duplicate.

### 2.4 Reconnect and session supervision

`amqp091-go` does **not** reconnect automatically. `Consumer.Run(ctx)` supervises the whole session: dial → channel → declare topology → QoS → consume.

- **Dial:** `amqp.DialConfig(url, amqp.Config{Heartbeat: 10 * time.Second, Dial: ...})`. The TCP dial is bounded by `RABBITMQ_CONNECT_TIMEOUT_SECONDS` through `net.Dialer.DialContext` with the run context.
- **Session:** one `*amqp.Connection`, one `*amqp.Channel`, one deliveries stream. An AMQP channel is not safe for concurrent use, so every channel operation is serialized by a mutex in `session`.
- **Connect failure:** log `rabbitmq connect failed, backing off` with `attempt`, `retry_in`, and `error`; sleep with a cancel-aware timer; reconnect. Backoff is deterministic exponential: `1s, 2s, 4s, ...` capped at `30s`. The first failure and every tenth failure log at `Warn`; the rest at `Debug`.
- **Session established:** log `rabbitmq consumer connected` on the first connection and `rabbitmq consumer reconnected` afterwards (with `attempts`).
- **Deliveries channel closed:** log `rabbitmq consumer connection lost`, close the old session, increment the failure counter, and reconnect with the same backoff. A session that delivered at least one message resets the failure counter first, so a long healthy connection reconnects after `1s`, not after a long accumulated delay.
- **Context cancellation:** cancel the consumer (`Channel.Cancel(tag, false)`), non-blocking drain of already-prefetched deliveries with `Nack(tag, false, true)`, close channel and connection, log `rabbitmq consumer shutting down`, and return `nil`. Cancellation during a backoff sleep logs `rabbitmq consumer shutting down during backoff` and also returns `nil`.
- **Fatal errors:** there are none for this consumer. Broker failures are always retried; only context cancellation ends `Run`. `Run` therefore returns `nil` on every normal path (the signature stays `error` for composition-root symmetry).

A connect that wins the race against cancellation closes the newly opened session immediately instead of leaking it.

### 2.5 Readiness probe

`internal/rabbitmq/probe.go` replaces the legacy metadata probe with the same observable contract:

- `Check(ctx)` opens a **fresh short-lived connection** (never shares the consumer's connection or channel), opens one channel, and calls:
  - `ExchangeDeclarePassive(name, type, true, false, false, false, nil)`
  - `QueueDeclarePassive(name, true, false, false, false, nil)`
- Passive declares create nothing, consume nothing, and publish nothing. They verify that the exchange exists with the configured type/durability and that the queue exists with the expected flags.
- The whole attempt is bounded by a 5 s timeout. Because AMQP RPCs have no context parameter, `Check` runs the dial + passive declares in a goroutine and, on timeout, closes the connection to unblock any in-flight RPC; this prevents a broker that accepts TCP but never answers from hanging the probe.
- Retry/backoff matches the existing probe: 1 s initial, doubling to 30 s maximum, healthy re-check every 60 s.
- Readiness is sticky: `onReady` is called at most once, and a later failure never flips `/health` back to 503.
- Probe failures are expected until the consumer has declared the topology. A 404 (`NOT_FOUND`) means the exchange or queue does not exist yet or the names are wrong; a 406 (`PRECONDITION_FAILED`) means an existing object has different flags. Both are logged through `error`.

Exact log messages:

| Message | Level | When |
| --- | --- | --- |
| `rabbitmq connectivity verified` | Info | First successful probe; immediately before readiness flips |
| `rabbitmq connectivity probe failed` | Warn on attempt 1 and every 10th attempt, otherwise Debug | Probe failed while not yet ready |
| `rabbitmq connectivity lost` | Warn | First failure after readiness was achieved |
| `rabbitmq connectivity restored` | Info | First success after a loss |

Fields: `broker` (redacted URL), `exchange`, `exchange_type`, `routing_key`, `queue`, `attempt`, `retry_in`, `error`.

**Why a separate connection.** An AMQP protocol error (404/406) closes the channel it occurred on. Sharing the consumer's channel with the probe would kill consumption whenever the probe checked a missing queue. A separate connection also means a probe cannot interfere with prefetch, acks, or the consumer tag.

### 2.6 Configuration

Add `config.RabbitMQConfig` and `Config.RabbitMQ`; the final state removes the legacy broker fields and `KafkaServers()` (Step 4).

```
type RabbitMQConfig struct {
    URL               string
    InputExchange     string
    InputExchangeType string
    InputRoutingKey   string
    Queue             string
    Prefetch          int
    ConnectTimeout    time.Duration
}
```

| Env var | Type | Default | Required | Validation |
| --- | --- | --- | --- | --- |
| `RABBITMQ_URL` | string URI | `amqp://stocker:dYO81E36p5KOON@192.168.1.48:5672/` | no | Trimmed. Blank or absent uses the default. Must parse as an absolute `amqp://` or `amqps://` URI with a non-empty host. If a port is present it must be numeric `1..65535`. The value is passed verbatim to `amqp.DialConfig`. Never include the raw value in an error or log. |
| `RABBITMQ_INPUT_EXCHANGE` | string | `stock.summary` | no | If the variable is **present**, the trimmed value must be non-empty and at most 255 bytes. An explicitly blank value is an error, not a fallback to the default. |
| `RABBITMQ_INPUT_EXCHANGE_TYPE` | enum | `topic` | no | Trimmed, lower-cased. Blank (present or absent) selects `topic`. Otherwise one of `direct`, `topic`, `fanout`, `headers`. |
| `RABBITMQ_INPUT_ROUTING_KEY` | string | `stock.summary.available.v1` | no | Same present/blank rule as the exchange. Max 255 bytes. |
| `RABBITMQ_QUEUE` | string | `stocker-informer` | no | Same present/blank rule as the exchange. Max 255 bytes. |
| `RABBITMQ_PREFETCH` | integer | `1` | no | Absent or blank uses the default. Otherwise an integer in `[1, 65535]` (AMQP prefetch is a 16-bit field). |
| `RABBITMQ_CONNECT_TIMEOUT_SECONDS` | integer | `10` | no | Absent or blank uses the default. Otherwise an integer in `[1, 300]`. |

Required set after migration: `GOTOSOCIAL_INSTANCE`, `GOTOSOCIAL_USER`, `GOTOSOCIAL_TOKEN`, `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` (6). Every RabbitMQ setting has a working default.

Implementation details:

- Use `os.LookupEnv` for the name variables so "present and blank" is distinguishable from "absent"; the existing `getEnv` helper cannot express that distinction.
- Keep the aggregated error format byte-for-byte: `invalid configuration: <err1>; <err2>; ...`, wrapped by `main` as `load config: invalid configuration: ...`, exit code 1.
- Exact validation messages:
  - `RABBITMQ_URL must be a valid amqp:// or amqps:// URI with a host`
  - `RABBITMQ_URL must use the amqp:// or amqps:// scheme`
  - `RABBITMQ_URL must include a host`
  - `RABBITMQ_URL must include a valid port (1-65535)`
  - `RABBITMQ_INPUT_EXCHANGE must be non-empty`
  - `RABBITMQ_INPUT_EXCHANGE must be at most 255 bytes (got N)`
  - `RABBITMQ_INPUT_EXCHANGE_TYPE must be one of direct|topic|fanout|headers (got "x")`
  - `RABBITMQ_INPUT_ROUTING_KEY must be non-empty`
  - `RABBITMQ_QUEUE must be non-empty`
  - `RABBITMQ_PREFETCH must be an integer (got "x")`
  - `RABBITMQ_PREFETCH must be > 0 (got N)`
  - `RABBITMQ_PREFETCH must be <= 65535 (got N)`
  - `RABBITMQ_CONNECT_TIMEOUT_SECONDS must be an integer (got "x")`
  - `RABBITMQ_CONNECT_TIMEOUT_SECONDS must be > 0 (got N)`
  - `RABBITMQ_CONNECT_TIMEOUT_SECONDS must be <= 300 (got N)`
- Add `config.RedactedAMQPURL(raw) string` returning `scheme://host[:port]/vhost`, dropping user info, query, and fragment; return `"<invalid>"` when unparseable. Use it everywhere a broker address is logged.
- A malformed URL error must not echo the input. A test with `amqp://user:supersecret@/missing-host` must prove the password is absent from the error.

### 2.7 Package layout

```
internal/rabbitmq/
├── consumer.go        # Consumer, New, Run, connect, consume, handleDelivery, decodeStock
├── session.go         # connection/channel seams, session ack/nack/close, dialAMQP
├── probe.go           # Probe, NewProbe, Check, Run
├── consumer_test.go   # topology, pipeline, ack, reconnect, backoff
├── session_test.go    # ack/nack/close semantics, session-closed behaviour
├── probe_test.go      # passive declare, timeout, transitions, sticky readiness
└── consumer_live_test.go # opt-in, RABBITMQ_LIVE_TEST=1 only
```

Delete `internal/kafka/` entirely: `consumer.go`, `consumer_test.go`, `probe.go`, `probe_test.go`, `logging.go`, `logging_test.go`, and the directory itself. The `slog`↔Kafka logger adapter has no RabbitMQ equivalent and must not survive as dead code.

`SummaryReader` moves to `internal/rabbitmq` with its signature unchanged (`ReadSummary(ctx, exchange, symbol) ([]byte, error)`). `rewriter.Rewriter` and `messenger.Publisher` are untouched. Deliveries are plain values (`amqp.Delivery`) and are constructed directly in tests; only the connection and channel need interfaces.

Public API:

```go
func New(cfg config.RabbitMQConfig, summaries SummaryReader,
    summaryRewriter rewriter.Rewriter, publisher messenger.Publisher,
    log *slog.Logger) *Consumer

func (c *Consumer) Run(ctx context.Context) error

func NewProbe(cfg config.RabbitMQConfig, log *slog.Logger) *Probe
func (p *Probe) Run(ctx context.Context, onReady func())
```

Both constructors are pure: no network I/O until `Run`. Tests use the unexported `newConsumer`/`newProbe` with an injected dial function and sleep function.

### 2.8 Error handling summary

| Failure | Behaviour |
| --- | --- |
| Invalid configuration | Aggregated startup error, exit 1, secrets and URL redacted |
| Broker unreachable at startup | `rabbitmq connect failed, backing off`, 1 s→30 s retry forever; `/health` stays 503 |
| Topology declaration mismatch (`406 PRECONDITION_FAILED`) | Connect fails, channel closes, retry with backoff; `error` logged. Operator must fix the existing object or the configuration. |
| Credentials/permissions (`403 ACCESS_REFUSED`) | Connect fails, retried; operator fixes the vhost permissions |
| Connection lost while idle | Deliveries channel closes, session torn down, reconnect |
| Connection lost mid-delivery | S3/Ollama/GoToSocial may still complete; ack fails with session-closed, logged, delivery requeued (possible duplicate) |
| Malformed protobuf / empty identity | Log + ack (skip, no redelivery loop) |
| Missing/empty summary | Log + ack |
| Rewrite failure or blank report | Log + publish original + ack |
| Publish failure | Log + ack (same at-most-once outcome as today) |
| Shutdown during delivery | Nack with requeue when the channel is alive; otherwise broker requeues |
| Probe failure after readiness | `rabbitmq connectivity lost`, sticky 200, retries every 60 s |

### 2.9 Logging contract

All consumer and probe logs are structured `log/slog` JSON. Message bodies, summaries, reports, tokens, and raw broker URLs are never logged.

Consumer messages (exact strings):

- `rabbitmq consumer started` — once at startup, with topology fields
- `rabbitmq consumer connected`
- `rabbitmq consumer reconnected`
- `rabbitmq connect failed, backing off`
- `rabbitmq consumer connection lost`
- `rabbitmq consumer shutting down`
- `rabbitmq consumer shutting down during backoff`
- `rabbitmq consumer stopped`
- `decode stock failed`
- `summary not found`
- `read summary failed`
- `summary is empty, skipping publish`
- `rewrite failed, publishing original summary`
- `rewrite returned empty output, publishing original summary`
- `publish failed`
- `summary published on GoToSocial`
- `ack delivery failed`

Per-message fields use `symbol`, `exchange` (the **stock** exchange), `routing_key` (the AMQP routing key), `delivery_tag`, and `redelivered`. There is no `offset` field. The success line keeps `rewritten`, and adds `summary_chars`/`report_chars` only when rewritten.

Topology fields (`rabbitmq consumer started`, `configuration loaded`) use `broker` (redacted), `exchange` (the AMQP exchange), `exchange_type`, `routing_key`, `queue`, `prefetch`.

### 2.10 Documentation and history handling

The zero-reference gate includes filenames, so historical plan documents must be removed or rewritten. Decision:

| Path | Action | Rationale |
| --- | --- | --- |
| `docs/deployment.md` | **Keep and rewrite** | Still the operator guide. Replace its broker section with RabbitMQ ops/troubleshooting. |
| `docs/plan-kafka-connectivity-observability.md` | **Delete** | Historical plan for the old client's logger and probe. The shipped readiness/observability contract is carried into `docs/deployment.md`. |
| `docs/stock-summary-gotosocial.md` | **Delete** | Historical implementation plan; current architecture is in `README.md`/`AGENTS.md`. |
| `docs/plan-ollama-cora9-rewrite.md` | **Delete** | Historical plan; current Ollama behaviour and tests are documented in `README.md`/`AGENTS.md`. Inline the live-test command in README before deleting. |
| `docs/plan-remove-rooted-fallback.md` | **Delete** | Historical plan; canonical-only reads are documented in README/AGENTS. |
| `review.md` | **Delete** | Stale review (it claims the repo has no tests), findings since addressed, and it names the retired broker. |
| `docs/plan-rabbitmq-replace-kafka.md` (this file) | **Delete in Step 7** | Migration artifact whose filename contains the retired name. |

If the team wants to retain any of these, the only safe alternative is a rename to a neutral filename **and** a full rewrite that replaces every occurrence of the retired broker name with "the legacy broker"; the final grep is the arbiter. Deletion is chosen because it is unambiguous, and git history preserves every removed file.

---

## 3. Steps

Each step is independently implementable by one developer in one pass. The tree builds and all tests pass at the end of every step. Steps 1–3 add the new stack beside the old one; Step 4 performs the atomic cut-over.

---

### Step 1 — Add `amqp091-go` and the RabbitMQ configuration

**Goal.** Land the dependency and the complete, tested configuration surface without changing runtime behaviour. The old broker remains wired until Step 4.

**Files.**

- Modify: `go.mod`, `go.sum`
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`

**What to build.**

1. Add the dependency at the exact version used by the sibling services:

   ```bash
   go get github.com/rabbitmq/amqp091-go@v1.16.0
   ```

   Do **not** run `go mod tidy` in this step: the dependency is not imported until Step 2, so tidy would remove it again. `go mod tidy` runs in Step 4 and Step 7.

2. Add `RabbitMQConfig` exactly as specified in §2.6, keep the existing legacy broker fields for now, and add `RabbitMQ RabbitMQConfig` to `Config`.

3. Implement the loader using `os.LookupEnv`:

   - `RABBITMQ_URL`: absent or blank → default `amqp://stocker:dYO81E36p5KOON@192.168.1.48:5672/`; validate scheme/host/port; never echo it.
   - Name variables: absent → default; present → trimmed, non-empty, ≤255 bytes. Blank present is an error.
   - `RABBITMQ_INPUT_EXCHANGE_TYPE`: absent or blank → `topic`; otherwise trim/lower and validate the enum.
   - `RABBITMQ_PREFETCH`: absent or blank → 1; otherwise integer `1..65535`.
   - `RABBITMQ_CONNECT_TIMEOUT_SECONDS`: absent or blank → 10; otherwise integer `1..300`; store `time.Duration`.
   - Append every problem to the existing `errs` slice so the final error remains `invalid configuration: a; b; c`.

4. Add `RedactedAMQPURL`. It must never return user info or query parameters.

5. Keep all validation messages from §2.6 verbatim so documentation and tests match.

**Tests to add** (table-driven, using `t.Setenv`; treat an empty `t.Setenv` value as "present and blank"):

- Defaults: URL, exchange, type, routing key, queue, prefetch, connect timeout, `Config.RabbitMQ` populated.
- URL: valid `amqp`, valid `amqps`, valid custom port, missing scheme, `http://`, empty host, port `0`, port `65536`, non-numeric port, garbage.
- Names: each of exchange/routing key/queue accepts absent and valid custom values; rejects present-and-blank and a 256-byte name.
- Exchange type: absent/blank → `topic`; `TOPIC`, `Direct`, `fanout`, `headers` accepted; `x-direct` rejected.
- Prefetch: `1`, `10`, `65535` accepted; `0`, `-1`, `65536`, `abc` rejected.
- Connect timeout: `1`, `10`, `300` accepted; `0`, `301`, `abc` rejected.
- Aggregation: several invalid values produce one error containing all variable names.
- Secret hygiene: a URL containing a password never appears in an error or in `RedactedAMQPURL`.
- Absence of legacy variables: with every legacy broker env var cleared, `Load` succeeds when the other required variables are set.

**Acceptance.**

```bash
gofmt -l internal/config          # empty
go vet ./internal/config          # clean
go test ./internal/config -count=1
go build ./...                    # succeeds
```

**Commit.** `feat(config): add RabbitMQ configuration and amqp091-go`

---

### Step 2 — Implement the RabbitMQ consumer and session

**Goal.** Add a complete, offline-tested `internal/rabbitmq` consumer. It is not imported by `main` yet, so the binary's behaviour is unchanged.

**Files.**

- Create: `internal/rabbitmq/consumer.go`
- Create: `internal/rabbitmq/session.go`
- Create: `internal/rabbitmq/consumer_test.go`
- Create: `internal/rabbitmq/session_test.go`
- Create: `internal/rabbitmq/consumer_live_test.go` (opt-in only)

**What to build.**

1. **`session.go`** — test seams and production dialer:

   ```go
   type connection interface {
       Channel() (channel, error)
       Close() error
   }

   type channel interface {
       ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
       QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
       QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
       Qos(prefetchCount, prefetchSize int, global bool) error
       Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
       Cancel(consumer string, noWait bool) error
       Ack(tag uint64, multiple bool) error
       Nack(tag uint64, multiple, requeue bool) error
       IsClosed() bool
       Close() error
   }

   type dialFunc func(ctx context.Context, url string, timeout time.Duration) (connection, error)
   ```

   `dialAMQP` must use `amqp.Config{Heartbeat: 10 * time.Second, Dial: func(network, addr string) (net.Conn, error) { ... }}` with `net.Dialer{Timeout: timeout}.DialContext(ctx, network, addr)` and then `amqp.DialConfig(rawURL, conf)`. Return the raw client error; never wrap the URL into it.

   `session` holds `conn`, `ch`, `deliveries`, `tag`, a `sync.Mutex`, and `closed`. Implement `ack(tag)`, `nack(tag, requeue)`, `cancelConsumer()`, and idempotent `close()`. On a closed session, `ack`/`nack` return `errSessionClosed` without touching the channel.

2. **`consumer.go`** — `SummaryReader`, `Consumer`, `New`, and `Run` exactly as specified in §2.4/§2.7.

3. `connect(ctx)`:

   - Dial with `cfg.ConnectTimeout`.
   - Open a channel.
   - `ExchangeDeclare(cfg.InputExchange, cfg.InputExchangeType, true, false, false, false, nil)`.
   - `QueueDeclare(cfg.Queue, true, false, false, false, nil)`.
   - `QueueBind(cfg.Queue, cfg.InputRoutingKey, cfg.InputExchange, false, nil)`.
   - `Qos(cfg.Prefetch, 0, false)`.
   - `Consume(cfg.Queue, fmt.Sprintf("stocker-informer-%d", os.Getpid()), false, false, false, false, nil)`.
   - On any error after dial, close channel and connection before returning.
   - If the run context is cancelled after a successful dial but before the session is adopted, close the new session immediately instead of leaking it.

4. `Run(ctx)`:

   - Log `rabbitmq consumer started` with redacted broker and topology fields.
   - Supervise connect/consume/reconnect with deterministic `1s→30s` exponential backoff.
   - Use cancel-aware sleeps (a timer + `select` on `ctx.Done()`), never `time.Sleep` in the loop.
   - Reset the failure counter after a session that delivered at least one message.
   - On cancellation: cancel the consumer, drain prefetched deliveries with `Nack(..., true)`, close the session, log `rabbitmq consumer shutting down`, return `nil`. During a backoff sleep, log `rabbitmq consumer shutting down during backoff` and return `nil`.
   - On session loss: log `rabbitmq consumer connection lost`, close the session, and reconnect.

5. `consume(ctx, sess)` selects on `sess.deliveries` and `ctx.Done()`. A closed deliveries channel returns the session-closed error. Context cancellation triggers `cancelConsumer()` + `drainDeliveries()` and returns `nil`.

6. `handleDelivery(ctx, sess, d)` implements §2.3 exactly:

   - Build `attrs` with `routing_key`, `delivery_tag`, `redelivered`.
   - Decode; on error log and ack.
   - Read summary; distinguish `summarystore.ErrNotFound`.
   - Trim; empty → warn + ack.
   - Rewrite when the rewriter is non-nil; fall back on error or blank output.
   - Publish; error → log + ack.
   - If `ctx.Err() != nil` before settling, nack with requeue and return (shutdown path).
   - Ack; on ack error log `ack delivery failed`; then log success.

7. `decodeStock` keeps the current rules: `proto.Unmarshal`, trim `symbol`/`exchange`, reject either if empty. Never log the body.

8. **Opt-in live integration test** (`consumer_live_test.go`):

   - Skip unless `RABBITMQ_LIVE_TEST=1`.
   - Read the broker URL from `RABBITMQ_URL`, falling back to the production default.
   - Use unique names with a timestamp suffix for the exchange, queue, and routing key so a live run can never touch the real `stocker-informer` queue or the shared `stock.summary` exchange.
   - Declare a direct exchange and queue, run `Consumer.Run` with a fake `SummaryReader`, a fake rewriter, and a fake publisher in a goroutine, publish one protobuf `Stock`, and assert the fake publisher receives the summary exactly once.
   - Cancel the run context, wait for `Run` to return, then delete the queue and exchange in `t.Cleanup`.
   - The test must be skipped by `go test ./...` with no environment variable, so the normal gate stays offline.

**Tests to add.**

- Topology: fake connection/channel records every call; assert exchange type/durable flags, queue flags (`durable=true`, `autoDelete=false`, `exclusive=false`), binding arguments, `Qos(prefetch, 0, false)`, and `Consume(..., autoAck=false, ...)`.
- Prefetch forwarding: `RABBITMQ_PREFETCH` values reach `Qos`.
- Ack-on-every-outcome: table over (a) invalid protobuf, (b) missing symbol, (c) missing exchange, (d) `ErrNotFound`, (e) other S3 error, (f) empty summary, (g) rewrite error, (h) blank rewrite, (i) publish error, (j) publish success. Assert exactly one ack per delivery and the right log message.
- Rewriter nil path: publishes the trimmed original and reports `rewritten=false`.
- Rewrite success: `rewritten=true`, `summary_chars` and `report_chars` present.
- Shutdown mid-delivery: cancel the context while a fake publisher blocks; assert no ack, one requeueing nack, and `Run` returns `nil`.
- Ack failure: fake channel returns an error from `Ack`; assert `ack delivery failed` is logged and no retry is attempted on a new channel.
- Reconnect: first dial fails, second succeeds; assert `rabbitmq connect failed, backing off`, then `rabbitmq consumer connected`, and the sleeps observed are `1s`, then (after loss) `1s`.
- Backoff growth: three failed dials produce `1s, 2s, 4s`; enough failures cap at `30s`.
- Cancel during backoff: `Run` returns `nil` promptly.
- Connection loss: close the deliveries channel; assert `rabbitmq consumer connection lost` and a reconnect attempt.
- Session: `ack`, `nack`, and `close` semantics; double close; settlement after close returns `errSessionClosed`.
- Log hygiene: capture the logger and assert the summary/report bodies and the raw broker password never appear.

**Acceptance.**

```bash
gofmt -l internal/rabbitmq       # empty
go vet ./internal/rabbitmq       # clean
go test ./internal/rabbitmq -count=1
go test ./internal/rabbitmq -run TestLiveConsumer -count=1   # skips without RABBITMQ_LIVE_TEST=1
go build ./...                   # succeeds
```

**Commit.** `feat(rabbitmq): add supervised consumer and session`

---

### Step 3 — Implement the RabbitMQ readiness probe

**Goal.** Add the replacement connectivity probe, still unwired. `/health` behaviour will switch in Step 4.

**Files.**

- Create: `internal/rabbitmq/probe.go`
- Create: `internal/rabbitmq/probe_test.go`

**What to build.**

1. `Probe` fields: `cfg`, `log`, `dial`, `attemptTimeout` (5 s), `retryInitial` (1 s), `retryMax` (30 s), `healthyEvery` (60 s). `NewProbe(cfg, log)` fills production defaults and a `dialAMQP`-based dial function. `newProbe(cfg, log, dial)` is the test constructor.

2. Probe-local seams:

   ```go
   type probeConnection interface {
       Channel() (probeChannel, error)
       Close() error
   }

   type probeChannel interface {
       ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
       QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
       Close() error
   }
   ```

3. `Check(ctx) error`:

   - Wrap the whole attempt in `context.WithTimeout(ctx, p.attemptTimeout)`.
   - Run dial + open channel + passive declares in a goroutine.
   - Exchange first: `ExchangeDeclarePassive(cfg.InputExchange, cfg.InputExchangeType, true, false, false, false, nil)`.
   - Queue second: `QueueDeclarePassive(cfg.Queue, true, false, false, false, nil)`.
   - Close the channel and connection before returning.
   - On attempt timeout, close the connection (if already dialed) to unblock the in-flight RPC, then return a wrapped `context.DeadlineExceeded`.
   - Return wrapped errors naming the exchange or queue. Never consume, publish, bind, or declare non-passively.

4. `Run(ctx, onReady)` copies the proven transition logic from the old probe:

   - Sticky `ready` and `lost` booleans; `onReady` at most once.
   - `rabbitmq connectivity probe failed` at Warn on attempt 1 and every 10th, otherwise Debug, with `attempt`, `retry_in`, `error`.
   - `rabbitmq connectivity lost` once after readiness.
   - `rabbitmq connectivity verified` and `rabbitmq connectivity restored`.
   - Backoff 1 s doubling to 30 s; 60 s healthy re-check; cancel-aware sleeps.

**Tests to add.**

- Success: passive exchange and queue declares called with the exact names/types/flags; channel and connection closed; no `Consume`, `QueueBind`, or non-passive declare.
- Exchange 404-like error: reported and connection closed.
- Queue 404-like error: reported.
- Dial error: reported.
- Timeout: a fake that blocks until its connection is closed; assert `Check` returns within the attempt timeout and closes the connection.
- `Run`: `onReady` called exactly once over repeated successes; a failure after success logs `rabbitmq connectivity lost` and never calls `onReady` again; recovery logs `rabbitmq connectivity restored`; cancel exits promptly.
- Backoff: 1 s, 2 s, 4 s, …, 30 s cap; cancel during sleep exits.
- Redaction: captured logs contain the redacted broker only.

**Acceptance.**

```bash
gofmt -l internal/rabbitmq       # empty
go vet ./internal/rabbitmq       # clean
go test ./internal/rabbitmq -count=1
go build ./...                   # succeeds
```

**Commit.** `feat(rabbitmq): add passive-topology readiness probe`

---

### Step 4 — Cut over the composition root and delete the legacy transport

**Goal.** Switch `main` to the new consumer/probe, remove every legacy configuration field and source file, and tidy the module graph. After this step the service runs only on RabbitMQ.

**Files.**

- Modify: `cmd/server/main.go`
- Modify: `cmd/server/main_test.go` (no behavioural change; update only if a comment mentions the transport)
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Delete: `internal/kafka/consumer.go`
- Delete: `internal/kafka/consumer_test.go`
- Delete: `internal/kafka/probe.go`
- Delete: `internal/kafka/probe_test.go`
- Delete: `internal/kafka/logging.go`
- Delete: `internal/kafka/logging_test.go`
- Delete: the now-empty `internal/kafka/` directory
- Modify: `go.mod`, `go.sum` (`go mod tidy`)

**What to build.**

1. `run()`:

   ```go
   consumer := rabbitmq.New(
       cfg.RabbitMQ,
       store,
       summaryRewriter,
       publisher,
       log,
   )
   probe := rabbitmq.NewProbe(cfg.RabbitMQ, log)
   ```

   Start the probe exactly as before:

   ```go
   log.Info("rabbitmq connectivity probe started",
       "broker", config.RedactedAMQPURL(cfg.RabbitMQ.URL),
       "exchange", cfg.RabbitMQ.InputExchange,
       "queue", cfg.RabbitMQ.Queue,
   )
   go probe.Run(ctx, func() { ready.Store(true) })
   ```

2. `configuration loaded` fields become:

   - `rabbitmq_broker` (redacted)
   - `rabbitmq_exchange`
   - `rabbitmq_exchange_type`
   - `rabbitmq_routing_key`
   - `rabbitmq_queue`
   - `rabbitmq_prefetch`

   Keep the existing S3, audience, and Ollama fields.

3. Remove `KafkaBootstrapServers`, `KafkaTopic`, `KafkaConsumerGroup`, `KafkaServers()`, `validateBroker`, and all legacy env validation/tests from `internal/config`. Keep the RabbitMQ tests from Step 1.

4. Delete `internal/kafka/` in full. Remove imports of the old package and the old client logger. There is no RabbitMQ equivalent of the client logger; `amqp091-go` does not take an application logger.

5. Update the `newHealthMux` comment to say readiness is flipped by the RabbitMQ probe. Response bodies, status codes, and stickiness are unchanged.

6. Run `go mod tidy`. `go.mod` must no longer require the legacy client, `github.com/klauspost/compress`, or `github.com/pierrec/lz4/v4`.

**Acceptance.**

```bash
gofmt -l .                                  # empty
go vet ./...                                # clean
go test ./... -count=1                      # passes
go build ./cmd/server/                      # succeeds
test ! -d internal/kafka                    # succeeds
grep -rin kafka internal cmd --include='*.go'   # no output
grep -n "segmentio\|klauspost/compress\|pierrec/lz4" go.mod go.sum   # no output
```

**Commit.** `feat: consume summary signals from RabbitMQ`

---

### Step 5 — Rewrite `README.md` and `AGENTS.md`

**Goal.** Make the two primary developer/operator documents describe only the RabbitMQ service. Every statement must match the Step 4 code.

**Files.**

- Modify: `README.md`
- Modify: `AGENTS.md`

**README changes (use the existing heading structure).**

- **Intro:** "consumes protobuf stock summary signals from RabbitMQ".
- **Key features:** replace the consumer-group/offset bullet with: durable RabbitMQ queue subscription, manual acknowledgements, automatic reconnect with exponential backoff, competing consumers share the queue.
- **At a glance:** dependency `github.com/rabbitmq/amqp091-go v1.16.0`; configuration "6 required + 21 optional".
- **Quick start:** replace the legacy export block with:

  ```bash
  # RabbitMQ (all optional; defaults shown)
  export RABBITMQ_URL=amqp://user:password@localhost:5672/
  export RABBITMQ_INPUT_EXCHANGE=stock.summary
  export RABBITMQ_INPUT_EXCHANGE_TYPE=topic
  export RABBITMQ_INPUT_ROUTING_KEY=stock.summary.available.v1
  export RABBITMQ_QUEUE=stocker-informer
  export RABBITMQ_PREFETCH=1
  ```

- **Prerequisites:** a reachable RabbitMQ broker (vhost `/` by default); this service declares the durable `stock.summary` exchange, its own durable queue, and the binding. A producer must publish protobuf signals under the configured routing key.
- **Configuration:** update the counts and the example error. Use an S3/RabbitMQ validation example, e.g. `load config: invalid configuration: RABBITMQ_INPUT_EXCHANGE must be non-empty; S3_BUCKET is required and must be non-empty`.
- **Environment Variables:** remove the three legacy rows; add exactly the seven RabbitMQ rows from §2.6, including that the URL is secret-bearing and never logged, and the present-but-blank rule for names.
- **Validation notes:** add the URL, name, type, prefetch, and timeout rules.
- **Example env file:** update the inline comment to say RabbitMQ, S3, Ollama, and GoToSocial values.
- **"Kafka input schema" → "Message input schema":** "Record value" → "Message body"; "Producer and topic" → "Producer, exchange, and routing key". State that the producer declares `stock.summary` and publishes `stock.summary.available.v1`, and that the body is a protobuf `stockstore.v1.Stock`.
- **Decode behaviour:** say the delivery is acked so the message is skipped rather than redelivered.
- **Running:** `configuration loaded` fields and `rabbitmq consumer started`.
- **Operations → Logging:** remove the old client logger bullet. Add: amqp091-go has no automatic reconnect; the consumer supervises session restarts; ack happens after every pipeline outcome; message bodies and the broker URL are never logged.
- **"Kafka consumer behaviour" → "RabbitMQ consumer behaviour":**
  - exchange/queue/binding/QoS/consume flags from §2.2, prefetch default 1;
  - manual ack with `autoAck=false`;
  - deterministic reconnect backoff `1s→30s`;
  - on connect failure/session loss the process retries forever; no crash;
  - declaration mismatch (`406 PRECONDITION_FAILED`) is a configuration error requiring operator action.
- **"Offset & failure semantics" → "Ack & failure semantics":** ack every completed outcome; at-most-once after ack; unacked deliveries are requeued on connection loss, so duplicates are possible; no exactly-once claim.
- **Health endpoint:** RabbitMQ probe wording; sticky readiness; `rabbitmq connectivity verified/lost/restored`.
- **Signals & shutdown:** cancels the consumer, requeues prefetched deliveries, closes channel and connection, returns `nil`.
- **How to verify it's working:** replace "produce on topic" with publishing a protobuf message to the `stock.summary` exchange with routing key `stock.summary.available.v1` (management UI, `rabbitmqadmin`, or a small Go publisher). Restart check: the durable queue retains messages and unacked deliveries are redelivered.
- **Repository structure:** `internal/rabbitmq/{consumer.go,session.go,probe.go}`. Leave the entries for `review.md` and the historical plan documents alone; Step 6 deletes those files and updates the structure list and any remaining links in the same commit.
- **Testing:** document `RABBITMQ_LIVE_TEST=1` and inline the Ollama live-test command (the Ollama plan is deleted).
- **Known limitations:** sticky readiness; duplicate publication possible after a connection loss before ack; prefetch 1 + single worker means a backlog grows during downstream outages; RabbitMQ declaration mismatch is fatal until fixed.
- **Troubleshooting link:** point to `docs/deployment.md#troubleshooting-rabbitmq-connectivity`.

**AGENTS.md changes.**

- Module summary: RabbitMQ instead of the legacy broker.
- Key facts: `github.com/rabbitmq/amqp091-go v1.16.0`; build/test/lint commands unchanged.
- Structure: `internal/rabbitmq/` with consumer/session/probe; the docs line lists `deployment.md` and notes that the historical plans are pruned in Step 6 (do not add this migration plan to the tree listing).
- Architecture paragraph: subscribe to the durable queue, manual ack, reconnect supervision, ack every outcome.
- Environment variables: replace the three legacy required variables with the seven RabbitMQ variables (all optional, with defaults), and state that the six required variables are the GoToSocial and S3 ones.
- Testing: offline fakes; `RABBITMQ_LIVE_TEST=1` opt-in; no network in `go test ./...`.

**Acceptance.**

```bash
grep -rin kafka README.md AGENTS.md          # no output (exit 1)
grep -n "KAFKA_" README.md AGENTS.md         # no output
grep -n "RABBITMQ_URL\|RABBITMQ_QUEUE\|RABBITMQ_INPUT" README.md AGENTS.md  # present
```

Manual check: every command and env var in the two files matches the Step 4 code and the defaults in §2.6.

**Commit.** `docs: document the RabbitMQ transport`

---

### Step 6 — Update deploy assets, the operator guide, and remove historical broker docs

**Goal.** Make the deploy path and the operator runbook accurate, and remove every historical document that cannot pass the zero-reference gate.

**Files.**

- Modify: `deploy/quadlet/.env.podman.example`
- Modify: `deploy/quadlet/stocker-informer.container`
- Modify: `docs/deployment.md`
- Delete: `docs/plan-kafka-connectivity-observability.md`
- Delete: `docs/stock-summary-gotosocial.md`
- Delete: `docs/plan-ollama-cora9-rewrite.md`
- Delete: `docs/plan-remove-rooted-fallback.md`
- Delete: `review.md`

**`.env.podman.example`.**

Replace the legacy block with:

```dotenv
# ── RabbitMQ ────────────────────────────────────────────────────────────────
# All values are optional; these are the shipped defaults.
# RABBITMQ_URL=amqp://stocker:REPLACE_ME@192.168.1.48:5672/
# Leaving RABBITMQ_URL unset uses the built-in shared-test-broker default.
RABBITMQ_INPUT_EXCHANGE=stock.summary
RABBITMQ_INPUT_EXCHANGE_TYPE=topic
RABBITMQ_INPUT_ROUTING_KEY=stock.summary.available.v1
RABBITMQ_QUEUE=stocker-informer
RABBITMQ_PREFETCH=1
RABBITMQ_CONNECT_TIMEOUT_SECONDS=10
```

Do not commit a real credential. Keep the existing "NEVER commit real credentials" warning.

**Quadlet.** Delete the line `After=kafka-broker-1.container`. `After=network-online.target` is sufficient; the broker is remote and the consumer already retries. Keep every other setting unchanged.

**`docs/deployment.md`.**

- Update the global-constraints dependency line and the configuration summary.
- Replace the legacy env names in the sample blocks and historical task snippets with the RabbitMQ names from §2.6. Since this file is a historical deployment plan that is still the operator guide, the text must read as current truth where it gives commands.
- Update the repository tree to `internal/rabbitmq/consumer.go` (plus `session.go`/`probe.go` if listed).
- Update `README.md`'s repository-structure block and any remaining links so the deleted `review.md` and historical plan files no longer appear.
- Rename the end-to-end check to "Manual end-to-end check (RabbitMQ + S3 + GoToSocial)".
- Replace the entire old troubleshooting section with `## Troubleshooting RabbitMQ connectivity`:

  **Log signatures**

  | Signature | Meaning | Transient? |
  | --- | --- | --- |
  | `rabbitmq connect failed, backing off` with `error` containing `connection refused` | Broker down, wrong host/port, or wrong scheme (`amqp` vs `amqps`). | Only if the broker is restarting; otherwise a configuration or network fault. |
  | `rabbitmq consumer connection lost` with `error` containing `EOF` or `connection reset by peer` | Broker restarted, connection dropped, or network flap. | Yes; the consumer reconnects with 1 s→30 s backoff. |
  | `rabbitmq connectivity probe failed` with a `NOT_FOUND` (404) error | Exchange or queue not declared yet, or names are wrong. | Startup: briefly, until the consumer declares. Sustained: configuration error. |
  | `rabbitmq connectivity probe failed` with `PRECONDITION_FAILED` (406) | An existing exchange/queue has different type/durability/flags/arguments than configured. | No; requires operator action. |
  | `ack delivery failed` with `consumer: session closed` | Session died mid-delivery; the broker requeued the delivery. | Yes, but expect a possible duplicate publication. |
  | `rabbitmq consumer started` then no `summary published on GoToSocial` and no errors | Wrong routing key, missing binding, or a producer writing elsewhere. | No; configuration problem. |

  **Broker-side checklist**

  - Broker is running and reachable on the configured host/port; `amqps://` requires TLS on that port.
  - The vhost in `RABBITMQ_URL` exists (`/` by default).
  - The user in `RABBITMQ_URL` can **configure, write, and read** on the exchange and queue (declaration needs `configure`).
  - Exchange `stock.summary` exists as type `topic`, durable.
  - Queue `stocker-informer` exists as durable, non-exclusive, non-auto-delete.
  - Binding: exchange `stock.summary`, queue `stocker-informer`, routing key `stock.summary.available.v1`.
  - No declaration mismatch (406) from a previous deployment with different flags.
  - The producer (`stocker-summarizer`) publishes to the same exchange/routing key and its messages are not expiring before consumption.

  **Verification commands**

  ```bash
  rabbitmqctl list_exchanges name type durable
  rabbitmqctl list_queues name durable messages messages_ready messages_unacknowledged consumers
  rabbitmqctl list_bindings source_name destination_name routing_key
  rabbitmqctl list_connections name peer_host state
  rabbitmqctl list_channels connection number consumer_count messages_unacknowledged
  # Publish one test message (management UI or rabbitmqadmin also work):
  #   exchange stock.summary, routing key stock.summary.available.v1,
  #   payload = protobuf stockstore.v1.Stock{symbol:"AAPL",exchange:"NASDAQ"}
  ```

  **406 PRECONDITION_FAILED.** The exchange or queue already exists with different properties (`type`, `durable`, `auto_delete`, `exclusive`, or arguments). The consumer cannot fix this by itself: stop the service, inspect with the commands above, then either correct the environment to match the existing object or delete the service-owned object and let the consumer recreate it. Do not delete a shared exchange that producers still use.

  **404 NOT_FOUND.** On a fresh deployment the probe can see 404s until the consumer's first connect declares the topology. If the 404s persist, check the exchange/queue names and the vhost.

  **Credentials and permissions.** `ACCESS_REFUSED` means the user cannot access the vhost or lacks configure/write/read permission. Verify with `rabbitmqctl authenticate_user <user> <password>` and `rabbitmqctl list_permissions -p <vhost>`. The URL is never logged; confirm the deployed value in the operator's env file, not in logs.

  **Readiness semantics.** `/health` returns 503 until the first successful passive verification, then 200. Readiness is sticky: a later loss logs `rabbitmq connectivity lost` and recovery logs `rabbitmq connectivity restored`, but the endpoint does not flip back. Post-startup outages are visible in logs, not in `/health`.

  **Configuration drift.** The operator's `~/.config/stocker-informer/.env.podman` is authoritative. The `configuration loaded` log shows the redacted broker plus exchange, type, routing key, queue, and prefetch; the `rabbitmq consumer connected` log repeats the topology.

**Historical documents.** Delete the five files listed above with `git rm`. Before deleting `docs/plan-ollama-cora9-rewrite.md`, make sure Step 5 has inlined its live-test command into README. The durable behaviour those plans described is already covered by README, AGENTS.md, this migration design, and the rewritten deployment guide.

**Acceptance.**

```bash
grep -rin kafka deploy docs review.md --exclude=plan-rabbitmq-replace-kafka.md 2>/dev/null   # no output
test ! -e review.md
test ! -e docs/stock-summary-gotosocial.md
test ! -e docs/plan-ollama-cora9-rewrite.md
test ! -e docs/plan-remove-rooted-fallback.md
test ! -e docs/plan-kafka-connectivity-observability.md
grep -n "After=" deploy/quadlet/stocker-informer.container    # only network-online.target
grep -n "RABBITMQ_" deploy/quadlet/.env.podman.example        # the seven variables
grep -n "Troubleshooting RabbitMQ connectivity" docs/deployment.md
```

**Commit.** `docs(deploy): switch operator assets to RabbitMQ`

---

### Step 7 — Repository-wide verification and removal of the migration artifact

**Goal.** Prove the zero-reference criterion, remove this plan (its filename contains the retired name), and leave a clean, reproducible tree.

**Files.**

- Delete: `docs/plan-rabbitmq-replace-kafka.md`
- Modify only if verification finds leftovers.

**What to do.**

1. Run `go mod tidy` and confirm it is a no-op.
2. Run the full gate (below).
3. Remove any reference to this plan from `README.md`, `AGENTS.md`, and `docs/deployment.md` (`grep -rn "plan-rabbitmq" README.md AGENTS.md docs` must return nothing), then delete this plan with `git rm docs/plan-rabbitmq-replace-kafka.md`. Its design and operational outcomes are already in README/AGENTS/deployment; git history preserves the plan.
4. Re-run the final grep and filename scan.
5. Commit and push.

**Acceptance — exact commands and expected results.**

```bash
# 1. Formatting: no output
gofmt -l .

# 2. Static analysis: no output
go vet ./...

# 3. Tests: all packages pass, no network required
go test ./... -count=1

# 4. Build: succeeds
go build ./cmd/server/

# 5. Module graph is tidy: no diff
go mod tidy
git diff --exit-code go.mod go.sum

# 6. Zero textual references: no output, exit status 1 (expected for "no match")
grep -rin kafka . --exclude-dir=.git

# 7. Zero filename references: no output
find . -path ./.git -prune -o -iname '*kafka*' -print

# 8. Legacy modules gone
grep -rn "segmentio/kafka-go\|klauspost/compress\|pierrec/lz4" go.mod go.sum   # no output
go list -m all | grep -i kafka                                                # no output

# 9. Legacy package gone
test ! -d internal/kafka

# 10. RabbitMQ stack present
go list -m github.com/rabbitmq/amqp091-go        # v1.16.0
grep -n "RABBITMQ_" deploy/quadlet/.env.podman.example

# 11. No stale reference to the deleted migration plan
grep -rn "plan-rabbitmq" README.md AGENTS.md docs   # no output

# 12. Working tree clean after the commit
git status --porcelain
```

The planning commit for this file is `docs: add RabbitMQ migration plan replacing Kafka`. The implementation commits are the per-step messages in Steps 1–6; Step 7 adds `chore: remove RabbitMQ migration plan after cutover`.

---

## 4. Risks and edge cases

### 4.1 Declaration mismatch (`406 PRECONDITION_FAILED`)

RabbitMQ refuses to declare an exchange or queue whose existing properties differ from the request (exchange type/durability/auto-delete/internal/arguments, queue durability/exclusivity/auto-delete/arguments). `connect` therefore fails and the consumer retries forever while `/health` stays 503. This is loud in logs (`rabbitmq connect failed, backing off` with a 406 error) but is **not** self-healing. The runbook must tell operators how to inspect and correct it. The probe's passive declares surface the same mismatch before readiness flips.

Mitigation: never change the declared flags without a migration; document the exact flags in README and deployment.md; use `rabbitmqctl list_exchanges`/`list_queues` to compare.

### 4.2 Credentials in the broker URL

`RABBITMQ_URL` contains a password. Never log the raw value, never wrap it into an error, and never include it in a validation message. Use `config.RedactedAMQPURL` for every log field. `amqp.DialConfig` errors are returned as-is; they describe host/port and protocol failures, not the URL. Tests assert that a password in a malformed URL does not appear in the error.

### 4.3 Shutdown during delivery

S3, Ollama, and GoToSocial calls honour the run context, so a SIGTERM cancels them. The delivery is then nacked with requeue when the channel is still open, or requeued automatically by the broker when the session closes. A slow Ollama call (up to `OLLAMA_TIMEOUT_SECONDS=120`) can delay process exit beyond the orchestrator's stop grace period; systemd then kills the process, which still requeues the unacked delivery. No message is lost; a duplicate is possible if publication completed before the interruption.

### 4.4 Duplicate processing

After an ack the message is gone; before an ack, a session loss requeues it. A connection drop between GoToSocial accepting a status and the ack reaching RabbitMQ republishes that stock. GoToSocial has no idempotency key in this client, so duplicates are possible. This matches the at-least-once delivery contract of the sibling services and must be documented, not silently assumed away.

### 4.5 Queue backlog

`RABBITMQ_PREFETCH=1` plus a single synchronous worker means throughput is one stock at a time. If S3, Ollama, or GoToSocial is slow or down, the durable queue grows instead of dropping work. Monitor `messages_ready` and `messages_unacknowledged`. Multiple replicas can share the queue (competing consumers); the queue is non-exclusive for that reason. Do not raise prefetch without introducing concurrency, or a single worker will hold multiple unacked deliveries and increase duplicate exposure on restart.

### 4.6 Readiness is not liveness

The probe verifies the broker and topology, not that messages are flowing. A missing binding, a producer publishing to a different routing key, or an empty queue still yields `/health` 200. Readiness is also sticky, so a post-startup broker outage does not flip it back to 503. This is deliberate (avoid restart loops) and is documented in the runbook; logs are the signal for post-startup outages.

### 4.7 Probe cannot verify bindings

`amqp091-go` exposes passive exchange and queue declaration but no passive binding check. The probe therefore verifies existence/flags, not the binding. A missing or wrong binding is a silent-no-message failure; `rabbitmqctl list_bindings` is the verification step in the runbook.

### 4.8 Probe timeout and hung brokers

AMQP RPCs are not context-aware. The probe runs its checks in a goroutine, bounds the attempt with a 5 s context, and closes the connection on timeout to unblock the RPC. Without the close, a broker that accepts TCP but never responds could leak a goroutine and stall readiness. The consumer's dial is bounded by `RABBITMQ_CONNECT_TIMEOUT_SECONDS`; a hung post-dial handshake is bounded by the 10 s heartbeat/connection-close behaviour of the client.

### 4.9 Default broker URL is a shared test broker

The default URL (`192.168.1.48:5672`) is the fleet's shared test broker, not production. It is a default for developer convenience and must be overridden in every real deployment via `RABBITMQ_URL`. Because the default contains credentials, the env template must not commit real production credentials and the runbook must point operators at their own env file.

### 4.10 Deleted historical plans

Deleting the five historical documents removes context from the tree (git history retains it). The trade-off is deliberate: the zero-reference gate includes filenames and text, and rewriting anachronistic plans to describe RabbitMQ would produce misleading history. The current truth is consolidated in README, AGENTS.md, and deployment.md. If the team prefers to keep any document, it must be renamed to a neutral filename and fully scrubbed before the final grep.

### 4.11 Explicitly blank names

Treating a present-but-blank `RABBITMQ_QUEUE`, `RABBITMQ_INPUT_EXCHANGE`, or `RABBITMQ_INPUT_ROUTING_KEY` as an error prevents a silent fallback that would consume from a queue the operator did not intend. `RABBITMQ_INPUT_EXCHANGE_TYPE=""` is the one exception (blank means `topic`), matching the sibling services and the AMQP default.

---

## 5. Final verification checklist

Copy this list into the pull request and tick each item with the command output.

- [ ] `gofmt -l .` prints nothing.
- [ ] `go vet ./...` prints nothing.
- [ ] `go test ./... -count=1` passes, with no broker or network access.
- [ ] `go build ./cmd/server/` succeeds.
- [ ] `go mod tidy` produces no diff in `go.mod`/`go.sum`.
- [ ] `grep -rin kafka . --exclude-dir=.git` prints nothing (exit status 1).
- [ ] `find . -path ./.git -prune -o -iname '*kafka*' -print` prints nothing.
- [ ] `grep -rn "segmentio/kafka-go\|klauspost/compress\|pierrec/lz4" go.mod go.sum` prints nothing.
- [ ] `go list -m all | grep -i kafka` prints nothing.
- [ ] `test ! -d internal/kafka` succeeds.
- [ ] `go list -m github.com/rabbitmq/amqp091-go` reports `v1.16.0`.
- [ ] `README.md`, `AGENTS.md`, and `docs/deployment.md` describe only the RabbitMQ topology and defaults.
- [ ] `deploy/quadlet/.env.podman.example` lists the seven RabbitMQ variables and no real credentials.
- [ ] `deploy/quadlet/stocker-informer.container` has no broker-specific `After=` line.
- [ ] `docs/deployment.md` contains `## Troubleshooting RabbitMQ connectivity` with `list_queues`, `list_bindings`, 406, 404, credentials, and readiness guidance.
- [ ] The historical plan documents and `review.md` are gone.
- [ ] `docs/plan-rabbitmq-replace-kafka.md` is gone (this plan is deleted in Step 7).
- [ ] `/health` still returns 503 `{"status":"starting","version":"..."}` until the probe succeeds, then 200 `{"status":"ok","version":"..."}`, sticky.
- [ ] A manual publish to `stock.summary` with routing key `stock.summary.available.v1` produces `summary published on GoToSocial` and logs `routing_key`/`queue` fields, never `offset`.
- [ ] An invalid RabbitMQ value fails startup with `load config: invalid configuration: ...` and exit code 1, without leaking the URL or any secret.
- [ ] A broker stop/start produces `rabbitmq consumer connection lost`, reconnect logs, and resumed consumption; no process exit.
- [ ] The opt-in live test (`RABBITMQ_LIVE_TEST=1 go test ./internal/rabbitmq -run TestLiveConsumer -v`) passes against the test broker and is skipped by default.
- [ ] The working tree is clean after the final commit, and the branch is pushed to `origin`.
