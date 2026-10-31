# Kafka Connectivity Observability & Truthful Readiness — Implementation Plan

**Goal:** Make Kafka connectivity observable and make `/health` truthful. Today the
consumer throws away every kafka-go client log (`discardLogger{}`), standard
kafka-go coordinator errors are routed to `Error`, and `/health` reports ready
before a single broker has ever been contacted. After this plan an operator can
tell "waiting for the coordinator" from "dead" from the logs alone, and a
container orchestrator cannot mistake an unreachable cluster for a healthy
service.

**Architecture:** Three contained changes in existing seams plus documentation.

1. `internal/kafka` gains a small `slog` adapter that implements kafka-go's
   `Logger` interface twice: client/trace output at `Debug` (`"kafka client"`),
   error output at `Warn` (`"kafka client error"`). The no-op `discardLogger`
   is deleted.
2. `internal/kafka` gains a `Probe` that dials the configured brokers and reads
   partition metadata for the configured topic, distinguishing "no broker
   reachable" from "topic missing" (`ErrTopicNotFound`). Its dial function and
   timings are injectable, so every test runs offline.
3. `cmd/server/main.go` starts `ready` at `false`, runs the probe in a
   background goroutine, and flips readiness `true` exactly once, on the first
   successful probe. Readiness is sticky; later loss of connectivity is logged
   (`"kafka connectivity lost"`) but never flips `/health` back to `503`.
4. `internal/config` rejects broker entries that are not `host:port`.
5. `docs/deployment.md` and `README.md` document the incident signatures, the
   broker-side causes, and the shipped-vs-deployed configuration drift.

**Tech Stack:** Go 1.25, `log/slog`, `net` (`SplitHostPort`), `context`,
`sync/atomic`, `net/http/httptest`, `github.com/segmentio/kafka-go v0.4.51`
(`ReaderConfig.Logger`, `ReaderConfig.ErrorLogger`, `kafka.Logger`,
`kafka.DialContext`, `Conn.ReadPartitions`, `kafka.UnknownTopicOrPartition`).

**Spec:** incident evidence and decisions D1–D4 below. Related docs:
`docs/stock-summary-gotosocial.md` (D1 Kafka input, D7 failure semantics),
`docs/deployment.md` (deployment mechanics), `AGENTS.md` (build/test/lint
contract).

Line references are against branch `fix/kafka-consumer-connectivity` at
`0cf26bb`. Re-locate them with `grep -rn` before editing.

## Global Constraints

- **No new third-party dependencies.** `kafka-go`, the AWS SDK, and the
  protobuf module are the whole dependency set; the probe uses
  `kafka.DialContext`/`Conn.ReadPartitions` from the library already present.
- **Tests stay offline.** No test may dial a broker, listen on a fixed port, or
  require Kafka. The probe's `dial` func, all timings, and the GoToSocial/S3
  seams stay injectable; `httptest` covers HTTP.
- **Build stays green after every task.** `go build ./cmd/server/`,
  `go test ./... -count=1`, `go vet ./...`, and empty `gofmt -l .` after each
  numbered task, not just at the end.
- **Do not change:** topic defaults, broker defaults, `S3_*` settings,
  GoToSocial publisher behavior, `AGENTS.md`, `Containerfile`,
  `deploy/quadlet/*` (comments only, if anything), `.env.podman.example`
  values, or `deploy/push.sh`.
- **Do not change the `/health` contract.** Path `/health`, port `:8080`, JSON
  bodies `{"status":"ok","version":"..."}` and
  `{"status":"starting","version":"..."}`, status `200`/`503`. No new
  endpoints, no new query parameters, no new env vars.
- **Secrets never logged.** `GOTOSOCIAL_TOKEN`, `S3_ACCESS_KEY`,
  `S3_SECRET_KEY`, and summary bodies stay out of every new log line. Broker
  addresses, topic names, and group names are not secrets and are logged.
- **Keep the diff minimal.** No opportunistic refactors of
  `internal/summarystore`, `internal/messenger`, `commitWithRetries`, or the
  consumer loop's existing backoff.
- **Conventional Commits** for every commit; verify with the Task 6 gate before
  claiming completion.

---

## Incident analysis

### Observed (host `10.0.2.100`, rootless podman quadlet)

```
{"level":"INFO","msg":"kafka consumer started","brokers":["192.168.1.48:9092","192.168.1.48:9094"],"topic":"stocker.gems.v1","group":"stocker-informer"}
{"level":"ERROR","msg":"Unable to establish connection to consumer group coordinator for group stocker-informer: read tcp 10.0.2.100:46254->192.168.1.48:9092: read: connection reset by peer"}
{"level":"ERROR","msg":"read tcp 10.0.2.100:46254->192.168.1.48:9092: read: connection reset by peer"}
{"level":"ERROR","msg":"Unable to establish connection to consumer group coordinator for group stocker-informer: [15] Group Coordinator Not Available: ..."}
```

### Root cause (established, not to be re-investigated)

- The application code and the broker list are **not** the cause.
- The target cluster at `192.168.1.48` restarted at ~`01:40`. Its 2-voter KRaft
  quorum lives entirely on one host and the brokers use ephemeral
  `/tmp/kafka-logs`, so a restart drops both the group coordinator's state and
  the log dirs.
- The consumer joined the group and reached `Stable` roughly **12 minutes**
  later: the service self-heals with no operator action.
- The **5-second** spacing between retries is kafka-go's
  `JoinGroupBackoff` (`ReaderConfig.JoinGroupBackoff`,
  `kafka-go@v0.4.51/reader.go:465-469`), not an application backoff.
- `[15] Group Coordinator Not Available` is `kafka.ErrGroupCoordinatorNotAvailable`
  — the coordinator for `__consumer_offsets` has not been elected/loaded yet.

Conclusion: these errors are **expected and transient** during a broker
restart. Nothing in this plan suppresses them; the plan makes them legible.

### Real deficiencies

| # | Deficiency | Location |
| --- | --- | --- |
| 1 | `Logger: discardLogger{}` discards every kafka-go client/coordinator/rebalance/join-assignment line | `internal/kafka/consumer.go:67` |
| 2 | `ErrorLogger` routes routine transient coordinator resets to `log.Error`, indistinguishable from fatal | `internal/kafka/consumer.go:68-70` |
| 3 | `ready.Store(true)` before any Kafka connectivity exists; `/health` returns `200` with zero reachable brokers | `cmd/server/main.go:76-77` |
| 4 | `KAFKA_BOOTSTRAP_SERVERS` accepts anything (`"broker"`, `"http://broker"`, empty port) | `internal/config/config.go:99-103,222-235` |
| 5 | No troubleshooting docs for these signatures or their broker-side causes | `README.md`, `docs/deployment.md` |

---

## Decision Log

### D1 — Structured Kafka client logging via a `slog` adapter

**Decision:** delete `discardLogger` (`internal/kafka/consumer.go:218-222`) and
wire two `kafka.Logger` implementations backed by `*slog.Logger`:

| kafka-go seam | slog level | `msg` (stable) | Attributes |
| --- | --- | --- | --- |
| `ReaderConfig.Logger` | `Debug` | `kafka client` | `component="kafka"`, `detail=<formatted library text>` |
| `ReaderConfig.ErrorLogger` | `Warn` | `kafka client error` | `component="kafka"`, `detail=<formatted library text>` |

Consumer-loop messages are unchanged: ordinary read retries stay `Warn`
(`read kafka message failed, backing off`), and the `>= maxConsecutiveErrors`
case stays `Error` (`exceeded max consecutive Kafka read failures`)
(`consumer.go:98-102`).

**Rationale:** transient coordinator errors must be visible but must not look
fatal. `Warn` for library errors plus `Error` only for the sustained-failure
threshold gives that separation without hiding anything. `Debug` for the
high-volume client trace keeps production stdout quiet while making the trace
available to anyone lowering the handler level locally.

**Consequence (documented, accepted):** `cmd/server` builds
`slog.NewJSONHandler(os.Stdout, nil)`, whose level is `Info`; `Debug` records
are dropped. The operator-facing signal is therefore the `Warn` `kafka client
error` line (which is where "Unable to establish connection to consumer group
coordinator" and `[15]` surface) plus the readiness probe from D2. This plan
deliberately does **not** add a `LOG_LEVEL` variable; that is new configuration
surface beyond the incident fix.

### D2 — Truthful, sticky readiness backed by an injectable probe

**Decision:** add `internal/kafka/probe.go` with:

```go
var ErrTopicNotFound = errors.New("kafka topic not found")

type metadataConn interface {
	ReadPartitions(topics ...string) ([]kafka.Partition, error)
	Close() error
}

type dialFunc func(ctx context.Context, addr string) (metadataConn, error)

type Probe struct {
	brokers []string
	topic   string
	log     *slog.Logger
	dial    dialFunc

	attemptTimeout time.Duration // default 5s
	retryInitial   time.Duration // default 1s
	retryMax       time.Duration // default 30s
	healthyEvery   time.Duration // default 60s
}

func NewProbe(brokers []string, topic string, log *slog.Logger) *Probe
func (p *Probe) Check(ctx context.Context) error
func (p *Probe) Run(ctx context.Context, onReady func())
```

`Check` tries each broker in order until one answers with `len(partitions) > 0`
for `p.topic`:

- broker reachable and topic known → `nil` (the only success);
- broker reachable, `ReadPartitions` returns `kafka.UnknownTopicOrPartition`
  (or zero partitions) → record `fmt.Errorf("%w: %q", ErrTopicNotFound, p.topic)`
  and keep trying the remaining brokers;
- dial or read failure → record `fmt.Errorf("%s: %w", broker, err)` and keep
  trying;
- nothing succeeded → return the recorded `ErrTopicNotFound` error if any
  broker answered, otherwise the last transport error; `len(p.brokers) == 0`
  returns a plain error.

`Run` is the supervisor `cmd/server` starts in a goroutine. It is the only
place that logs connectivity state:

| Situation | Level | `msg` | Attributes |
| --- | --- | --- | --- |
| first successful `Check` | `Info` | `kafka connectivity verified` | `topic`, `brokers` |
| failed `Check` before ready, attempt 1 and every 10th attempt | `Warn` | `kafka connectivity probe failed` | `topic`, `brokers`, `attempt`, `retry_in`, `error` |
| failed `Check` returning `ErrTopicNotFound` (same cadence) | `Warn` | `kafka topic not available` | `topic`, `brokers`, `attempt`, `retry_in`, `error` |
| other pre-ready attempts | `Debug` | same messages | same attributes |
| first failure **after** ready | `Warn` | `kafka connectivity lost` | `topic`, `error` |
| subsequent failures after ready | `Debug` | `kafka connectivity probe failed` | `topic`, `error` |
| first success after a post-ready failure | `Info` | `kafka connectivity restored` | `topic` |

Timing: backoff `retryInitial` → ×2 → capped at `retryMax`; **unbounded
attempts** until `ctx` is cancelled (the delay is bounded, the retry count is
not). After success, re-probe every `healthyEvery`. Every wait is a
`select` on `ctx.Done()` and a timer, so shutdown is immediate.

**Readiness is sticky:** `onReady` is called at most once, on the first
success. Afterwards the flag is never cleared — a broker restart must not
trigger a container restart loop. Post-ready failures only produce the
`kafka connectivity lost` / `kafka connectivity restored` transition pair.

`cmd/server/main.go` wiring:

```go
ready := new(atomic.Bool) // false: no Kafka connectivity verified yet
probe := kafka.NewProbe(cfg.KafkaServers(), cfg.KafkaTopic, log)
log.Info("kafka connectivity probe started", "brokers", cfg.KafkaServers(), "topic", cfg.KafkaTopic)
go probe.Run(ctx, func() { ready.Store(true) })
```

The `/health` handler body is unchanged. `newHealthMux(ready *atomic.Bool)
http.Handler` is extracted from `run()` so both states are unit-testable.

**Alternatives rejected:**

| Option | Verdict |
|---|---|
| Flip readiness on the consumer's first successful `ReadMessage` | Rejected. A quiet topic yields no messages, so `/health` would stay `503` forever on a perfectly healthy cluster. |
| Use `kafka.Dialer.LookupPartitions`/`ReadPartitions` inline in `cmd/server` | Rejected. Untestable offline and mixes transport concerns into the composition root. |
| Non-sticky readiness (flip back to `503` on probe failure) | Rejected. Guarantees a restart loop during every broker restart — the exact incident we are hardening against. |
| Add `/readyz` and keep `/health` optimistic | Rejected by the brief: keep one endpoint and the existing JSON contract. |
| Add a `LOG_LEVEL` env var to expose Debug | Rejected. New configuration surface; out of scope for the incident fix. |

**Known limitation to document:** brokers with `auto.create.topics.enable=true`
will create a mistyped topic on the probe's metadata request (kafka-go sends
`AllowAutoTopicCreation=true`, `kafka-go@v0.4.51/conn.go:987`). The probe then
reports success while no producer writes to the created topic, so the consumer
waits silently. That is why D4 documents checking `KAFKA_TOPIC` and
`kafka-consumer-groups.sh --describe` rather than relying on readiness alone.

### D3 — Validate broker addresses as `host:port`

**Decision:** in `config.Load`, after the existing non-empty checks, validate
every entry returned by `cfg.KafkaServers()` with `net.SplitHostPort` plus a
numeric port range check (`1..65535`). Preserve splitting on `,`, trimming, and
dropping empty entries exactly as today (`config.go:222-235`).

Exact messages (all prefixed by `KAFKA_BOOTSTRAP_SERVERS`, aggregated by the
existing `add`/`invalid configuration:` mechanism, exit code `1`):

| Input | Message fragment |
| --- | --- |
| `localhost` | `entry "localhost" must be host:port` |
| `:9092` | `entry ":9092" must include a host and a port` |
| `localhost:` | `entry "localhost:" must include a host and a port` |
| `localhost:abc` | `entry "localhost:abc" has an invalid port (must be 1-65535)` |
| `localhost:0`, `localhost:70000` | `entry "..." has an invalid port (must be 1-65535)` |

**Rationale:** a typo'd bootstrap list currently fails at dial time with a
`Warn` retry loop that looks identical to a broker outage. Failing at startup
with a named variable is cheaper to diagnose and preserves the documented
`load config: invalid configuration: ...` / exit `1` contract.

Bracketed IPv6 (`[::1]:9092`) is accepted; bare IPv6 without brackets is
rejected with the `host:port` message. No DNS resolution happens at config
time (entries may be names resolved later, and config must stay offline).

### D4 — Documentation of signatures, causes, and drift

**Decision:** append a `## Troubleshooting Kafka connectivity` section to
`docs/deployment.md` and update the operational sections of `README.md`. The
material is limited to the facts already established:

- the three signatures (`connection reset by peer`,
  `[15] Group Coordinator Not Available`, silent "connected but no messages");
- for each: meaning, transient-or-not, and a concrete operator checklist;
- the broker-side causes: single-host 2-voter KRaft quorum, ephemeral
  `/tmp/kafka-logs`, `offsets.topic.replication.factor` greater than the broker
  count, topic name mismatch;
- an explicit warning that a 2-voter quorum on one host has zero fault
  tolerance, so **every** broker restart produces exactly these errors;
- the shipped-vs-deployed drift, marked as operator-side and **not** a
  repository change: shipped `deploy/quadlet/.env.podman.example` uses
  `KAFKA_BOOTSTRAP_SERVERS=192.168.1.71:9092,192.168.1.75:9092`,
  `KAFKA_TOPIC=stock.summary.available.v1`, `S3_ENDPOINT=http://192.168.1.31:3900`;
  the deployed operator instance uses
  `192.168.1.48:9092,192.168.1.48:9094`, topic `stocker.gems.v1`, and
  `S3_ENDPOINT=http://192.168.1.31:30188`;
- the auto-create hazard: a wrong/auto-created topic exists but is empty, the
  consumer logs nothing and waits forever.

No product names, hosts, or cluster details beyond those listed may be
introduced.

---

## Target File Inventory

**Add:**

- `internal/kafka/logging.go` — `slog` adapters for `kafka.Logger`.
- `internal/kafka/logging_test.go` — level/message/attribute assertions.
- `internal/kafka/probe.go` — `Probe`, `ErrTopicNotFound`, default dialer.
- `internal/kafka/probe_test.go` — offline probe and supervisor tests.
- `cmd/server/main_test.go` — `/health` 503-before / 200-after tests.

**Modify:**

- `internal/kafka/consumer.go` — wire adapters; delete `discardLogger`.
- `internal/config/config.go` — broker `host:port` validation (`net` import).
- `internal/config/config_test.go` — broker validation table.
- `cmd/server/main.go` — false-initialized readiness, probe goroutine,
  extracted `newHealthMux`, probe-start log line.
- `README.md` — log-message list, Kafka behaviour pointer, health endpoint,
  known limitations, `KAFKA_BOOTSTRAP_SERVERS` validation note.
- `docs/deployment.md` — troubleshooting appendix.

**Delete:** none (the only removal is the `discardLogger` type inside
`consumer.go`).

**Unchanged:** `internal/summarystore/*`, `internal/messenger/*`,
`AGENTS.md`, `Containerfile`, `deploy/push.sh`, `deploy/quadlet/*`,
`go.mod`, `go.sum`.

---

## Implementation Tasks

Ordering rule: Tasks 1–3 are additive and independent; Task 4 wires the probe
into `main`; Task 5 documents the new behavior; Task 6 is the gate. The tree
builds and all tests pass after each task. Commit after each task with the
message given.

### Task 1: Replace `discardLogger` with structured `slog` adapters

**Files:**
- Add: `internal/kafka/logging.go`
- Add: `internal/kafka/logging_test.go`
- Modify: `internal/kafka/consumer.go`

**Behavior:**
- `logging.go` declares `type clientLogger struct { log *slog.Logger; level
  slog.Level; msg string }` with `Printf(format string, args ...any)` calling
  `l.log.Log(context.Background(), l.level, l.msg, "component", "kafka",
  "detail", fmt.Sprintf(format, args...))`.
- Constructors: `newClientLogger(log *slog.Logger) clientLogger` →
  `{level: slog.LevelDebug, msg: "kafka client"}`; `newErrorLogger(log
  *slog.Logger) clientLogger` → `{level: slog.LevelWarn, msg: "kafka client
  error"}`.
- Compile-time assertions `var _ kafka.Logger = clientLogger{}` (both
  constructors return `clientLogger`, which satisfies `kafka.Logger`).
- `consumer.go`: `Logger: newClientLogger(c.log)`,
  `ErrorLogger: newErrorLogger(c.log)`; delete `discardLogger` and its `Printf`
  (`consumer.go:218-222`). Keep `ReadBatchTimeout`, `MaxBytes`,
  `StartOffset`, `GroupID`, the consumer loop, and all loop-level log
  levels/messages byte-identical.
- `fmt` stays imported in `consumer.go` (still used by `decodeStock`).

**Tests (`internal/kafka/logging_test.go`):**
1. `TestClientLoggerDebug` — handler is
   `slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})`;
   `newClientLogger(log).Printf("joining group %s", "g")`; decode the single
   JSON line; assert `level=="DEBUG"`, `msg=="kafka client"`,
   `component=="kafka"`, `detail=="joining group g"`.
2. `TestErrorLoggerWarn` — same handler; `newErrorLogger(log).Printf("read
   tcp: %v", errors.New("connection reset by peer"))`; assert `level=="WARN"`,
   `msg=="kafka client error"`, `detail` contains `connection reset by peer`.
3. `TestClientLoggerNoArgs` — `Printf("hello")` yields `detail=="hello"` (no
   `%!(EXTRA` artifacts).

**Acceptance:**
- `grep -rn 'discardLogger' internal/ cmd/` → no matches.
- `grep -n 'newErrorLogger\|newClientLogger' internal/kafka/consumer.go` →
  one wiring line each.
- `go test ./internal/kafka -count=1` passes; `go vet ./internal/kafka` clean.

**Commit:**
```bash
git add internal/kafka/logging.go internal/kafka/logging_test.go internal/kafka/consumer.go
git commit -m "feat(kafka): log client and coordinator messages through slog"
```

### Task 2: Reject broker entries that are not `host:port`

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`

**Behavior:**
- Add `net` to imports (`strconv` is already imported).
- Replace the `KAFKA_BOOTSTRAP_SERVERS` validation block
  (`config.go:99-103`) with: empty → existing `is required and must be
  non-empty` message; `len(cfg.KafkaServers()) == 0` → existing `must contain
  at least one non-empty broker` message; otherwise validate each entry.
- Add `func validateBroker(entry string) error`:
  - `host, port, err := net.SplitHostPort(entry)`; on error or empty `host` or
    empty `port` → `fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS entry %q must be
    host:port", entry)` for the `SplitHostPort` failure and `fmt.Errorf(
    "KAFKA_BOOTSTRAP_SERVERS entry %q must include a host and a port", entry)`
    for empty parts;
  - `strconv.Atoi(port)` error or `n < 1 || n > 65535` →
    `fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS entry %q has an invalid port (must be
    1-65535)", entry)`.
- `KafkaServers()` is unchanged (split, trim, drop empties). Errors go through
  the existing `add(...)` aggregation, so multiple problems appear in one
  `invalid configuration: ...` error and the process exits `1`.
- No DNS resolution, no defaulting to `:9092`, no rejection of hostnames.

**Tests (`internal/config/config_test.go`):**
1. `TestLoadBrokerAddressValidation` — table:
   - valid: `localhost:9092`, `192.168.1.48:9094`, `[::1]:9092`,
     `kafka.internal:9092`, `a:1, b:2,,` → `Load()` returns `nil` error.
   - invalid: `localhost`, `:9092`, `localhost:`, `localhost:abc`,
     `localhost:0`, `localhost:70000`, `http://localhost:9092` → error
     contains `KAFKA_BOOTSTRAP_SERVERS`; for the first, second, third and last
     cases also contains `host:port`; for the port cases contains
     `invalid port`.
2. `TestLoadInvalidBrokerAggregates` — `KAFKA_BOOTSTRAP_SERVERS=bogus,also-bad`
   plus `KAFKA_TOPIC=""` → single error containing `invalid configuration:`
   and both variable names.
3. Keep `TestLoadBrokers` passing unchanged (it already asserts split/trim/drop
   behavior with `a:1, b:2,,`).

**Acceptance:**
- `go test ./internal/config -count=1` passes.
- `go build ./cmd/server/` succeeds.
- `KAFKA_BOOTSTRAP_SERVERS=localhost ./stocker-informer` prints
  `{"level":"ERROR","msg":"fatal error","error":"load config: invalid
  configuration: KAFKA_BOOTSTRAP_SERVERS entry \"localhost\" must be host:port"}`
  and exits `1` (manual only; no test needs a broker).

**Commit:**
```bash
git add internal/config/config.go internal/config/config_test.go
git commit -m "feat(config): require host:port for Kafka broker entries"
```

### Task 3: Add the Kafka connectivity probe with injectable dial and timings

**Files:**
- Add: `internal/kafka/probe.go`
- Add: `internal/kafka/probe_test.go`

**Behavior:**
- Types and defaults exactly as in D2. `NewProbe` sets `attemptTimeout=5s`,
  `retryInitial=1s`, `retryMax=30s`, `healthyEvery=60s`, and
  `dial = func(ctx context.Context, addr string) (metadataConn, error) {
  return kafka.DialContext(ctx, "tcp", addr) }` (`*kafka.Conn` satisfies
  `metadataConn`).
- `Check` is pure apart from dialing: it applies `attemptTimeout` on top of the
  caller's context, dials brokers in order, calls
  `conn.ReadPartitions(p.topic)`, always closes the connection
  (`defer conn.Close()` inside the per-broker scope, close error ignored), and
  classifies as in D2. Zero partitions counts as `ErrTopicNotFound`.
  `errors.Is(err, kafka.UnknownTopicOrPartition)` is the topic-missing signal
  (`kafka-go@v0.4.51/conn.go:1054-1061` returns the bare `kafka.Error` value).
  `Check` logs nothing.
- `Run` implements the state machine and log table from D2:
  - calls `onReady` at most once (skip if `nil`);
  - attempts counter and `retry_in` reflect the delay actually slept;
  - `Warn` on attempt 1 and when `attempt%10 == 0`, `Debug` otherwise before
    ready;
  - after ready, the first failure logs `kafka connectivity lost` and sets a
    degraded flag; later failures stay `Debug` until a success logs
    `kafka connectivity restored` and clears it;
  - returns when `ctx` is cancelled, with no error value (it is a supervisor,
    not a request).
- Add `func sleepCtx(ctx context.Context, d time.Duration) bool` (returns
  `false` if the context ended first) or an equivalent `select` inline; keep
  one helper, used by every wait.
- `probe.go` must not import `internal/config` or `internal/messenger`; it
  depends only on `context`, `errors`, `fmt`, `log/slog`, `time`, and
  `github.com/segmentio/kafka-go`.

**Tests (`internal/kafka/probe_test.go`, all offline):**
1. `TestProbeCheckSuccess` — fake `metadataConn` returns two partitions;
   assert `Check` returns `nil`, `ReadPartitions` received `"topic"`, and
   `Close` was called.
2. `TestProbeCheckTopicNotFound` — fake returns
   `kafka.UnknownTopicOrPartition`; assert
   `errors.Is(err, ErrTopicNotFound)` and the message contains the topic name.
3. `TestProbeCheckZeroPartitions` — fake returns `nil, nil`; same
   `ErrTopicNotFound` assertion.
4. `TestProbeCheckNoBrokerReachable` — dial fails for every broker; assert the
   error is **not** `ErrTopicNotFound` and names each broker.
5. `TestProbeCheckFallsBackToBroker` — first broker dials but reports topic
   missing, second broker returns partitions; assert `nil` (a partial metadata
   view is not a topic-missing verdict while another broker knows the topic).
6. `TestProbeRunFlipsReadyOnce` — `retryInitial=healthyEvery=1ms`, fake always
   succeeds; run in a goroutine, wait for an `onReady` channel signal, cancel;
   assert exactly one `onReady` call and a captured log containing
   `kafka connectivity verified` at `INFO`.
7. `TestProbeRunRetriesThenReady` — first two `Check`s return
   `ErrTopicNotFound`, third succeeds; assert one `onReady`, at least one
   `WARN` record with `kafka topic not available`, and the success record.
8. `TestProbeRunReadinessSticky` — success, then failure, then success, then
   cancel (`healthyEvery=1ms`); assert one `onReady`, one
   `kafka connectivity lost` and one `kafka connectivity restored` record.
9. `TestProbeRunStopsOnCancel` — dial always fails, `retryInitial=1ms`; cancel
   and assert `Run` returns within `2s` (done channel + `select` timeout).

Test helpers: `fakeConn{partitions []kafka.Partition, err error, closed bool}`,
`fakeDialer{...}` returning scripted results, and a `newCapturingLogger()`
(`slog.NewJSONHandler(&bytes.Buffer, &slog.HandlerOptions{Level:
slog.LevelDebug})`) decoding JSON lines into a slice.

**Acceptance:**
- `go test ./internal/kafka -count=1` passes with no network access.
- `grep -rn 'kafka.DialContext' internal/kafka/` matches only `probe.go`.
- `go vet ./internal/kafka` and `gofmt -l .` clean.
- `cmd/server` still builds unchanged (probe not yet wired).

**Commit:**
```bash
git add internal/kafka/probe.go internal/kafka/probe_test.go
git commit -m "feat(kafka): add injectable connectivity probe for topic metadata"
```

### Task 4: Make readiness truthful in `cmd/server`

**Files:**
- Modify: `cmd/server/main.go`
- Add: `cmd/server/main_test.go`

**Behavior:**
- `ready := new(atomic.Bool)` with **no** `ready.Store(true)` at startup.
- After `consumer := kafka.New(...)`:
  ```go
  probe := kafka.NewProbe(cfg.KafkaServers(), cfg.KafkaTopic, log)
  log.Info("kafka connectivity probe started", "brokers", cfg.KafkaServers(), "topic", cfg.KafkaTopic)
  go probe.Run(ctx, func() { ready.Store(true) })
  ```
  The probe goroutine starts before `consumer.Run(ctx)` and is independent of
  it; `ctx` cancellation (SIGINT/SIGTERM) stops both.
- Extract the handler into
  `func newHealthMux(ready *atomic.Bool) http.Handler` in `main.go`, containing
  the current `ServeMux` and handler verbatim:
  - ready → `200`, `{"status":"ok","version":"%s"}\n`;
  - not ready → `503`, `{"status":"starting","version":"%s"}\n`;
  - `Content-Type: application/json` on both paths.
  `run()` uses `server := &http.Server{Addr: ":8080", Handler:
  newHealthMux(ready)}`. Nothing else about the server changes (still a
  goroutine, still `Warn` on `ListenAndServe` failure, still non-fatal).
- No consumer/publisher/store changes, no new env vars, no new endpoints.

**Tests (`cmd/server/main_test.go`):**
1. `TestHealthNotReady` — `httptest.NewRequest("GET","/health")` against
   `newHealthMux(new(atomic.Bool))`; assert `503`, `Content-Type:
   application/json`, body exactly
   `{"status":"starting","version":"dev"}` (accept `Version` variable).
2. `TestHealthReady` — `ready := new(atomic.Bool); ready.Store(true)`; assert
   `200` and body exactly `{"status":"ok","version":"dev"}`.
3. `TestHealthOtherPathNotFound` — `GET /nope` → `404` (proves the mux is
   unchanged).

**Acceptance:**
- `go build ./cmd/server/` and `go test ./cmd/server -count=1` pass.
- `grep -n 'ready.Store' cmd/server/main.go` → exactly one match, inside the
  `probe.Run` callback.
- Manual: with `KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:1` the process stays up,
  `/health` returns `503` `{"status":"starting",...}`, and the log shows
  `kafka connectivity probe started` then repeated
  `kafka connectivity probe failed` `WARN` lines at a widening cadence. With a
  reachable cluster, `/health` flips to `200` `{"status":"ok",...}` and logs
  `kafka connectivity verified`. Manual only; no test requires a broker.

**Commit:**
```bash
git add cmd/server/main.go cmd/server/main_test.go
git commit -m "feat(server): report ready only after Kafka connectivity is verified"
```

### Task 5: Document signatures, causes, and configuration drift

**Files:**
- Modify: `docs/deployment.md`
- Modify: `README.md`

**Behavior — `docs/deployment.md`:** append a top-level
`## Troubleshooting Kafka connectivity` section after the existing final
`##`‑level content (end of file). Keep it operational and concise; include:

1. **Log signatures table** with the exact strings and level after this plan:
   | Signature | Meaning | Transient? |
   | --- | --- | --- |
   | `kafka client error` with `detail` containing `read tcp ...: read:
     connection reset by peer` | Broker closed the connection mid-request (restart, LB, or broker death) | Yes during a broker restart; sustained → broker/network fault |
   | `kafka client error` with `detail` containing `[15] Group Coordinator Not
     Available` | `__consumer_offsets` has no loaded coordinator yet for the group | Yes; clears once the coordinator is elected and the group rejoins |
   | `kafka consumer started` then no `summary published on GoToSocial` and no
     errors | Consumer waits; topic/group mismatch or an empty topic | **No** — configuration problem |
2. **What the 5-second spacing means:** kafka-go's `JoinGroupBackoff`; retries
   are automatic, no operator action needed.
3. **Broker-side causes checklist:** broker process up; KRaft quorum voters
   reachable; `offsets.topic.replication.factor` ≤ broker count; a
   `__consumer_offsets` topic exists with all partitions online; `log.dirs` is
   persistent (not `/tmp`, which discards logs and group state on restart);
   `KAFKA_TOPIC` matches the topic the producer writes exactly (case-sensitive).
4. **Verification commands (generic kafka tooling, no invented names):**
   `kafka-topics.sh --bootstrap-server <brokers> --describe --topic
   <KAFKA_TOPIC>`, `kafka-consumer-groups.sh --bootstrap-server <brokers>
   --describe --group <KAFKA_CONSUMER_GROUP>` (expect `Stable` with a partition
   assignment), and `kafka-console-consumer.sh --bootstrap-server <brokers>
   --topic <KAFKA_TOPIC> --max-messages 1` to confirm records exist.
5. **Zero-fault-tolerance warning:** a 2-voter KRaft quorum on a single host
   survives no broker failure; any restart of that host produces exactly these
   errors until the quorum and coordinator recover (observed: ~12 minutes in
   the incident). Recommends a third voter on a separate host and persistent
   `log.dirs` if this matters operationally.
6. **Auto-create hazard:** with `auto.create.topics.enable=true`, a mistyped
   `KAFKA_TOPIC` is created empty; the consumer is "connected" but receives
   nothing, and readiness can be `200`. Always confirm the topic name and that
   the producer writes to it.
7. **Shipped vs deployed drift (operator-side, not a repo defect):** the
   shipped example `deploy/quadlet/.env.podman.example` uses
   `192.168.1.71:9092,192.168.1.75:9092` and
   `KAFKA_TOPIC=stock.summary.available.v1`; the deployed operator instance
   uses `192.168.1.48:9092,192.168.1.48:9094`, topic `stocker.gems.v1`, and
   `S3_ENDPOINT=http://192.168.1.31:30188`. Shipped defaults are **not**
   changed by this plan; the operator's env file is authoritative. Verify with
   the `configuration loaded` log line (`kafka_topic`, `s3_endpoint`) and the
   `kafka consumer started` line (`brokers`, `topic`, `group`).
8. **Readiness semantics after this plan:** `/health` is `503` until the probe
   first verifies metadata for `KAFKA_TOPIC`, then sticky `200`; a later loss
   logs `kafka connectivity lost` without flipping the endpoint.

**Behavior — `README.md`:**
- `## Environment Variables` → `KAFKA_BOOTSTRAP_SERVERS` row: append "Each
  entry must be `host:port` (bracketed IPv6 allowed), otherwise startup fails
  with `invalid configuration`."
- `### Logging` → representative log lines: add `kafka connectivity probe
  started`, `kafka connectivity verified`, `kafka connectivity probe failed`,
  `kafka topic not available`, `kafka connectivity lost`, `kafka connectivity
  restored`, `kafka client error`. Add one bullet: kafka-go client/trace output
  is emitted at `Debug` (dropped by the default `Info` handler) while
  kafka-go errors are `Warn` (`kafka client error`).
- `### Kafka consumer behaviour` → add: coordinator/join errors are expected
  during a broker restart, are logged as `Warn`, and are retried automatically;
  link to `docs/deployment.md#troubleshooting-kafka-connectivity`.
- `### Health endpoint` → replace the "optimistic readiness" caveat with: `503`
  /`{"status":"starting"}` until the first successful Kafka probe, then `200`
  /`{"status":"ok"}`; sticky thereafter; post-ready failures log
  `kafka connectivity lost` but do not flip the endpoint; keep the `:8080`
  not-published caveat.
- `### How to verify it's working` → step 1: add that `/health` remains `503`
  until the probe verifies connectivity, and that
  `kafka connectivity verified` precedes readiness.
- `## Known limitations` → replace the optimistic-readiness bullet with a
  sticky-readiness bullet (a dead cluster after startup keeps `/health` at
  `200`; only the logs show the loss, by design to avoid restart loops).

**Acceptance:**
- `docs/deployment.md` contains a `## Troubleshooting Kafka connectivity`
  section covering all three signatures, the 2-voter warning, the auto-create
  hazard, and the shipped-vs-deployed drift values verbatim.
- `README.md` contains no claim that readiness is optimistic; it links to the
  troubleshooting section; the broker row documents `host:port`.
- Neither file changes any default value, host, or topic in the repository.
- Markdown is coherent: heading anchors resolve, tables render, and every task
  is fully specified with no unresolved placeholders.

**Commit:**
```bash
git add README.md docs/deployment.md
git commit -m "docs: troubleshooting for Kafka coordinator errors and readiness"
```

### Task 6: Verification gate

**Files:** verify only (no edits unless a check fails).

**Commands and expected results:**

```bash
gofmt -l .                      # empty output
go build ./cmd/server/          # exit 0
go vet ./...                    # exit 0
go test ./... -count=1          # every package ok, no skips needed
go test -race ./... -count=1    # optional; no Makefile race target exists
grep -rn 'discardLogger' internal/ cmd/          # no matches
grep -rn 'kafka.DialContext' internal/           # only internal/kafka/probe.go
grep -n 'ready.Store' cmd/server/main.go         # exactly one, inside probe.Run callback
git diff --stat origin/main...HEAD               # only the files in the inventory
```

**Manual smoke (optional, requires a broker; not part of the gate):**
1. Run with an unreachable broker (`KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:1`) →
   `/health` `503`, `kafka connectivity probe failed` warnings, no crash.
2. Point at the cluster → `kafka connectivity verified`, `/health` `200`.
3. Stop/restart the cluster → `kafka client error` warnings continue,
   `/health` stays `200`, and `kafka connectivity lost` appears once.

**Acceptance:**
- All commands above produce the stated result.
- No new dependencies in `go.mod`/`go.sum` (`git diff origin/main...HEAD --
  go.mod go.sum` is empty).
- No changes outside the target file inventory.

**Commit:** none (verification only); if fixes were required, commit them with
a `fix:`/`test:` subject describing the correction.

---

## Whole-change acceptance criteria

1. **Client logs are no longer discarded:** `discardLogger` does not exist;
   `ReaderConfig.Logger` → `Debug` `kafka client`, `ReaderConfig.ErrorLogger`
   → `Warn` `kafka client error`, both with `component="kafka"` and `detail`.
2. **Levels are honest:** transient coordinator resets appear as `Warn`, and
   `Error` is reserved for the sustained consumer-loop failure threshold.
3. **Readiness is truthful:** `/health` returns `503` until the first
   successful probe and `200` afterwards; the JSON bodies and status codes are
   unchanged; readiness never flips back.
4. **Probe is diagnosable:** `ErrTopicNotFound` distinguishes "topic missing"
   from "no broker reachable", and the two produce different `Warn` messages.
5. **Probe is testable offline:** dial and all timings are injectable; no test
   contacts a broker, and `go test ./...` passes without network.
6. **Config is strict:** non-`host:port` broker entries fail startup with
   `load config: invalid configuration: ...` and exit `1`, while split/trim/
   drop-empty behavior is preserved.
7. **Docs are operational:** `docs/deployment.md` explains the three
   signatures, the 2-voter quorum and ephemeral-`log.dirs` causes, the
   auto-create hazard, and the operator-side shipped-vs-deployed drift;
   `README.md` points at it and no longer claims optimistic readiness.
8. **Scope respected:** no changes to topics, defaults, S3, GoToSocial,
   `AGENTS.md`, `Containerfile`, `deploy/quadlet/*`, or dependencies; `gofmt
   -l .` empty and `go build`, `go vet`, `go test` clean.

## Risks

| Risk | Mitigation |
| --- | --- |
| Probe log spam during an outage | attempt-gated `Warn` (1st, every 10th) plus backoff capped at 30s; post-ready failures log once per transition |
| Sticky readiness hides a dead cluster | documented explicitly in README known limitations and the deployment troubleshooting section; `kafka connectivity lost` is `Warn` |
| Probe itself triggers topic auto-creation | same metadata path kafka-go already uses; documented auto-create hazard with an explicit verification checklist |
| Extra broker connections per probe | one short-lived connection per broker per probe, 5s attempt timeout, 60s cadence when healthy |
| Probe blocks shutdown | every wait is context-aware; `Run` returns on cancellation |

## Rollback

Each task is an independent, stateless commit; revert in reverse order with
`git revert <sha>`.

- Reverting Task 4 restores optimistic readiness (`/health` `200` at startup).
- Reverting Task 3 alone is not possible after Task 4 (compile error) — revert
  4 first, then 3.
- Reverting Tasks 1, 2, and 5 restores the previous logging, permissive broker
  parsing, and documentation independently.
- No broker-side, S3, or GoToSocial state is touched by any task.

## Out of scope

- Changes to the Kafka cluster (quorum size, `log.dirs`, replication factors,
  topic creation) — broker-side operator work, documented only.
- A `LOG_LEVEL` variable, log sampling framework, or metrics/Prometheus
  endpoint.
- New `/readyz`, `/livez`, or `/metrics` endpoints.
- GoToSocial publisher retry/queueing behavior, summary store behavior,
  at-least-once semantics.
- Deployment changes (`Containerfile`, quadlets, `push.sh`) and environment
  default changes.
- Changing `KAFKA_TOPIC`, broker defaults, or the `stocker.gems.v1` operator
  configuration.
