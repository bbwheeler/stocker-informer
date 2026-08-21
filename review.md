# Stocker-Informer Review: Improvements, Fixes & Simplifications

## Bugs & Correctness

**1. Consumer hangs on shutdown (consumer.go:62)**
`Run()` returns `ctx.Err()` on graceful shutdown, which is `context.Canceled`. In `main.go:52` the code treats any non-nil error as fatal unless the context errored — so it works, but only accidentally because of that check. If someone removes that guard, the daemon exits with "canceled" as a fatal error. Return `nil` on `ctx.Err() != nil` to make shutdown behavior explicit and correct.

**2. Formatted message text can exceed GoToSocial character limit (stock_event_formatter.go:16-43)**
GoToSocial `api/v1/statuses` accepts a maximum of **500 characters**. The formatter can produce messages that exceed this (long company names + metadata). There's no truncation or safeguard — the request will fail silently or with a misleading API error.

**3. Empty ChangePercent comparison is wrong (stock_event_formatter.go:30)**
The condition `event.ChangePercent > 0` does not account for the case where `event.ChangePercent == 0`. That's fine logicologically, but the formatter has no test coverage to verify this path, so a zero-change stock produces a bare price line with no percentage — which is arguably misleading (it doesn't indicate "no change").

**4. `in_reply_to_id` set to empty string unnecessarily (gotosocial_publisher.go:45)**
Setting `"in_reply_to_id"` to `""` in the form body sends an empty value to the GoToSocial API. The field should either be omitted from the form entirely or set conditionally only when replying. Sending an empty `in_reply_to_id` may cause unexpected behavior on some API versions.

**5. Response body not read on non-2xx status (gotosocial_publisher.go:61)**
On error responses, `resp.Body` is closed via `defer` but the body is never read — this can leak TCP connections in long-running daemons because Go's HTTP transport waits for body reads before reusing idle connections. Add a `io.Copy(io.Discard, resp.Body)` or at minimum `ioutil.ReadAll` before `Close()` on error paths.

---

## Reliability & Error Handling

**6. No retry/backoff on publish failures (gotosocial_publisher.go:39)**
Every failed `Publish()` call is logged and the message consumer continues to the next Kafka message — meaning that stock update is effectively dropped forever. For a monitoring/alerting-style service, transient failures should be retried with exponential backoff before being abandoned.

**7. `CommitMessages` errors are ignored (consumer.go:76, 83, 92)**
Failed offset commits mean that on restart the same messages will be reprocessed. Worse, if the process dies between commit and publish, messages can be lost (delivered to GoToSocial but never committed). The retryable/error distinction is important here — transient network errors should trigger reprocessing, while decode failures can safely skip.

**8. Consumer loop has no backoff on persistent read failures (consumer.go:65-70)**
If Kafka becomes unavailable, the consumer will spin at full CPU in a tight loop calling `reader.ReadMessage()` without any sleep or exponential backoff between failures. Add a growing delay (e.g., `time.Sleep(...)` with backoff) when errors persist.

**9. No health check or readiness endpoint (cmd/server/main.go)**
For container deployments, there's no way for Podman/systemd to know if the consumer actually connected to Kafka. A `/health` HTTP endpoint or readiness probe that reports Kafka connection status would help operational tooling detect dead pods.

**10. `maxConsecutiveErrors` not tracked anywhere (consumer.go)**
If errors are persistent (e.g., GoToSocial is down, Kafka is unreachable), the loop runs at full speed indefinitely. A counter with threshold-based backoff or alert logging after N consecutive errors would improve operational visibility.

---

## Architecture & Design

**11. `DESIGN_CHANGE_KAFKA.md` should be removed (DESIGN_CHANGE_KAFKA.md)**
This is a design change document that was likely meant to guide the migration but has been committed and left in the repo. It documents what *was* changed from gRPC polling to Kafka, which is historical cruft not useful in the final codebase. Git history preserves this anyway.

**12. No test files exist anywhere in the project**
There are no `_test.go` files at all — `config`, `kafka`, `messenger` packages have zero tests. A minimal test suite should include: config validation (missing env vars), formatter output for various StockEvent inputs, and mock-publisher behavior in consumer flow.

**13. Config struct stores raw string while parsing method exists separately (config.go:9-17 + 54-67)**
The `KafkaServers()` method parses the comma-separated string into a slice. Consider either accepting the parsed slice directly in the Config struct or documenting that `KafkaBootstrapServers` is an internal representation, not a field users should access directly. The pattern creates a hidden dependency on `KafkaServers()` being called for the config to be useful.

**14. Messenger layer does too much (gotosocial_publisher.go)**
The `GoToSocialPublisher` embeds HTTP transport configuration (`MaxIdleConns`, TLS timeouts, etc.) that could drift out of sync with the shared `http.Client` defaults in Go. Consider making these configurable or using `http.DefaultTransport` with a single timeout override.

**15. No graceful consumer drain before shutdown (consumer.go:39)**
On SIGTERM, the consumer closes the reader immediately when context is canceled. If a message is mid-processing (e.g., `Publish()` is in-flight), the context cancellation will abort it via `http.NewRequestWithContext`. Add logic to let in-flight publishes complete while rejecting new messages during shutdown.

---

## Security

**16. GoToSocial token logged in errors (various)**
The logger doesn't currently log the token, which is good — but error messages include the Kafka brokers and offset values. While not a direct leak, ensure tokens are never printed in any panic recover or panic hooks added later. Consider using `slog.Redacted` if sensitive data is ever logged as a struct field.

**17. No TLS verification settings for GoToSocial (gotosocial_publisher.go:24-28)**
The HTTP transport uses default TLS config and does not explicitly control certificate validation. This could be a problem if the target GoToSocial instance has invalid/self-signed certificates. Add a `TLSSkipVerify` option to the Config for development deployments, documented clearly as dangerous.

**18. Container runs as root (Containerfile)**
The distroless image runs as UID 0 by default. For security, add a non-root user or `USER 65534:nobody` in the Containerfile.

---

## Simplifications & Code Quality

**19. Unreachable loop body in formatter (stock_event_formatter.go:19-27)**
The loop on lines 20-25 iterates over `parts` but nothing is appended inside it — it's dead code. The next line (26) uses direct formatting. This entire loop can be removed, making the function simpler and fixing a bug where the intended logic was never executed.

**20. `go.mod` has wrong module path (`github.com/example/stocker-informer`)**
This is the default template placeholder from `go mod init`. It should be updated to the actual repository URL (e.g., `github.com/<org>/stocker-informer`) before publishing or sharing.

**21. No `go.sum` in repo (implied, not present)**
The `go.sum` file is gitignored by convention in some projects but for open-source you should commit it to ensure reproducible builds. Run `go mod tidy` and verify `go.sum` is tracked.

**22. Hardcoded image registry URL in two deploy scripts (publish.sh:4 + install.sh:7)**
`REGISTRY="containers.wheeli.ca"` is duplicated. Move it to a shared variable or environment variable, or use a single source of truth.

**23. `install.sh` requires `go.sum` but doesn't generate it (install.sh:14)**
The script copies `go.sum` from the project directory. If someone runs install without first running `go mod tidy`, it will fail silently or copy a stale file. Consider generating it or adding a check.

**24. PodmanArgs repeated key is valid but unusual (stocker-informer.container:4-5)**
Quadlet allows duplicate keys for array values, so this works correctly — but could be confusing to readers unfamiliar with Quadlet syntax. Add a comment explaining the behavior.

**25. `MaxBytes: 10e6` uses float literal (consumer.go:47)**
`10e6` is a float; `maxInt32` or explicit integer (`10_000_000`) would be clearer since max batch size is an integer limit. Minor, but could confuse readers about the actual value (it works due to Go's type conversion, but intent isn't obvious).

**26. Kafka logger inconsistency (consumer.go:49-53)**
`Logger` is discarded but `ErrorLogger` writes to the app logger. This means reader-level warnings/info are lost. Either use the app logger for both, or document why only errors propagate — currently it's an unclear half-filtering.

**27. No version or build info output (cmd/server/main.go:25)**
For debugging in production, the daemon prints no version string, commit hash, or build time. Add `-ldflags` support in the Containerfile and print these at startup for operational triage.

---

## Deployment & Operations

**28. `AutoUpdate=local` in quadlet may cause unexpected restarts (stocker-informer.container:6)**
This automatically updates the pod image whenever it changes on disk, which for local development is fine but for production deployment can cause uncontrolled version drift. Consider removing or using a tag-specific approach (e.g., `AutoUpdate=registry`).

**29. No systemd watchdog configuration (stocker-informer.container:10)**
Without `WatchdogSec=` in the `[Service]` section, systemd won't restart the container if it hangs while still running. Add `WatchdogSec=` for resiliancy against stuck processes.

**30. No log rotation configured (Containerfile)**
The daemon writes structured JSON to stdout. In long-running deployments without log rotation (e.g., via systemd journal rate limits or a log driver), this can consume disk space indefinitely. Add note about configuring journald or using `--log-opt max-size` in the Container section.

**31. `.env.podman` has Kafka bootstrap set to localhost (`.env.podman:5`)**
The default `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` will not work outside local development. Consider labeling this more prominently as a development-only default or adding a `.env.sample` convention that's clearly distinct from `.env.podman`.

**32. `install.sh` runs `systemctl --user daemon-reload` non-destructively (install.sh:28)**
This is generally safe, but running it in the middle of the script means if it fails, the subsequent steps won't execute due to `set -e`. Good behavior overall — just worth noting that users need a running user systemd instance (`systemd --user`), which isn't always available (e.g., WSL, containers without PID 1 as systemd).
