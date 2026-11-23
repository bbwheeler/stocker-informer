# Kubernetes Conformity Implementation Plan

**Goal:** Replace stocker-informer's Kubernetes manifests with a two-file layout
that matches stocker-store exactly, and move the service onto RabbitMQ exchange
`stocker` with binding key `stock.gems.v1.#` and queue `stock.gems.v1.informer`.

**Architecture:** Deployment packaging changes only. `deploy/kubernetes/`
becomes `stocker-informer.yaml` (Namespace + ConfigMap + Deployment) plus a
`secret.yaml` template. Three compiled-in RabbitMQ defaults change; the
consumer, probe, publisher, and rewriter are untouched. Docs and the quadlet
env template follow the new values.

**Tech Stack:** Go 1.25, `amqp091-go`, plain Kubernetes YAML (no Helm, no
Kustomize), Bash, systemd quadlet, `podman`.

**Spec:** `docs/design-kubernetes-conformity.md` (source of truth for every
decision, value, and rationale). Read it with this plan.

## Global Constraints

- Namespace `stocker` for every object, declared in the main manifest.
- Exactly two files in `deploy/kubernetes/`: `stocker-informer.yaml` and
  `secret.yaml`.
- Labels are plain `app: stocker-informer`; no `app.kubernetes.io/*` keys
  anywhere in `deploy/kubernetes/`.
- No `imagePullSecrets`.
- Container security context, exactly:
  `allowPrivilegeEscalation: false`, `capabilities.drop: ["ALL"]`,
  `runAsNonRoot: true`, `seccompProfile.type: RuntimeDefault`,
  `runAsUser: 65532`, `runAsGroup: 65532`.
- Resources, exactly: requests `cpu: 50m` / `memory: 128Mi`; limits
  `memory: 512Mi`.
- Readiness: `httpGet /health` on named port `health` (8080),
  `initialDelaySeconds: 5`, `periodSeconds: 10`. No liveness probe.
- No Service.
- RabbitMQ values: exchange `stocker`, type `topic`, routing key
  `stock.gems.v1.#`, queue `stock.gems.v1.informer`, prefetch `1`, connect
  timeout `10`.
- Environment variable names stay `RABBITMQ_INPUT_EXCHANGE`,
  `RABBITMQ_INPUT_EXCHANGE_TYPE`, `RABBITMQ_INPUT_ROUTING_KEY`,
  `RABBITMQ_QUEUE`.
- `RABBITMQ_URL`'s compiled-in default does not change; the in-cluster URL is
  supplied by `secret.yaml`.
- No real credentials in git. Manifests contain `REPLACE_ME`,
  `RABBITMQ_PASSWORD`, or values already committed in source. The tracked
  `secret.yaml` stays a placeholder template; a filled copy is applied from
  outside the repository and never added to git.
- Conventional Go hygiene: `gofmt -l .` empty, `go vet ./...` clean,
  `go test ./...` green.

## Baseline (recorded before any change)

Baseline recorded at `d7596fe`, where the tree matched `origin/main`. The
branch's later commits only add documentation (this plan and its design), so
the code baseline is unchanged. Commands and observed results on 2026-11-21:

| Command | Result |
| --- | --- |
| `go build ./...` | exit 0, no output |
| `GOMAXPROCS=2 go test -count=1 -p 1 ./...` | 6/6 packages `ok` (`cmd/server`, `internal/config`, `internal/messenger`, `internal/rabbitmq`, `internal/rewriter`, `internal/summarystore`) |
| `GOMAXPROCS=2 go vet ./...` | exit 0, no output |
| `gofmt -l .` | no output |

Environment note: the sandbox intermittently fails a plain `go test ./...`
with `runtime: failed to create new OS thread (errno=11)` under thread
pressure. That is a host limit, not a test failure; the constrained
invocation above is the reliable baseline. Use it in this environment, and
plain `go test ./...` anywhere else.

No tests were failing before this work. If a step below fails, the cause is
in that step, not a pre-existing break.

---

### Task 1: Restructure `deploy/kubernetes/` to the gold-standard layout

**Files:**
- Create: `deploy/kubernetes/stocker-informer.yaml`
- Overwrite: `deploy/kubernetes/secret.yaml`
- Delete: `deploy/kubernetes/configmap.yaml`
- Delete: `deploy/kubernetes/deployment.yaml`

**Produces:** the only two manifests the later docs and runbooks refer to, and
the ConfigMap values the running image reads.

- [ ] **Step 1: Create `deploy/kubernetes/stocker-informer.yaml`**

Write exactly this content:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: stocker
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: stocker-informer-config
  namespace: stocker
data:
  RABBITMQ_INPUT_EXCHANGE: stocker
  RABBITMQ_INPUT_EXCHANGE_TYPE: topic
  RABBITMQ_INPUT_ROUTING_KEY: stock.gems.v1.#
  RABBITMQ_QUEUE: stock.gems.v1.informer
  RABBITMQ_PREFETCH: "1"
  RABBITMQ_CONNECT_TIMEOUT_SECONDS: "10"
  S3_ENDPOINT: http://192.168.1.31:3900
  S3_REGION: us-east-1
  S3_TIMEOUT_SECONDS: "10"
  S3_BUCKET: REPLACE_ME
  GOTOSOCIAL_INSTANCE: https://wheeli.social
  GOTOSOCIAL_USER: bot
  GOTOSOCIAL_AUDIENCE: global
  OLLAMA_ENABLED: "true"
  OLLAMA_BASE_URL: http://192.168.1.71:11434
  OLLAMA_MODEL: gemma4:26b
  OLLAMA_TIMEOUT_SECONDS: "120"
  OLLAMA_TEMPERATURE: "0.7"
  OLLAMA_MAX_TOKENS: "1024"
  OLLAMA_MAX_OUTPUT_CHARS: "0"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stocker-informer
  namespace: stocker
spec:
  replicas: 1
  selector:
    matchLabels:
      app: stocker-informer
  template:
    metadata:
      labels:
        app: stocker-informer
    spec:
      containers:
        - name: stocker-informer
          image: git.wheeli.ca/brian/stocker-informer:latest
          imagePullPolicy: Always
          ports:
            - name: health
              containerPort: 8080
          envFrom:
            - configMapRef:
                name: stocker-informer-config
            - secretRef:
                name: stocker-informer-env
          readinessProbe:
            httpGet:
              path: /health
              port: health
            initialDelaySeconds: 5
            periodSeconds: 10
          resources:
            requests:
              cpu: 50m
              memory: 128Mi
            limits:
              memory: 512Mi
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
            runAsNonRoot: true
            seccompProfile:
              type: RuntimeDefault
            runAsUser: 65532
            runAsGroup: 65532
```

- [ ] **Step 2: Overwrite `deploy/kubernetes/secret.yaml` with the template**

Write exactly this content:

```yaml
# Template only. Fill in the real passwords, apply it, and don't commit the filled-in version.
# Assumes the RabbitMQ client Service is named "rabbitmq" in namespace "messaging"
# (check with: kubectl -n messaging get svc).
apiVersion: v1
kind: Secret
metadata:
  name: stocker-informer-env
  namespace: stocker
type: Opaque
stringData:
  RABBITMQ_URL: "amqp://stocker:RABBITMQ_PASSWORD@rabbitmq.messaging.svc.cluster.local:5672/"
  S3_ACCESS_KEY: REPLACE_ME
  S3_SECRET_KEY: REPLACE_ME
  GOTOSOCIAL_TOKEN: REPLACE_ME
```

- [ ] **Step 3: Delete the old per-kind files**

```bash
git rm deploy/kubernetes/configmap.yaml deploy/kubernetes/deployment.yaml
```

Expected: two `rm '...'` lines. `ls deploy/kubernetes/` lists only
`secret.yaml` and `stocker-informer.yaml`.

- [ ] **Step 4: Validate YAML parses and has the expected kinds**

```bash
python3 - <<'PY'
import glob, yaml
for path in sorted(glob.glob('deploy/kubernetes/*.yaml')):
    docs = [d for d in yaml.safe_load_all(open(path)) if d]
    print(path, [d['kind'] for d in docs])
PY
```

Expected:

```text
deploy/kubernetes/secret.yaml ['Secret']
deploy/kubernetes/stocker-informer.yaml ['Namespace', 'ConfigMap', 'Deployment']
```

- [ ] **Step 5: Validate conformity invariants mechanically**

```bash
grep -rn "app.kubernetes.io" deploy/kubernetes/ ; echo "labels check exit: $?"
grep -rn "imagePullSecrets\|wheeli-registry" deploy/kubernetes/ ; echo "pull secret check exit: $?"
grep -c "runAsUser: 65532" deploy/kubernetes/stocker-informer.yaml
grep -n "livenessProbe\|kind: Service" deploy/kubernetes/*.yaml ; echo "probe/service check exit: $?"
```

Expected: both `grep` calls print nothing and exit `1`; the `runAsUser` count
is `1`; the probe/service grep prints nothing and exits `1`.

- [ ] **Step 6: Validate with the cluster tooling if available**

```bash
if command -v kubectl >/dev/null; then
  kubectl apply --dry-run=client --validate=false -f deploy/kubernetes/stocker-informer.yaml
  kubectl apply --dry-run=client --validate=false -f deploy/kubernetes/secret.yaml
else
  echo "kubectl not installed; YAML parse in step 4 is the local gate"
fi
```

Expected: `namespace/stocker created (dry run)`, `configmap/... created (dry
run)`, `deployment.apps/... created (dry run)`, `secret/... created (dry
run)`; or the fallback message on hosts without `kubectl`.

- [ ] **Step 7: Verify the ConfigMap carries the new topology**

```bash
grep -n "RABBITMQ" deploy/kubernetes/stocker-informer.yaml
```

Expected: `stocker`, `topic`, `stock.gems.v1.#`, `stock.gems.v1.informer`,
`"1"`, `"10"`, in that order.

- [ ] **Step 8: Commit**

```bash
git add deploy/kubernetes/stocker-informer.yaml deploy/kubernetes/secret.yaml
git commit -m "k8s: conform manifests to stocker-store gold standard"
```

---

### Task 2: Align the runtime image UID with the pod security context

**Files:**
- Modify: `Containerfile` (line 15, the `USER` line)

**Interfaces:**
- Consumes: nothing.
- Produces: image `USER 65532:65532`, matching `runAsUser`/`runAsGroup` in
  Task 1.

- [ ] **Step 1: Change the runtime user**

Replace:

```dockerfile
USER 65534:65534
```

with:

```dockerfile
USER 65532:65532
```

- [ ] **Step 2: Confirm the edit**

```bash
grep -n '^USER' Containerfile
```

Expected: `USER 65532:65532`.

- [ ] **Step 3: Verify the image metadata and that the binary runs as 65532**

```bash
podman build -t stocker-informer:uidcheck -f Containerfile .
podman inspect --format '{{.Config.User}}' stocker-informer:uidcheck
podman run --rm --user 65532:65532 stocker-informer:uidcheck
```

Expected: `65532:65532` from `inspect`; the run starts the binary, fails
config validation, prints a JSON line containing
`load config: invalid configuration:` (required variables absent), and exits
`1`. That proves execution as 65532 with no writable filesystem need.

If the build cannot reach `git.wheeli.ca` to resolve the private module,
record that in the commit message and rely on the static-binary evidence:
distroless ships `nonroot` as 65532 and `/stocker-informer` is `0755`.

- [ ] **Step 4: Commit**

```bash
git add Containerfile
git commit -m "build: run container as uid 65532 to match pod securityContext"
```

---

### Task 3: Change the compiled-in RabbitMQ defaults (test first)

**Files:**
- Test: `internal/config/config_test.go`
- Modify: `internal/config/config.go:31-34` (`defaultRabbitMQURL` on line 30
  is deliberately untouched)

**Interfaces:**
- Consumes: nothing.
- Produces: `config.Load()` defaults
  `InputExchange="stocker"`, `InputRoutingKey="stock.gems.v1.#"`,
  `Queue="stock.gems.v1.informer"`, consumed by Task 4's fixtures and the
  docs in Task 6.

- [ ] **Step 1: Update the expected defaults in `TestLoadRabbitMQDefaults`**

Replace the `want` block's three values:

```go
	want := RabbitMQConfig{
		URL:               testRabbitMQURL,
		InputExchange:     "stocker",
		InputExchangeType: "topic",
		InputRoutingKey:   "stock.gems.v1.#",
		Queue:             "stock.gems.v1.informer",
		Prefetch:          1,
		ConnectTimeout:    10 * time.Second,
	}
```

- [ ] **Step 2: Update the defaults table in `TestLoadRabbitMQNameVariables`**

Replace the `variables` slice entries:

```go
	variables := []struct {
		name string
		def  string
	}{
		{name: "RABBITMQ_INPUT_EXCHANGE", def: "stocker"},
		{name: "RABBITMQ_INPUT_ROUTING_KEY", def: "stock.gems.v1.#"},
		{name: "RABBITMQ_QUEUE", def: "stock.gems.v1.informer"},
	}
```

- [ ] **Step 3: Run the two tests and confirm they fail**

```bash
go test ./internal/config -run 'TestLoadRabbitMQDefaults|TestLoadRabbitMQNameVariables' -count=1
```

Expected: FAIL. The output shows the loaded values still carry
`InputExchange:stock.summary`, `InputRoutingKey:stock.summary.available.v1`,
and `Queue:stocker-informer` against the new `want`, and the name-variable
subtests report `got "stock.summary"` (or the other old values) instead of
the new defaults.

If the sandbox reports `failed to create new OS thread`, rerun as
`GOMAXPROCS=2 go test ./internal/config -run '...' -count=1`; the expected
failure is unchanged.

- [ ] **Step 4: Change the three constants**

In `internal/config/config.go`, replace:

```go
	defaultRabbitMQInputExchange         = "stock.summary"
	defaultRabbitMQInputExchangeType     = "topic"
	defaultRabbitMQInputRoutingKey       = "stock.summary.available.v1"
	defaultRabbitMQQueue                 = "stocker-informer"
```

with:

```go
	defaultRabbitMQInputExchange         = "stocker"
	defaultRabbitMQInputExchangeType     = "topic"
	defaultRabbitMQInputRoutingKey       = "stock.gems.v1.#"
	defaultRabbitMQQueue                 = "stock.gems.v1.informer"
```

Do not touch `defaultRabbitMQURL`, the URL validation, or `RedactedAMQPURL`.

- [ ] **Step 5: Run the two tests and confirm they pass**

```bash
go test ./internal/config -run 'TestLoadRabbitMQDefaults|TestLoadRabbitMQNameVariables' -count=1
```

Expected: `ok  github.com/example/stocker-informer/internal/config`.

- [ ] **Step 6: Run the whole config package**

```bash
go test ./internal/config -count=1
```

Expected: `ok`. If a name-length or custom-value test assumed a specific old
default, fix that assertion against the new value rather than reintroducing
the old one.

- [ ] **Step 7: Confirm no old default remains in the package**

```bash
grep -rn "stock\.summary\|stocker-informer\"" internal/config/*.go
```

Expected: no output.

- [ ] **Step 8: Commit**

```bash
git add internal/config/config.go internal/config/config_test.go
git commit -m "config: default to stocker exchange and stock.gems.v1 topology"
```

---

### Task 4: Update RabbitMQ package test fixtures

**Files:**
- Modify: `internal/rabbitmq/consumer_test.go` (`testCfg`, delivery routing keys, log assertion)
- Modify: `internal/rabbitmq/probe_test.go` (fake 404 messages, name assertions)
- Modify: `internal/rabbitmq/consumer_live_test.go` (comment)

**Interfaces:**
- Consumes: Task 3's defaults are irrelevant here because every fixture is
  explicit; this task keeps the fixtures representative of the new topology.
- Produces: offline tests that pass with the new names and still assert the
  same behaviour.

- [ ] **Step 1: Update `testCfg()` in `consumer_test.go`**

Replace:

```go
	return config.RabbitMQConfig{
		URL:               "amqp://stocker:" + secretPassword + "@192.168.1.48:5672/",
		InputExchange:     "stock.summary",
		InputExchangeType: "topic",
		InputRoutingKey:   "stock.summary.available.v1",
		Queue:             "stocker-informer",
		Prefetch:          1,
		ConnectTimeout:    10 * time.Second,
	}
```

with:

```go
	return config.RabbitMQConfig{
		URL:               "amqp://stocker:" + secretPassword + "@192.168.1.48:5672/",
		InputExchange:     "stocker",
		InputExchangeType: "topic",
		InputRoutingKey:   "stock.gems.v1.#",
		Queue:             "stock.gems.v1.informer",
		Prefetch:          1,
		ConnectTimeout:    10 * time.Second,
	}
```

- [ ] **Step 2: Update the delivery routing keys in `consumer_test.go`**

Three places build an `amqp.Delivery` (around lines 1293, 1364, and 1439),
and one log assertion checks the logged field (around line 1371). Replace
every occurrence of `"stock.summary.available.v1"` **in those delivery
fixtures and the assertion** with `"stock.gems.v1.AAPL"`, for example:

```go
			c.handleDelivery(context.Background(), sess, amqp.Delivery{
				DeliveryTag: 7,
				RoutingKey:  "stock.gems.v1.AAPL",
				Redelivered: true,
				Body:        tt.body,
			})
```

and:

```go
	checks := map[string]any{
		"routing_key":   "stock.gems.v1.AAPL",
		"delivery_tag":  float64(11),
		"redelivered":   true,
		"symbol":        "AAPL",
		"exchange":      "NASDAQ",
		"rewritten":     true,
		"summary_chars": float64(len([]rune(summary))),
		"report_chars":  float64(len([]rune(report))),
	}
```

Do not change `testCfg()`'s `InputRoutingKey` to `stock.gems.v1.AAPL`; that
field is the binding key.

- [ ] **Step 3: Confirm no old routing key remains in `consumer_test.go`**

```bash
grep -n "stock\.summary" internal/rabbitmq/consumer_test.go
```

Expected: no output.

- [ ] **Step 4: Update `probe_test.go` fake broker errors and assertions**

`newTestProbe` builds its config from `testCfg()`, so the probe now expects
`stocker` and `stock.gems.v1.informer`. Replace the four old names:

```go
	fch := &fakeProbeChannel{exchangeErr: notFoundError("no exchange 'stocker' in vhost '/'")}
```

```go
	if !strings.Contains(err.Error(), `exchange "stocker"`) {
		t.Errorf("error %q does not name the exchange", err)
	}
```

```go
	fch := &fakeProbeChannel{queueErr: notFoundError("no queue 'stock.gems.v1.informer' in vhost '/'")}
```

```go
	if !strings.Contains(err.Error(), `queue "stock.gems.v1.informer"`) {
		t.Errorf("error %q does not name the queue", err)
	}
```

- [ ] **Step 5: Update the live-test comment**

In `internal/rabbitmq/consumer_live_test.go`, replace the comment sentence
fragment `touch the shared stock.summary exchange or the stocker-informer
queue` with `touch the shared stocker exchange or the stock.gems.v1.informer
queue`. The live test builds unique names at run time, so no code changes.

- [ ] **Step 6: Run the package tests**

```bash
go test ./internal/rabbitmq -count=1
```

Expected: `ok  github.com/example/stocker-informer/internal/rabbitmq`. The
live test remains skipped.

- [ ] **Step 7: Confirm no old topology value remains in the package**

```bash
grep -rn "stock\.summary" internal/rabbitmq/*.go
grep -rn 'Queue: *"stocker-informer"' internal/rabbitmq/*.go
```

Expected: no output from either. The `stocker-informer-` consumer-tag prefix
(`consumerTagPrefix`) and the `stocker-informer-live-*` unique live-test names
are unrelated to the queue under test and must stay as they are.

- [ ] **Step 8: Commit**

```bash
git add internal/rabbitmq/consumer_test.go internal/rabbitmq/probe_test.go internal/rabbitmq/consumer_live_test.go
git commit -m "test: update rabbitmq fixtures for the stock.gems.v1 topology"
```

---

### Task 5: Update the quadlet environment template

**Files:**
- Modify: `deploy/quadlet/.env.podman.example` (lines 9-12)

- [ ] **Step 1: Replace the three RabbitMQ values**

Replace:

```text
RABBITMQ_INPUT_EXCHANGE=stock.summary
RABBITMQ_INPUT_EXCHANGE_TYPE=topic
RABBITMQ_INPUT_ROUTING_KEY=stock.summary.available.v1
RABBITMQ_QUEUE=stocker-informer
```

with:

```text
RABBITMQ_INPUT_EXCHANGE=stocker
RABBITMQ_INPUT_EXCHANGE_TYPE=topic
RABBITMQ_INPUT_ROUTING_KEY=stock.gems.v1.#
RABBITMQ_QUEUE=stock.gems.v1.informer
```

Leave the comment block above it unchanged. `#` is literal in a systemd
`EnvironmentFile` value (inline `#` is not a comment), so no quoting is
needed.

- [ ] **Step 2: Confirm the template and no residue**

```bash
grep -n "RABBITMQ" deploy/quadlet/.env.podman.example
grep -rn "stock\.summary" deploy/quadlet/ ; echo "residue check exit: $?"
```

Expected: the four new values, plus `RABBITMQ_PREFETCH`,
`RABBITMQ_CONNECT_TIMEOUT_SECONDS`, and the commented `# RABBITMQ_URL=...`
guidance lines; the residue grep prints nothing and exits `1`.

- [ ] **Step 3: Commit**

```bash
git add deploy/quadlet/.env.podman.example
git commit -m "deploy: update quadlet env template for the new rabbitmq topology"
```

---

### Task 6: Update operator documentation and mark stale Kubernetes docs

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `docs/deployment.md` (live operator sections only)
- Modify: `docs/design-kubernetes-deployment.md` (superseded banner)
- Modify: `docs/plan-kubernetes-deployment.md` (superseded banner)

**Interfaces:**
- Consumes: Task 1's manifest layout and values, Task 2's UID, Task 3's
  defaults, Task 5's template.
- Produces: docs that agree with the shipped code and manifests.

- [ ] **Step 1: `README.md` at-a-glance and image UID**

Line 35: change `user \`nobody\`/\`65534\`` to `user \`nonroot\`/\`65532\``.

Lines 127-129: change `runs as \`USER 65534:65534\` (\`nobody\`)` to
`runs as \`USER 65532:65532\` (\`nonroot\`)`, and `non-root user (\`65534\`)` to
`non-root user (\`65532\`)`.

Line 23: append to the deployment bullet: `; Kubernetes manifests live in
\`deploy/kubernetes/\` (Namespace + ConfigMap + Deployment, conforming to
\`stocker-store\`)`.

- [ ] **Step 2: `README.md` quick start and environment table**

Lines 53-56: replace the three values with:

```bash
export RABBITMQ_INPUT_EXCHANGE=stocker
export RABBITMQ_INPUT_EXCHANGE_TYPE=topic
export RABBITMQ_INPUT_ROUTING_KEY=stock.gems.v1.#
export RABBITMQ_QUEUE=stock.gems.v1.informer
```

Environment table rows:
- `RABBITMQ_INPUT_EXCHANGE`: default `stocker`.
- `RABBITMQ_INPUT_ROUTING_KEY`: default `stock.gems.v1.#`.
- `RABBITMQ_QUEUE`: default `stock.gems.v1.informer`.

- [ ] **Step 3: `README.md` prerequisites and topology descriptions**

Lines 85-86: change the declarations sentence to name the durable `stocker`
exchange and the `stock.gems.v1.informer` queue; change the producer sentence
to name the `stocker` exchange and the `stock.gems.v1.#` routing key, and note
that the exchange is shared with `stocker-store` and declared with the same
`topic`/durable arguments.

Lines 229-230: replace the exchange/routing-key bullets with `Exchange:
stocker` and `Routing key: stock.gems.v1.#`, keeping the variable references.

Lines 490-492: replace the default values in the topology bullets with
`stocker`, `stock.gems.v1.#`, and `stock.gems.v1.informer`.

Line 532: correct the health-endpoint description. The probe passively
declares the exchange and queue to verify they exist; passive declaration does
not check their arguments, so drop "checking its type and durability" and
"checking `durable=true`, non-exclusive". Passive declares still create, bind,
consume, and publish nothing.

Line 552: change the publish instruction to `stocker` exchange and
`stock.gems.v1.AAPL` routing key.

Line 557: the restart check names the durable queue; change `stocker-informer`
to `stock.gems.v1.informer`. Leave the `stocker-informer-<pid>` consumer tag on
line 494 alone: it is not the queue name.

- [ ] **Step 4: `README.md` repository tree**

Replace the `docs/` entry and extend the `deploy/` subtree so both list the
new files:

```text
├── docs/
│   ├── deployment.md
│   ├── design-kubernetes-conformity.md
│   ├── design-kubernetes-deployment.md
│   ├── plan-kubernetes-conformity.md
│   └── plan-kubernetes-deployment.md
```

```text
└── deploy/
    ├── kubernetes/
    │   ├── stocker-informer.yaml
    │   └── secret.yaml
    ├── push.sh
    └── quadlet/
        ├── .env.podman.example
        └── stocker-informer.container
```

- [ ] **Step 5: `AGENTS.md` structure, architecture, and defaults**

Structure block: replace the `deploy/quadlet/` line with

```text
deploy/kubernetes/        # gold-standard manifests: stocker-informer.yaml + secret.yaml template
deploy/quadlet/           # single rootless quadlet + .env.podman.example template
```

and extend the `docs/` comment to list `deployment.md`,
`design-kubernetes-conformity.md`, `plan-kubernetes-conformity.md`, and the
two superseded documents (`design-kubernetes-deployment.md`,
`plan-kubernetes-deployment.md`).

Architecture paragraph: change the declared defaults to
`RABBITMQ_INPUT_EXCHANGE` default `stocker` and `RABBITMQ_QUEUE` default
`stock.gems.v1.informer`.

Environment list: change the three defaults to `stocker`,
`stock.gems.v1.#`, and `stock.gems.v1.informer`.

- [ ] **Step 6: `docs/deployment.md` live operator sections**

Line 56: change `non-root 65534` to `non-root 65532`.

Lines 739-740: `rabbitmqadmin publish exchange=stocker
routing_key=stock.gems.v1.AAPL`.

Line 750: the sample log line becomes
`routing_key=stock.gems.v1.AAPL`.

Lines 785-787: exchange `stocker` type `topic` durable; queue
`stock.gems.v1.informer` durable, non-exclusive, non-auto-delete; binding
exchange `stocker`, queue `stock.gems.v1.informer`, routing key
`stock.gems.v1.#`.

Lines 800-801: the commented publish example becomes exchange `stocker`,
routing key `stock.gems.v1.AAPL`.

Leave the historical Tasks 1-18 above the manual check untouched; they record
what was done at the time.

- [ ] **Step 7: Mark the two stale Kubernetes docs superseded**

At the top of `docs/design-kubernetes-deployment.md`, under the H1 and before
`## Overview`, add:

```markdown
> **Superseded 2026-11-21** by
> [design-kubernetes-conformity.md](design-kubernetes-conformity.md). The
> three-file layout, `app.kubernetes.io/*` labels, and `wheeli-registry`
> pull secret described here are no longer used. Kept for history.
```

At the top of `docs/plan-kubernetes-deployment.md`, under the H1, add:

```markdown
> **Superseded 2026-11-21** by
> [plan-kubernetes-conformity.md](plan-kubernetes-conformity.md). Do not
> execute this plan against the current tree; it describes the retired
> three-file layout and old RabbitMQ topology.
```

- [ ] **Step 8: Confirm no stale live values remain in the updated docs**

```bash
grep -n "stock\.summary\|stocker-informer\` queue\|USER 65534\|65534" README.md AGENTS.md docs/deployment.md
```

Expected: no output. `docs/deployment.md` has no historical reference to the
old topology: its Tasks 1-18 describe the quadlet and never name the exchange,
routing key, or queue values, so the live sections are the only ones to fix.
If that ever changes, prefix the historical line with `Historical:` rather
than deleting the record.

- [ ] **Step 9: Commit**

```bash
git add README.md AGENTS.md docs/deployment.md docs/design-kubernetes-deployment.md docs/plan-kubernetes-deployment.md
git commit -m "docs: document kubernetes conformity and the new rabbitmq topology"
```

---

### Task 7: Verification gate and push

**Files:** none modified; this task proves the tree is releasable.

- [ ] **Step 1: Format, build, vet**

```bash
gofmt -l .
go build ./...
GOMAXPROCS=2 go vet ./...
```

Expected: no output from `gofmt`; `go build` and `go vet` exit `0` with no
diagnostics.

- [ ] **Step 2: Full test suite**

```bash
GOMAXPROCS=2 go test -count=1 -p 1 ./...
```

Expected: six `ok` lines (`cmd/server`, `internal/config`,
`internal/messenger`, `internal/rabbitmq`, `internal/rewriter`,
`internal/summarystore`). If the environment is not thread-constrained, plain
`go test ./...` is equivalent.

- [ ] **Step 3: Manifest gate**

```bash
python3 - <<'PY'
import glob, yaml
for path in sorted(glob.glob('deploy/kubernetes/*.yaml')):
    docs = [d for d in yaml.safe_load_all(open(path)) if d]
    print(path, [d['kind'] for d in docs])
PY
grep -rn "app.kubernetes.io\|imagePullSecrets\|kind: Service\|livenessProbe" deploy/kubernetes/ ; echo "conformity grep exit: $?"
grep -rn "192.168.1.48\|REPLACE_ME\|RABBITMQ_PASSWORD" deploy/kubernetes/
```

Expected: the two expected file/kind lines from Task 1; the conformity grep
prints nothing and exits `1`; the last grep shows only placeholders (no
`192.168.1.48` and no real password).

- [ ] **Step 4: Cross-check docs against code defaults**

```bash
grep -n "defaultRabbitMQInputExchange\|defaultRabbitMQInputRoutingKey\|defaultRabbitMQQueue" internal/config/config.go
grep -n "RABBITMQ_INPUT_EXCHANGE\|RABBITMQ_INPUT_ROUTING_KEY\|RABBITMQ_QUEUE" README.md AGENTS.md deploy/kubernetes/stocker-informer.yaml deploy/quadlet/.env.podman.example
```

Expected: `stocker`, `stock.gems.v1.#`, `stock.gems.v1.informer` everywhere.

- [ ] **Step 5: Old-value grep gate**

The old exchange and routing key both contain `stock.summary`, so one grep
covers them. The old queue name needs targeted patterns because
`stocker-informer` is also the application, image, and consumer-tag prefix; a
bare word grep could never come back empty.

```bash
grep -rn "stock\.summary" \
  internal/config/*.go internal/rabbitmq/*.go \
  deploy/kubernetes/ deploy/quadlet/.env.podman.example \
  README.md AGENTS.md docs/deployment.md ; echo "exchange/routing-key grep exit: $?"

grep -rn 'defaultRabbitMQQueue.*stocker-informer\|Queue: *"stocker-informer"\|RABBITMQ_QUEUE=stocker-informer\|RABBITMQ_QUEUE: stocker-informer' \
  internal/config/*.go internal/rabbitmq/*.go \
  deploy/kubernetes/ deploy/quadlet/.env.podman.example ; echo "queue value grep exit: $?"

grep -rn 'default `stocker-informer`\|`stocker-informer` queue\|Queue `stocker-informer`' \
  README.md AGENTS.md docs/deployment.md ; echo "queue prose grep exit: $?"
```

Expected: all three greps print nothing and exit `1`. The two superseded
documents are excluded on purpose: they are history. Any other
`stocker-informer` hit (repository name, image name, package path, consumer
tag) is the application name, not the old queue value.

- [ ] **Step 6: Optional cluster validation**

```bash
if command -v kubectl >/dev/null; then
  kubectl apply --dry-run=client --validate=false -f deploy/kubernetes/stocker-informer.yaml
  kubectl apply --dry-run=client --validate=false -f deploy/kubernetes/secret.yaml
fi
```

Expected: four `created (dry run)` lines, or no output when `kubectl` is
absent (this sandbox has none).

- [ ] **Step 7: Confirm the branch state and push**

```bash
git status --short --branch
git log --oneline origin/main..HEAD
git push -u origin feature/kubernetes-conformity
```

Expected: clean working tree; the six task commits plus the design/plan and
any review-fix commits on top of `origin/main`; the push updates the existing
`origin/feature/kubernetes-conformity` and prints the branch URL plus the
tracking message.

- [ ] **Step 8: Record the pushed SHA**

```bash
git rev-parse HEAD
git rev-parse origin/feature/kubernetes-conformity
```

Expected: identical SHAs.

## Rollout (after merge, not part of the code tasks)

1. Confirm the external producer publishes `stock.gems.v1.*` to `stocker`.
2. Copy `secret.yaml` outside the repo, fill the copy, and apply both:
   `kubectl apply -f deploy/kubernetes/stocker-informer.yaml -f
   /tmp/stocker-informer-secret.yaml`. Never edit or `git add` the tracked
   template with real values.
3. Confirm `kubectl -n stocker get pods -l app=stocker-informer` shows the pod
   ready and the logs show `rabbitmq consumer connected` with the new
   exchange, routing key, and queue.
4. Publish one test signal and confirm the GoToSocial post.
5. Delete the old queue once the new consumer is healthy:
   `rabbitmqctl delete_queue stocker-informer`. Leave the `stock.summary`
   exchange until no other consumer uses it.

## Rollback

Revert the ConfigMap values to `stock.summary` /
`stock.summary.available.v1` / `stocker-informer`, reapply, and restart the
deployment. Messages published to `stock.gems.v1.*` while the new topology was
live are not replayed into the old queue.
