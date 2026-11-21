# Kubernetes Conformity Design: stocker-informer

**Status:** Accepted, 2026-11-21
**Reference implementation:** `stocker-store/deploy/kubernetes/` at commit `6d96e02` (`stocker-store.yaml`, `secret.yaml`)
**Supersedes:** `docs/design-kubernetes-deployment.md`, `docs/plan-kubernetes-deployment.md`
**Implementation plan:** `docs/plan-kubernetes-conformity.md`

## 1. Goal

Make `stocker-informer`'s Kubernetes manifests conform to the `stocker-store`
manifests, which are the gold standard for this fleet: same namespace, same
file layout, same labels, same security posture, same secret template. In the
same change, move the service onto the new RabbitMQ topology:

| Setting | Old value | New value |
| --- | --- | --- |
| Input exchange | `stock.summary` | `stocker` |
| Exchange type | `topic` | `topic` (unchanged) |
| Binding key | `stock.summary.available.v1` | `stock.gems.v1.#` |
| Queue | `stocker-informer` | `stock.gems.v1.informer` |
| Prefetch | `1` | `1` (unchanged) |
| Connect timeout | `10s` | `10s` (unchanged) |

Nothing about the application's pipeline changes: it still decodes protobuf
`stockstore.v1.Stock`, reads `[S3_PATH_PREFIX/]{EXCHANGE}/{SYMBOL}/summary.md`
from Garage/S3, rewrites with Ollama when enabled, and publishes to
GoToSocial. Only transport names and deployment packaging move.

## 2. What the gold standard actually contains

Read directly from `stocker-store/deploy/kubernetes/`:

`stocker-store.yaml` is one multi-document file holding, in order:

1. `Namespace` `stocker` (no labels).
2. `ConfigMap` `stocker-store-config` in `stocker` (no labels, no comments,
   plain non-secret values).
3. `Deployment` `stocker-store` in `stocker`, with metadata limited to
   `name`/`namespace`, `replicas: 1`, `selector.matchLabels.app`, a pod
   template labelled `app`, `envFrom` referencing the ConfigMap and the
   Secret, a `tcpSocket` readiness probe on a named port, a resources block,
   and the container security context.
4. `Service` `stocker-store` (ClusterIP, gRPC port 3500).

`secret.yaml` is a comment-headed Opaque Secret template named
`stocker-store-env` in `stocker`, holding `RABBITMQ_URL` (in-cluster broker
DNS plus a password placeholder) and the database credentials.

Observed conventions, verbatim from the files:

| Convention | stocker-store |
| --- | --- |
| Files | Two: one multi-document main YAML plus a Secret template |
| Namespace | `stocker` declared in the main file |
| Labels | Plain `app: <name>` on selector, pod template, and Service; no `app.kubernetes.io/*` anywhere |
| Resource metadata | Only `name` (and `namespace` where required); no `labels` block |
| imagePullSecrets | None |
| Image | `git.wheeli.ca/brian/stocker-store:latest`, `imagePullPolicy: Always` |
| Replicas | `1` |
| Readiness | `tcpSocket` on the named gRPC port, `initialDelaySeconds: 5`, `periodSeconds: 10` |
| Liveness | None |
| Resources | requests `cpu: 50m`, `memory: 128Mi`; limits `memory: 512Mi` (no CPU limit) |
| Container securityContext | `allowPrivilegeEscalation: false`, `capabilities.drop: ["ALL"]`, `runAsNonRoot: true`, `seccompProfile.type: RuntimeDefault`, `runAsUser: 65532`, `runAsGroup: 65532` |
| Pod securityContext | None (container-level only) |
| Secret | Comment-headed template, `stringData`, `REPLACE_ME`/`RABBITMQ_PASSWORD` placeholders, in-cluster broker URL `rabbitmq.messaging.svc.cluster.local:5672` |

Two corrections to the task brief, based on the files themselves:

- The brief said stocker-store has no resources stanza. It does, and the
  informer will copy it exactly.
- The brief said the readiness probe is the only probe. That matches: there is
  no liveness probe.

## 3. Current stocker-informer state and the drift

`deploy/kubernetes/` today holds three files (`configmap.yaml`,
`deployment.yaml`, `secret.yaml`) with these deviations:

| Deviation | Current value | Gold standard |
| --- | --- | --- |
| File layout | Three files, one kind each | Two files: main multi-doc + Secret template |
| Namespace | Not declared (assumed to exist) | Declared in the main file |
| Labels | `app.kubernetes.io/name`, `app.kubernetes.io/part-of` | `app` only |
| imagePullSecrets | `wheeli-registry` | None |
| Security context | `runAsNonRoot`, `allowPrivilegeEscalation` only | Plus capabilities drop, seccomp, `runAsUser`/`runAsGroup` 65532 |
| Resources | requests CPU/memory; limits CPU 500m / memory 512Mi | requests CPU/memory; memory-only limit 512Mi |
| Liveness probe | `httpGet /health` | None |
| RabbitMQ topology | `stock.summary` / `stock.summary.available.v1` / `stocker-informer` | n/a (new values in section 6) |
| Secret comments | None | Comment header plus placeholder guidance |
| Secret RabbitMQ host | `192.168.1.48:5672` | `rabbitmq.messaging.svc.cluster.local:5672` |

`Containerfile` also declares `USER 65534:65534` while the gold-standard
security context pins UID/GID `65532`. Section 8 resolves that.

## 4. Decisions

### D1: Two files, gold-standard layout

`deploy/kubernetes/stocker-informer.yaml` becomes the single multi-document
file: `Namespace` `stocker`, `ConfigMap` `stocker-informer-config`,
`Deployment` `stocker-informer`. `deploy/kubernetes/secret.yaml` is rewritten
as the template. `configmap.yaml` and `deployment.yaml` are deleted.

Rationale: file-for-file mirror of stocker-store makes the fleet reviewable
as one shape and removes the per-resource file drift that let labels and
pull secrets diverge. Deleting the old files also removes a second source of
truth; leaving them would invite partial applies.

### D2: Plain `app` labels, no metadata labels

Selector and pod-template labels become `app: stocker-informer`. The
Deployment, ConfigMap, Secret, and Namespace carry no `labels` block beyond
`name`/`namespace`. The `app.kubernetes.io/*` keys are removed everywhere.

Rationale: the gold standard uses `app` for match labels. Adding both
schemes would be redundant, and keeping only the old scheme would break a
fleet-wide `-l app=<service>` query. The Service selector does not exist here
(D8), so `app` only has to match the Deployment selector.

### D3: No imagePullSecrets

The `wheeli-registry` entry is removed. Nodes pull
`git.wheeli.ca/brian/stocker-informer:latest` the same way they pull
`stocker-store:latest` without a pull secret.

Rationale: stocker-store pulls from the same registry with no pull secret, so
registry access is already solved at the node/registry level. Keeping a
secret reference the gold standard does not use would also mean an undeclared
cluster dependency. Risk: if registry access was in fact provided only by
that secret, the first pull after rollout fails with `ErrImagePull`; the
rollback is to re-add the two lines, and the operator confirms registry
behaviour before rollout (Task 1 verification in the plan).

### D4: Exact gold-standard security context

```yaml
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

Rationale: this is the gold standard verbatim. It is also sufficient for this
workload: the image is distroless static, the binary is static
(`CGO_ENABLED=0`), the process binds `:8080` (unprivileged, so dropped
capabilities cost nothing), and it writes no files. `runAsNonRoot` with an
explicit numeric `runAsUser` lets the kubelet verify the pod is non-root
without resolving a name, and distroless ships a `nonroot` entry for 65532.

The pod-level security context is left unset, matching stocker-store.

### D5: Copy the gold-standard resources block

```yaml
resources:
  requests:
    cpu: 50m
    memory: 128Mi
  limits:
    memory: 512Mi
```

Rationale: matches the file that is the standard. Dropping the CPU limit keeps
the rewrite burst from being throttled by a 500m ceiling while retaining the
memory guard that matters on a shared node. The current CPU limit (500m) is
therefore removed as part of conforming.

### D6: Readiness stays `httpGet /health`; liveness probe is removed

`/health` is the application's real readiness signal: it returns `503` until
the RabbitMQ probe has passively verified the exchange and queue, then `200`,
and it is sticky afterwards. A `tcpSocket` probe on `:8080` would report ready
while the broker topology is still missing.

The liveness probe is dropped, matching the gold standard. Because readiness
is sticky, `/health` never fails after the first success, so the liveness
probe could not detect the failures it appears to guard; it only added a
restart path for a process that already exits on its own errors.

Probe timing mirrors stocker-store: `initialDelaySeconds: 5`,
`periodSeconds: 10`, target the named port `health` rather than the number.

### D7: No Service

stocker-informer is a consumer. Its only listener is `/health` on `:8080`,
reached by the kubelet inside the pod; no other workload connects to it. The
ClusterIP Service in stocker-store exists for inbound gRPC, which has no
analogue here.

### D8: Keep the `RABBITMQ_INPUT_*` environment variable names

Only values change:

| Variable | New default |
| --- | --- |
| `RABBITMQ_INPUT_EXCHANGE` | `stocker` |
| `RABBITMQ_INPUT_EXCHANGE_TYPE` | `topic` |
| `RABBITMQ_INPUT_ROUTING_KEY` | `stock.gems.v1.#` |
| `RABBITMQ_QUEUE` | `stock.gems.v1.informer` |

stocker-store names the same concepts `RABBITMQ_EXCHANGE`,
`RABBITMQ_EXCHANGE_TYPE`, and `RABBITMQ_ROUTING_KEY`. Renaming the informer's
variables would break the quadlet env file, the documented operator
interface, and every existing host deployment, with no functional gain; the
names describe the informer's input role, not the broker objects. The
mismatch is deliberate and documented here.

### D9: Keep the built-in `RABBITMQ_URL` default

`defaultRabbitMQURL` stays `amqp://stocker:<password>@192.168.1.48:5672/`.
The in-cluster URL is supplied by the Secret template
(`rabbitmq.messaging.svc.cluster.local:5672`), not by a code default.

Rationale: the default serves host/quadlet and live-test runs, where cluster
DNS does not resolve. Making the cluster name the default would break those
paths and force every host deployment to set `RABBITMQ_URL` just to keep
working. The URL validation and redaction code (`validateAMQPURL`,
`RedactedAMQPURL`) is unchanged.

### D10: Secret template mirrors the gold standard

`secret.yaml` gets the same two comment lines (adapted name), an Opaque
Secret `stocker-informer-env` in `stocker`, and `stringData` with the
in-cluster broker URL plus `REPLACE_ME` placeholders for the S3 keys and the
GoToSocial token. `S3_BUCKET` stays a ConfigMap placeholder because it is not
a secret.

The template is committed with placeholders only. It is applied once with
real values and the filled copy stays out of git, exactly as the gold standard
comment instructs.

### D11: ConfigMap holds non-secret values, with no comments

All RabbitMQ, Garage/S3, GoToSocial (instance/user/audience), and Ollama
settings move into the main file's ConfigMap with the current values, except
the new RabbitMQ topology. The existing explanatory comments are removed so
the file reads like the gold standard; explanations live in this design
document and in `README.md`.

### D12: Namespace is declared, not assumed

The main file starts with the `stocker` Namespace, as stocker-store does.
Applying is idempotent and a fresh cluster converges in one command.

## 5. Target manifests

### 5.1 File layout

```
deploy/kubernetes/
├── stocker-informer.yaml   # Namespace + ConfigMap + Deployment
└── secret.yaml             # Secret template (placeholders only)
```

No other files. No Service. No Ingress. No HPA.

### 5.2 `deploy/kubernetes/stocker-informer.yaml`

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

Notes:

- `stock.gems.v1.#` is a valid plain YAML scalar: `#` only starts a comment
  when preceded by whitespace. It is left unquoted to match stocker-store's
  `stock.update.v1.#` style.
- Resource order (`ports`, `envFrom`, probes, `resources`, `securityContext`)
  follows stocker-store so diffs across repositories line up.
- `protocol: TCP` is omitted on the port, as in stocker-store.
- `S3_BUCKET: REPLACE_ME` is an operator-filled ConfigMap value, exactly like
  stocker-store's `STOCK_TTL`/environment values are operator-owned.

### 5.3 `deploy/kubernetes/secret.yaml`

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

`secretRef` wiring in the Deployment (`stocker-informer-env`) is unchanged in
name, so the Secret name and the `envFrom` entry stay in step.

### 5.4 ConfigMap keys

| Key | Value | Why |
| --- | --- | --- |
| `RABBITMQ_INPUT_EXCHANGE` | `stocker` | Shared topic exchange; stocker-store declares the same object |
| `RABBITMQ_INPUT_EXCHANGE_TYPE` | `topic` | Matches the existing exchange type, so declarations stay idempotent |
| `RABBITMQ_INPUT_ROUTING_KEY` | `stock.gems.v1.#` | Binding key; `#` matches every gem signal, including deeper keys |
| `RABBITMQ_QUEUE` | `stock.gems.v1.informer` | Service-owned queue, named after the stream it consumes |
| `RABBITMQ_PREFETCH` | `1` | One in-flight message per replica |
| `RABBITMQ_CONNECT_TIMEOUT_SECONDS` | `10` | Unchanged |
| `S3_ENDPOINT` | `http://192.168.1.31:3900` | Garage |
| `S3_REGION` | `us-east-1` | Garage default |
| `S3_TIMEOUT_SECONDS` | `10` | Unchanged |
| `S3_BUCKET` | `REPLACE_ME` | Operator picks the summarizer bucket |
| `GOTOSOCIAL_INSTANCE` | `https://wheeli.social` | Instance base URL |
| `GOTOSOCIAL_USER` | `bot` | Metadata only; auth is the token |
| `GOTOSOCIAL_AUDIENCE` | `global` | Post visibility |
| `OLLAMA_ENABLED` | `true` | Cora-9 rewrite on |
| `OLLAMA_BASE_URL` | `http://192.168.1.71:11434` | Ollama host |
| `OLLAMA_MODEL` | `gemma4:26b` | Must be pulled on that host |
| `OLLAMA_TIMEOUT_SECONDS` | `120` | Per-rewrite bound |
| `OLLAMA_TEMPERATURE` | `0.7` | Persona warmth |
| `OLLAMA_MAX_TOKENS` | `1024` | `num_predict` |
| `OLLAMA_MAX_OUTPUT_CHARS` | `0` | Unlimited |

Secret keys: `RABBITMQ_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`,
`GOTOSOCIAL_TOKEN`. Everything else is ConfigMap-only, matching the
gold-standard split.

## 6. RabbitMQ topology change

### 6.1 Binding semantics

The informer binds queue `stock.gems.v1.informer` to exchange `stocker` with
key `stock.gems.v1.#`. In AMQP topic syntax, `#` matches zero or more
dot-separated words, so the binding catches `stock.gems.v1`,
`stock.gems.v1.AAPL`, `stock.gems.v1.NASDAQ.AAPL`, and any future suffix. The
consumer code already passes the routing key straight to `QueueBind`; only
the configured value changes.

### 6.2 Shared exchange ownership

`stocker` is the fleet exchange that stocker-store already declares as
`topic`, durable. The informer declares it with the same arguments on every
connect (`durable=true`, `autoDelete=false`, `internal=false`), which is
idempotent when the exchange exists and creates a compatible object when it
does not. The queue and binding are owned by the informer and are recreated
on connect if missing. If the existing exchange has different arguments, the
broker answers `PRECONDITION_FAILED` (406); that is a configuration fault,
not something the consumer can repair.

The readiness probe passively declares the exchange and queue, so it turns
ready only after the exchange exists with type `topic` and the queue exists as
durable and non-exclusive.

### 6.3 Producer contract

Nothing in this repository publishes to `stocker`; the producer is external
(README currently names `stocker-summarizer`). The deployment assumes that
producer emits protobuf `stockstore.v1.Stock` bodies under `stock.gems.v1.*`.
That contract must be true before the informer is expected to publish
anything. Publishing under the old `stock.summary` exchange or key leaves the
new queue silent; readiness still shows green because the topology is
correct.

### 6.4 S3 layout is unaffected

The S3 key is built from the protobuf `Stock.exchange` and `Stock.symbol`
(`{EXCHANGE}/{SYMBOL}/summary.md`). The RabbitMQ exchange name never appears in
the key, so no Garage data moves when the broker topology changes.

### 6.5 Rollout and cleanup

1. Confirm the producer publishes `stock.gems.v1.*` to `stocker`.
2. Apply `stocker-informer.yaml` and the filled `secret.yaml`; the consumer
   creates the queue and binding on its first connect.
3. Verify readiness, one end-to-end post, and the log line
   `rabbitmq consumer connected` carrying the new exchange, routing key, and
   queue.
4. Once the new consumer is healthy, remove the old queue and binding:
   `rabbitmqctl delete_queue stocker-informer` and
   `rabbitmqctl list_bindings` to confirm no `stock.summary` binding remains.
   Delete the `stock.summary` exchange only after every other consumer has
   moved.
5. Rollback is the reverse ConfigMap edit: restore the old exchange, routing
   key, and queue values. Messages published under `stock.gems.v1.*` in the
   meantime are not replayed into the old queue.

### 6.6 Impact on existing deployments

Any host or quadlet deployment that relied on the compiled-in defaults now
picks up the new topology on upgrade. The shipped
`deploy/quadlet/.env.podman.example` is updated, and operators must update
their live `~/.config/stocker-informer/.env.podman` in the same window if they
have not migrated to the new topology. Explicit values in that env file
override the new defaults, which makes the migration opt-in per host.

## 7. Code and documentation touchpoints

| Area | Change |
| --- | --- |
| `internal/config/config.go` | Three default constants: exchange `stocker`, routing key `stock.gems.v1.#`, queue `stock.gems.v1.informer` |
| `internal/config/config_test.go` | Expected defaults in `TestLoadRabbitMQDefaults` and the `TestLoadRabbitMQNameVariables` table |
| `internal/rabbitmq/consumer_test.go` | `testCfg()` values plus delivery routing-key fixtures and log assertions |
| `internal/rabbitmq/probe_test.go` | Fake 404 messages and name assertions driven by `testCfg()` |
| `internal/rabbitmq/consumer_live_test.go` | Comment only; the live test builds its own unique topology |
| `deploy/quadlet/.env.podman.example` | New values; variable names unchanged |
| `README.md` | Environment table, producer section, topology bullets, end-to-end check, structure tree, UID |
| `AGENTS.md` | Architecture defaults, environment defaults, structure, Kubernetes entry points |
| `docs/deployment.md` | Live operator sections (topology checks, smoke test); historical tasks above the manual check stay |
| `docs/design-kubernetes-deployment.md`, `docs/plan-kubernetes-deployment.md` | Marked superseded with a pointer here; content left for history |

No behavioural code changes: the consumer, probe, publisher, and rewriter are
untouched. `defaultRabbitMQURL` and the URL validation/redaction helpers are
untouched (D9).

## 8. Runtime UID alignment

The gold-standard security context runs the container as UID/GID `65532`.
`Containerfile` currently declares `USER 65534:65534`. Kubernetes
`runAsUser`/`runAsGroup` override the image `USER`, so the pod would work
either way: distroless static ships both `nonroot` (65532) and `nobody`
(65534), the binary is installed root-owned `0755` at `/stocker-informer`,
and the process writes nothing.

The image `USER` is still changed to `65532:65532` so that `podman run`
(quadlet path) and Kubernetes use one UID. Two UIDs for one workload buys
nothing and makes filesystem troubleshooting ambiguous.

Verification before rollout:

1. `grep -n '^USER' Containerfile` reports `USER 65532:65532`.
2. `podman build -t stocker-informer:uidcheck -f Containerfile .` then
   `podman inspect --format '{{.Config.User}}' stocker-informer:uidcheck`
   reports `65532:65532`.
3. `podman run --rm --user 65532:65532 stocker-informer:uidcheck` starts the
   binary and exits `1` with a `load config: invalid configuration: ...`
   message (required env vars absent). That proves the binary executes as
   65532 without a writable root filesystem.
4. Optional full check: run the image with the required env vars and confirm
   `/health` responds on `:8080`.

If step 2 or 3 is unavailable (no network for the private module), the
fallback evidence is the static binary mode and the fact that distroless
defines `nonroot` as 65532.

## 9. Conformity checklist

Every row is checked against `stocker-store.yaml` and `secret.yaml`.

| Check | stocker-store | stocker-informer target |
| --- | --- | --- |
| Namespace `stocker` declared in main file | yes | yes |
| Two files in `deploy/kubernetes/` | yes | yes |
| Plain `app` label for selector and pod | yes | yes |
| No `app.kubernetes.io/*` labels | yes | yes |
| No labels on Deployment/ConfigMap/Secret metadata | yes | yes |
| No imagePullSecrets | yes | yes |
| `imagePullPolicy: Always` on `:latest` | yes | yes |
| `replicas: 1` | yes | yes |
| securityContext exact match | yes | yes |
| resources block exact match | yes | yes |
| Readiness probe on named port | yes (`tcpSocket`) | yes (`httpGet /health`, D6) |
| No liveness probe | yes | yes |
| Service present | yes (inbound gRPC) | no (no inbound traffic, D7) |
| Secret template comments and placeholders | yes | yes |
| In-cluster broker hostname | yes | yes |
| Real credentials absent from git | yes | yes |

## 10. Risks and non-goals

- **Registry auth (D3).** Removing `wheeli-registry` assumes node-level
  registry access, as in stocker-store. Verify an image pull after rollout.
- **Producer timing (6.3).** The informer can be green while silent if the
  producer still publishes to `stock.summary`.
- **Old queue residue (6.5).** The old queue keeps unconsumed messages until
  it is deleted; they are not migrated.
- **Host env drift (6.6).** Host deployments with no explicit RabbitMQ values
  move to the new topology on upgrade.
- **Non-goals:** HPA, PodDisruptionBudget, NetworkPolicy, Ingress, Service
  mesh annotations, Helm/Kustomize packaging, `readOnlyRootFilesystem`
  (stocker-store does not set it), and any change to the pipeline logic.
