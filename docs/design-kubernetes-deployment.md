# Kubernetes Deployment Design: stocker-informer

> **Superseded 2026-11-21** by
> [design-kubernetes-conformity.md](design-kubernetes-conformity.md). The
> three-file layout, `app.kubernetes.io/*` labels, and `wheeli-registry`
> pull secret described here are no longer used. Kept for history.

## Overview

`stocker-informer` is a RabbitMQ consumer/worker: it consumes summary
notifications from exchange `stock.summary` (routing key
`stock.summary.available.v1`), reads the summary markdown object from
Garage/S3 (`{EXCHANGE}/{SYMBOL}/summary.md`), optionally rewrites it with
Ollama (Cora-9 persona), and posts it to GoToSocial (global timeline or a
direct message). It also serves a minimal HTTP health endpoint on port `8080`
(path `/health`) — the only inbound listener in the service.

This design covers a plain-vanilla Kubernetes deployment of
`git.wheeli.ca/brian/stocker-informer:latest` into the shared `stocker`
namespace.

## Scope

- New directory `deploy/kubernetes/` in this repository holding plain-vanilla
  Kubernetes YAML (no Helm, no Kustomize, no CRDs).
- Manifests: `configmap.yaml`, `secret.yaml`, `deployment.yaml`. **No
  Service** — the `/health` listener exists for pod-level probes only; nothing
  else connects inbound.
- The `stocker` namespace and the `wheeli-registry` pull secret are
  cluster-level prerequisites (see the plan).
- Out of scope: GoToSocial, Garage, RabbitMQ, Ollama (all operator-provided).

## Global conventions (identical across all 12 stocker repositories)

These conventions are the same in every stocker-* deployment document.
`<service>` means the service name of the repository the document belongs to
(e.g. `stocker-informer`).

- **Namespace:** `stocker` (all resources).
- **Image:** `git.wheeli.ca/brian/<service>:latest` with
  `imagePullPolicy: Always` (because the tag is `:latest`).
- **Registry auth:** `kubernetes.io/dockerconfigjson` Secret named
  `wheeli-registry` in namespace `stocker`, referenced from the Pod spec via
  `imagePullSecrets`.
- **Labels:** `app.kubernetes.io/name=<service>` and
  `app.kubernetes.io/part-of=stocker` on the Deployment, the Service (if any),
  and the Pod template labels. The Deployment `selector.matchLabels` uses
  `app.kubernetes.io/name=<service>`.
- **Replicas:** `1`. No HPA, no anti-affinity.
- **Resources:** requests `cpu: 50m`, `memory: 128Mi`; limits `cpu: 500m`,
  `memory: 512Mi`. Exception: `stocker-evaluator` uses limit `memory: 1Gi`.
- **Security:** container-level `securityContext` with
  `runAsNonRoot: true` and `allowPrivilegeEscalation: false`.
- **Service objects:** none, except `stocker-store` (ClusterIP
  `stocker-store`, port `3500`, name `grpc`).
- **Manifest layout:** plain-vanilla YAML under `deploy/kubernetes/` — one
  file per resource kind. No Helm, no Kustomize, no CRDs.
- **Credentials:** committed Secret manifests use `REPLACE_ME` placeholders.
  Real values for local fill-in live in `/home/brian/workspace/credentials.md`;
  long-lived tokens must never be written into committed manifests or docs.
- **Infrastructure endpoints (use exactly these):**
  - Kafka: `192.168.1.71:9092,192.168.1.75:9092` — do NOT use the
    `kafka1.lan` / `kafka2.lan` / `kafka-*.internal` names that appear in
    in-repo sample files.
  - RabbitMQ: `amqp://stocker:REPLACE_ME@192.168.1.48:5672/` (user
    `stocker`, vhost `/`). `guest` is rejected by the broker from
    non-loopback sources.
  - Ollama: `http://192.168.1.71:11434`.
  - Garage/S3 and Postgres: per-service values in the Configuration table
    below.

## Architecture

- **Namespace:** `stocker`.
- **Kind set:** ConfigMap + Secret + Deployment only. **No Service** — the
  worker needs no inbound reachability; the port-8080 health listener is used
  by Kubernetes probes only.
- **Registry auth:** `imagePullSecrets: [wheeli-registry]`.
- **Deployment strategy:** default rolling update, acceptable with
  `replicas: 1`.
- **Image:** built by `Containerfile` (distroless, runs as UID 65534, binary
  `/stocker-informer`). Built and pushed by `deploy/push.sh` on the host —
  nothing is built inside the cluster.

## Resource requirements

- CPU: requests `50m`, limits `500m`.
- Memory: requests `128Mi`, limits `512Mi`.

## Configuration (Environment Variables)

Variable names exactly as read by `internal/config/config.go`. Six variables
are **required** at startup (startup fails if any is missing):
`GOTOSOCIAL_INSTANCE`, `GOTOSOCIAL_USER`, `GOTOSOCIAL_TOKEN`, `S3_BUCKET`,
`S3_ACCESS_KEY`, `S3_SECRET_KEY`.

### Set in ConfigMap `stocker-informer-config`

| Variable | Value | Notes |
|---|---|---|
| `RABBITMQ_INPUT_EXCHANGE` | `stock.summary` | Code default. |
| `RABBITMQ_INPUT_EXCHANGE_TYPE` | `topic` | Code default. |
| `RABBITMQ_INPUT_ROUTING_KEY` | `stock.summary.available.v1` | Code default. |
| `RABBITMQ_QUEUE` | `stocker-informer` | Consumer queue (code default). |
| `RABBITMQ_PREFETCH` | `1` | Code default. |
| `RABBITMQ_CONNECT_TIMEOUT_SECONDS` | `10` | Code default. |
| `S3_ENDPOINT` | `http://192.168.1.31:3900` | Code default (this Garage node). |
| `S3_REGION` | `us-east-1` | Code default; Garage ignores it. |
| `S3_TIMEOUT_SECONDS` | `10` | Code default. |
| `S3_BUCKET` | `REPLACE_ME` | **Required.** The bucket holding `{EX}/{SYM}/summary.md` written by `stocker-summarizer`. Known buckets on this Garage: `stocker`, `stocker-news`. Must match what the summarizer writes. |
| `GOTOSOCIAL_INSTANCE` | `https://wheeli.social` | **Required.** |
| `GOTOSOCIAL_USER` | `bot` | **Required.** |
| `GOTOSOCIAL_AUDIENCE` | `global` | `global` \| `local` \| `direct`. Use `direct` + `GOTOSOCIAL_RECIPIENT` to DM a handle instead. |
| `OLLAMA_ENABLED` | `true` | `false` publishes the original summary unchanged (skips LLM). |
| `OLLAMA_BASE_URL` | `http://192.168.1.71:11434` | Code default. |
| `OLLAMA_MODEL` | `gemma4:26b` | Code default; must be pulled on the Ollama host. |
| `OLLAMA_TIMEOUT_SECONDS` | `120` | Code default (1..3600). |
| `OLLAMA_TEMPERATURE` | `0.7` | Code default. |
| `OLLAMA_MAX_TOKENS` | `1024` | Code default. |
| `OLLAMA_MAX_OUTPUT_CHARS` | `0` | `0` = unlimited (code default). |

Not set (code defaults apply): `GOTOSOCIAL_RECIPIENT` (only for `direct`
audience), `OLLAMA_SYSTEM_PROMPT` (built-in Cora-9 prompt), `S3_PATH_PREFIX`
(empty).

### Set in Secret `stocker-informer-env`

| Variable | Placeholder value | Real value from |
|---|---|---|
| `RABBITMQ_URL` | `amqp://stocker:REPLACE_ME@192.168.1.48:5672/` | `/home/brian/workspace/credentials.md` → RabbitMQ (`stocker`). Optional (a built-in default exists) but set explicitly for in-cluster correctness. |
| `S3_ACCESS_KEY` | `REPLACE_ME` | `/home/brian/workspace/credentials.md` → Garage/NAS Key ID. |
| `S3_SECRET_KEY` | `REPLACE_ME` | `/home/brian/workspace/credentials.md` → Garage/NAS Secret Key. |
| `GOTOSOCIAL_TOKEN` | `REPLACE_ME` | GoToSocial bot API token (operator-provided; do **not** write it into docs). |

## Message flow

```
RabbitMQ exchange stock.summary (topic)
   routing key stock.summary.available.v1
        ▼
stocker-informer (queue stocker-informer)
        │  GET {S3_BUCKET}/{EX}/{SYM}/summary.md  (S3 192.168.1.31:3900)
        │  optional Ollama rewrite (Cora-9, gemma4:26b)
        ▼
GoToSocial post (global timeline or direct message)
```

## Connectivity & probes

- **Readiness probe:** `httpGet` `path: /health`, `port: 8080`. Readiness is
  sticky in the code (starts false until first successful work path) — a
  readiness probe is the correct fit.
- **Liveness probe:** `httpGet` `path: /health`, `port: 8080`.
- **Container port:** `8080`, name `health` (declare it so probes and humans
  share one name).
- **Service exposure:** **none.** The `/health` listener is for probes only;
  do not create a Service for this pod.
- **Outbound connectivity needed from the pod:** RabbitMQ
  `192.168.1.48:5672`, Garage `192.168.1.31:3900`, Ollama
  `192.168.1.71:11434`, GoToSocial `https://wheeli.social`.
- **No Kafka, no gRPC, no Postgres.**

## Dependencies

- Garage bucket with summary objects written by `stocker-summarizer`
  (endpoint `192.168.1.31:3900`).
- Ollama at `192.168.1.71:11434` with model `gemma4:26b` (or set
  `OLLAMA_ENABLED=false`).
- GoToSocial instance + bot token.
- RabbitMQ broker at `192.168.1.48:5672` (operator-provided).
- Registry `git.wheeli.ca` + `wheeli-registry` pull secret.

## Manifest layout (exact files & resource names)

| File (under `deploy/kubernetes/`) | Kind | Name | Namespace |
|---|---|---|---|
| `configmap.yaml` | `ConfigMap` | `stocker-informer-config` | `stocker` |
| `secret.yaml` | `Secret` | `stocker-informer-env` | `stocker` |
| `deployment.yaml` | `Deployment` | `stocker-informer` | `stocker` |

The Deployment consumes both via `envFrom` (`configMapRef` + `secretRef`).

## Verification (what "healthy" looks like)

- Pod `Running`, `READY 1/1` (readiness flips once the consumer is up).
- `kubectl exec`-free health check: `curl -f http://<podIP>:8080/health` from
  another pod (or `kubectl port-forward`) returns `200`.
- Logs show a successful consume of a `stock.summary.available.v1` message and
  a GoToSocial post.
