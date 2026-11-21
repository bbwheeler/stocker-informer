# Kubernetes Deployment Plan: stocker-informer

> **Superseded 2026-11-21** by
> [plan-kubernetes-conformity.md](plan-kubernetes-conformity.md). Do not
> execute this plan against the current tree; it describes the retired
> three-file layout and old RabbitMQ topology.

Companion design doc: `docs/design-kubernetes-deployment.md` (source of truth
for variable names, values, and rationale). Implement the tasks **one at a
time, in order**; each task is self-contained.

## Prerequisites

1. A Kubernetes cluster whose nodes can reach:
   - registry `git.wheeli.ca` (image pull),
   - RabbitMQ `192.168.1.48:5672`,
   - Garage `192.168.1.31:3900`,
   - Ollama `192.168.1.71:11434` (model pulled, or set `OLLAMA_ENABLED=false`),
   - GoToSocial `https://wheeli.social`.
2. Namespace exists:
   ```bash
   kubectl create namespace stocker
   ```
3. Registry pull secret exists (create once per cluster). Real user/token:
   `/home/brian/workspace/credentials.md` → `git.wheeli.ca`:
   ```bash
   kubectl create secret docker-registry wheeli-registry \
     --docker-server=git.wheeli.ca \
     --docker-username=REPLACE_ME \
     --docker-password=REPLACE_ME \
     -n stocker
   ```
4. The image `git.wheeli.ca/brian/stocker-informer:latest` has been built and
   pushed (this repo's `deploy/push.sh` does that from a host with a Go
   toolchain and git credentials for `git.wheeli.ca`).

## Tasks

### Task 1: Create `deploy/kubernetes/`

Create the directory `deploy/kubernetes/` at the repository root. It will
hold exactly three files (created in Tasks 2–4):

| File | Kind | Name |
|---|---|---|
| `deploy/kubernetes/configmap.yaml` | `ConfigMap` | `stocker-informer-config` |
| `deploy/kubernetes/secret.yaml` | `Secret` | `stocker-informer-env` |
| `deploy/kubernetes/deployment.yaml` | `Deployment` | `stocker-informer` |

No other files. No `service.yaml` (the port-8080 `/health` endpoint is for
Kubernetes probes only).

### Task 2: Write `deploy/kubernetes/configmap.yaml`

Create `deploy/kubernetes/configmap.yaml` with exactly this content
(`S3_BUCKET` is the **one value you must pick**: it must be the bucket on
Garage `192.168.1.31:3900` where `stocker-summarizer` writes
`{EX}/{SYM}/summary.md`; known bucket names there are `stocker` and
`stocker-news`):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: stocker-informer-config
  namespace: stocker
  labels:
    app.kubernetes.io/name: stocker-informer
    app.kubernetes.io/part-of: stocker
data:
  # RabbitMQ input (code defaults, set explicitly).
  RABBITMQ_INPUT_EXCHANGE: "stock.summary"
  RABBITMQ_INPUT_EXCHANGE_TYPE: "topic"
  RABBITMQ_INPUT_ROUTING_KEY: "stock.summary.available.v1"
  RABBITMQ_QUEUE: "stocker-informer"
  RABBITMQ_PREFETCH: "1"
  RABBITMQ_CONNECT_TIMEOUT_SECONDS: "10"
  # Garage/S3 (endpoint + region are code defaults).
  S3_ENDPOINT: "http://192.168.1.31:3900"
  S3_REGION: "us-east-1"
  S3_TIMEOUT_SECONDS: "10"
  # REQUIRED: bucket holding {EX}/{SYM}/summary.md objects written by stocker-summarizer.
  S3_BUCKET: "REPLACE_ME"
  # GoToSocial (instance/user are required at startup).
  GOTOSOCIAL_INSTANCE: "https://wheeli.social"
  GOTOSOCIAL_USER: "bot"
  GOTOSOCIAL_AUDIENCE: "global"
  # Ollama rewrite (Cora-9 persona). OLLAMA_ENABLED=false skips the LLM entirely.
  OLLAMA_ENABLED: "true"
  OLLAMA_BASE_URL: "http://192.168.1.71:11434"
  OLLAMA_MODEL: "gemma4:26b"
  OLLAMA_TIMEOUT_SECONDS: "120"
  OLLAMA_TEMPERATURE: "0.7"
  OLLAMA_MAX_TOKENS: "1024"
  OLLAMA_MAX_OUTPUT_CHARS: "0"
```

Then validate it parses:

```bash
kubectl apply --dry-run=client -f deploy/kubernetes/configmap.yaml
```

### Task 3: Write `deploy/kubernetes/secret.yaml`

Create `deploy/kubernetes/secret.yaml` with exactly this content
(**`REPLACE_ME` stays in the committed file** — fill only in your local copy
before applying; RabbitMQ + Garage values from
`/home/brian/workspace/credentials.md`):

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: stocker-informer-env
  namespace: stocker
  labels:
    app.kubernetes.io/name: stocker-informer
    app.kubernetes.io/part-of: stocker
type: Opaque
stringData:
  RABBITMQ_URL: "amqp://stocker:REPLACE_ME@192.168.1.48:5672/"
  S3_ACCESS_KEY: "REPLACE_ME"
  S3_SECRET_KEY: "REPLACE_ME"
  # GoToSocial bot API token (operator-provided).
  GOTOSOCIAL_TOKEN: "REPLACE_ME"
```

Then validate:

```bash
kubectl apply --dry-run=client -f deploy/kubernetes/secret.yaml
```

### Task 4: Write `deploy/kubernetes/deployment.yaml`

Create `deploy/kubernetes/deployment.yaml` with exactly this content:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stocker-informer
  namespace: stocker
  labels:
    app.kubernetes.io/name: stocker-informer
    app.kubernetes.io/part-of: stocker
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: stocker-informer
  template:
    metadata:
      labels:
        app.kubernetes.io/name: stocker-informer
        app.kubernetes.io/part-of: stocker
    spec:
      imagePullSecrets:
        - name: wheeli-registry
      containers:
        - name: stocker-informer
          image: git.wheeli.ca/brian/stocker-informer:latest
          imagePullPolicy: Always
          securityContext:
            runAsNonRoot: true
            allowPrivilegeEscalation: false
          envFrom:
            - configMapRef:
                name: stocker-informer-config
            - secretRef:
                name: stocker-informer-env
          ports:
            - name: health
              containerPort: 8080
              protocol: TCP
          readinessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 30
            periodSeconds: 30
          resources:
            requests:
              cpu: 50m
              memory: 128Mi
            limits:
              cpu: 500m
              memory: 512Mi
```

Then validate:

```bash
kubectl apply --dry-run=client -f deploy/kubernetes/deployment.yaml
```

### Task 5: Apply and verify

Apply all three manifests:

```bash
kubectl apply -f deploy/kubernetes/
```

Wait for the rollout and inspect:

```bash
kubectl -n stocker rollout status deployment/stocker-informer --timeout=120s
kubectl -n stocker get pods -l app.kubernetes.io/name=stocker-informer
kubectl -n stocker describe pod -l app.kubernetes.io/name=stocker-informer
kubectl -n stocker logs deployment/stocker-informer --tail=100
```

Probe check (no curl inside the distroless image — check from outside):

```bash
kubectl -n stocker get pod -l app.kubernetes.io/name=stocker-informer -o jsonpath='{.items[0].status.podIP}'
# from a host that can reach the pod network:
curl -fsS http://<podIP>:8080/health
# or:
kubectl -n stocker port-forward pod/<pod-name> 8080:8080 &  curl -fsS http://127.0.0.1:8080/health
```

Expected:

- Pod `Running`, `READY 1/1`, `0` restarts (after the consumer is up).
- `/health` returns `200` once ready (readiness is sticky in the code: it
  starts false).
- Missing required vars (`GOTOSOCIAL_TOKEN`, `S3_BUCKET`, `S3_ACCESS_KEY`,
  `S3_SECRET_KEY`) show up as a startup `fatal error` log naming the
  variables — fix the Secret, don't paper over it.

**One-shot message-flow smoke test:**

1. Put a summary object in the bucket (any `{EX}/{SYM}/summary.md`, e.g.
   `NASDAQ/AAPL/summary.md`) on Garage `192.168.1.31:3900` — or wait for a
   real one from `stocker-summarizer`.
2. Publish a notification to the input exchange. **The message body must be a
   protobuf-encoded `stockstore.v1.Stock`** (`symbol` + `exchange` fields; see
   `internal/rabbitmq/consumer.go` `decodeStock`) — a JSON body will fail the
   protobuf decode and be rejected as a decode error. Simplest: reuse a real
   `stock.summary.available.v1` message produced by `stocker-summarizer`, or
   generate one with a throwaway Go module:

   ```bash
   mkdir -p /tmp/stock-signal && cd /tmp/stock-signal
   cat > main.go <<'EOF'
   package main

   import (
   	"os"

   	stockstorev1 "git.wheeli.ca/brian/stocker-store/proto/v1"
   	"google.golang.org/protobuf/proto"
   )

   func main() {
   	b, err := proto.Marshal(&stockstorev1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"})
   	if err != nil {
   		panic(err)
   	}
   	os.Stdout.Write(b)
   }
   EOF
   go mod init stock-signal
   go mod tidy
   go run . > /tmp/stock-signal.bin
   rabbitmqadmin -H 192.168.1.48 -u stocker -p REPLACE_ME publish \
     exchange=stock.summary routing_key=stock.summary.available.v1 \
     < /tmp/stock-signal.bin
   ```
   (or capture the real payload: `rabbitmqadmin ... get_queue_message
   queue=<summarizer output queue> count=1` and republish it).
3. Within seconds: pod log shows the summary fetch + GoToSocial post (or an
   explicit fallback/error naming the failing step), and the post appears on
   the GoToSocial global timeline (or the configured direct recipient).
4. PASS = post delivered (or a clean logged skip with a reason). FAIL modes:
   `S3 404` (wrong bucket/keys), `401` from GoToSocial (bad token),
   RabbitMQ auth error (user must be `stocker`, not `guest`).
