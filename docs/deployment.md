# Deployment Simplification Implementation Plan

**Goal:** Replace the multi-piece, root/sudo stocker-informer deployment with a SINGLE rootless podman quadlet (systemd `--user`) that consumes a pre-published image from the `git.wheeli.ca/brian` registry, mirror the sibling `stocker-store`'s "proper deployment" pattern (`deploy/push.sh` + one `deploy/quadlet/<name>.container`), and remove the now-unnecessary root install script, the standalone `Build` unit, the old publisher, and the shipped `.env.podman`.

**Architecture:** `deploy/push.sh` builds the image from the repo-root `Containerfile` and pushes it to `git.wheeli.ca/brian/stocker-informer:latest`. On a host, the operator hands-writes `~/.config/stocker-informer/.env.podman` (NOT shipped) and `symlink`s the single `deploy/quadlet/stocker-informer.container` unit into their user systemd `--user` directory (`/etc/containers/systemd` or `~/.config/systemd/user`), then `systemctl enable --now stocker-informer`. The quadlet runs the registry image read-only with a scratch `--tmpfs`.

**Tech Stack:** Go 1.25; podman quadlet (`podman build`/`podman push`); systemd `--user` units; `git.wheeli.ca/brian` (Forgejo) container registry.

**Spec:** The deployment requirements are the user goal in the task brief (items 1–5) and the `stocker-store` reference pattern. This plan is the single source of truth for the target state.

## Global Constraints

- **Module:** `github.com/example/stocker-informer`. **Go:** 1.25.
- **Build:** `go build ./cmd/server/`. **Test:** `go test ./...` (a no-op — no test files).
- **Kafka dep:** `github.com/segmentio/kafka-go`. **No CI, no Makefile, no linter** — only standard Go conventions and the `bash -n` syntax check.
- **Registry + image name (single source of truth):** `git.wheeli.ca/brian/stocker-informer:latest`. The quadlet's `Image=` and `push.sh`'s pushed name MUST match this verbatim.
- **User config path (single source of truth):** `~/.config/stocker-informer/.env.podman` (the quadlet references it as `%h/.config/stocker-informer/.env.podman`).
- **`push.sh` MUST be mode `0755`** (executable). The quadlet MUST be mode `0644`.
- **Six required env vars** (all validated non-empty; empty → the app exits `1`): `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_CONSUMER_GROUP`, `GOTOSOCIAL_INSTANCE`, `GOTOSOCIAL_USER`, `GOTOSOCIAL_TOKEN`.
- **Do not modify** `Containerfile` (keep as-is), `cmd/`, or `internal/`.
- **After all tasks:** the working tree must be `git status`-clean in spirit (no stray/new files except `docs/deployment.md`), and no references to `install.sh`, `publish.sh`, or `stocker-informer.build` may remain in any tracked `*.md` except historical mentions inside `review.md` (see Task 18).

---

## Decision Log

### D1 — Single quadlet: what to change vs. the current one

Mirror the `stocker-store` reference **and** keep the hardening. Rationale per line:

| Setting | Current stocker-informer | Reference `stocker-store` | **Chosen** for stocker-informer | Why |
| --- | --- | --- | --- | --- |
| `[Unit]` | *(absent)* | `After=network-online.target` | **Add it** | Correct ordering: don't start before the user network is up. Reference parity. |
| `Image=` | `git.wheeli.ca/brian/stocker-informer:latest` | same shape | **Keep** | Must equal what `push.sh` publishes (Global Constraint). |
| `EnvironmentFile=` | `%h/.config/stocker-informer/.env.podman` | same shape | **Keep** | Operator-owned env file path (Global Constraint). |
| `PodmanArgs=--read-only` | **present** | *(absent)* | **Keep** | The runtime image is `gcr.io/distroless/static-debian12` running as non-root `65534` with a **static** (non-dynamic linker, no libc at runtime) binary. A read-only root FS is a genuine, zero-cost hardening win for a Go daemon that never writes files. Diverges from the reference on purpose — documented. |
| `PodmanArgs=--tmpfs=/tmp:rw,noexec,nosuid,size=64m` | **present** | *(absent)* | **Keep** | Lets the container have a small writable scratch `/tmp` while the rest stays read-only; `noexec,nosuid` block executing and setuid'ing files off it. Diverges from the reference on purpose — documented. |
| `AutoUpdate=` | `local` | `registry` | **Change to `registry`** | `local` auto-updates from a **local** image file, which we no longer build on the host — it's meaningless here. `registry` re-pulls a new `:latest` on auto-update — correct for consuming a registry-published image. Matches reference; also resolves review.md item #28. |
| `[Service] Restart=` | `always` | `always` | **Keep** | — |
| `[Service] RestartSec=` | `5` | `10` | **Change to `10`** | Reference parity; slightly gentler restart cadence. |
| `[Install] WantedBy=` | `default.target` | `default.target` | **Keep** | Start on user login. |

> **Note on the two "divergences" (read-only + tmpfs).** The reference intentionally omits hardening, and a naive mirror would drop it. We deliberately retain it for stocker-informer because it is safe and effective for this specific static/non-root image. Both lines are kept **and** called out in the README quickstart so an operator can see them. This is a conscious, documented deviation from "mirror the reference exactly" in favor of "mirror the reference's *structure* while keeping a defensible hardening win."

### D2 — `push.sh`: mirror the reference verbatim

Keep the reference's single-tag (`:latest`) build + push (drop the old `publish.sh`'s second UTC-timestamp tag). `:latest` is exactly what the quadlet consumes; `podman push` already prints the digest, which is how an operator pins a specific revision. Simplest and matches the reference.

### D3 — AGENTS.md: NO CHANGE

`AGENTS.md` documents module, Go version, build/test commands, package structure, env vars, and the credentials pointer. It contains **no** reference to `deploy/`, `install.sh`, `publish.sh`, the `.build` unit, or `.env.podman` (verified by grep). It stays accurate after this work. **Do not modify it.**

### D4 — `review.md`: LEAVE AS-IS

`review.md` is a point-in-time audit snapshot. It references `install.sh`, `publish.sh`, and `.env.podman`, but only inside historical numbered findings (#22, #23, #31, #32) describing what *was* in the repo at audit time — **not** as operational instructions. Modifying it would corrupt the historical record. **Leave it untouched.** (Optional, only if a reviewer disagrees: prepend a one-line `> _Historical audit snapshot (as of <date>); artifact names it cites are superseded by the single-quadlet deployment documented in `docs/deployment.md`._`)

### D5 — `go.sum` (keep the caveat, soften from "install.sh")

`go.sum` is **not committed** (verified absent) and the `Containerfile`'s `COPY . .` + `go build` need module hashes resolved. `go mod tidy` generates it locally into the **repo root**, which is then present in the **build context** (`podman build` of `$PROJECT_DIR`) — so `push.sh`'s build succeeds even with `go.sum` uncommitted. The old, stronger claim that `deploy/install.sh` would *fail* its copy step is gone with `install.sh`. Keep only a brief, accurate `go.sum` note near the container build.

---

## Target File Inventory

**KEEP (unchanged):**
- `Containerfile` — correct two-stage static/non-root build.
- `deploy/quadlet/stocker-informer.container` — **rewritten** in-place to the single-quadlet target (below).
- All of `cmd/`, `internal/`, `go.mod`, `AGENTS.md`, `review.md`.

**ADD (new):**
- `deploy/push.sh` (mode `0755`) — builds + pushes to the registry.
- `docs/deployment.md` — this document.

**MODIFY:**
- `README.md` — every deployment/quadlet/publish/known-limitations reference to the old model (Tasks 11–17).

**DELETE:**
- `deploy/install.sh` — root/sudo install script (not in reference).
- `deploy/quadlet/stocker-informer.build` — standalone `Build` unit (not in reference).
- `deploy/publish.sh` — replaced by `deploy/push.sh`.
- `.env.podman` (repo root; shipped) — reference ships **no** env file; the operator creates it and the README documents a sample in-line.

**Exact target content — `deploy/push.sh`:**

```bash
#!/usr/bin/env bash
set -euo pipefail

REGISTRY="git.wheeli.ca/brian"
IMAGE_NAME="stocker-informer:latest"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "==> Building image for $REGISTRY"
podman build -t "${REGISTRY}/${IMAGE_NAME}" "$PROJECT_DIR"

echo "==> Pushing image to $REGISTRY"
podman push "${REGISTRY}/${IMAGE_NAME}"

echo "Done. Image pushed to ${REGISTRY}/${IMAGE_NAME}"
```

**Exact target content — `deploy/quadlet/stocker-informer.container` (the single quadlet):**

```ini
[Unit]
After=network-online.target

[Container]
Image=git.wheeli.ca/brian/stocker-informer:latest
EnvironmentFile=%h/.config/stocker-informer/.env.podman
PodmanArgs=--read-only
PodmanArgs=--tmpfs=/tmp:rw,noexec,nosuid,size=64m
AutoUpdate=registry

[Service]
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
```

> The two `PodmanArgs=` lines are quadlet **array-value** syntax (duplicate key appends). See D1.

---

## Implementation Steps

> Ordering rule: each task depends only on prior ones. Deletions are independent of additions (they reference different paths) and are done **before** the README edits so the README never points at a file that has already been replaced. The README tasks (11–17) are all one file (`README.md`) and are applied top-to-bottom (task order) — each is a single bounded `old → new` edit, so they are individually verifiable. Do **not** parallelize Tasks 1–17 (README edits collide on the same file); the final commit + verify (Task 18) is the only gate after.

### Task 1: Delete `deploy/install.sh`

**Why:** A root/sudo install script that copies to `/opt` and `/etc/containers/systemd` and installs a shipped `.env.podman`. The reference `stocker-store` ships no install script; the operator handles unit installation themselves. Removing it is the start of "one quadlet, no root."

- [ ] **Step 1.1: Delete the file**

  ```bash
  git rm deploy/install.sh
  ```

- [ ] **Step 1.2: Confirm it is gone**

  Run: `git status deploy/install.sh`
  Expected: `deleted:    deploy/install.sh`

**Verification:** `ls deploy/install.sh` → `No such file or directory`.

---

### Task 2: Delete `deploy/quadlet/stocker-informer.build`

**Why:** A standalone `[Build]` quadlet that generated `stocker-informer-build.service`. The reference ships no `Build` unit — the image is built/published by `push.sh`, not by a systemd build unit.

- [ ] **Step 2.1: Delete the file**

  ```bash
  git rm deploy/quadlet/stocker-informer.build
  ```

- [ ] **Step 2.2: Confirm**

  Run: `git status deploy/quadlet/stocker-informer.build`
  Expected: `deleted:    deploy/quadlet/stocker-informer.build`

**Verification:** The only file remaining under `deploy/quadlet/` is `stocker-informer.container`: `ls deploy/quadlet/` → `stocker-informer.container`.

---

### Task 3: Create `deploy/push.sh` (mode `0755`)

**Why (D2):** The single "build + publish to registry" entry point, mirroring `stocker-store/deploy/push.sh` verbatim (single `:latest` tag). Consumes the repo-root `Containerfile`; the build context is `"$PROJECT_DIR"` (the repo root), so `go mod tidy`'s generated `go.sum` (if present) is included in `COPY . .`.

- [ ] **Step 3.1: Write the file with EXACT content** (copy the block below verbatim — note the trailing newline and the `#!/usr/bin/env bash` line):

  ```bash
  #!/usr/bin/env bash
  set -euo pipefail

  REGISTRY="git.wheeli.ca/brian"
  IMAGE_NAME="stocker-informer:latest"
  PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

  echo "==> Building image for $REGISTRY"
  podman build -t "${REGISTRY}/${IMAGE_NAME}" "$PROJECT_DIR"

  echo "==> Pushing image to $REGISTRY"
  podman push "${REGISTRY}/${IMAGE_NAME}"

  echo "Done. Image pushed to ${REGISTRY}/${IMAGE_NAME}"
  ```

- [ ] **Step 3.2: Make it executable**

  ```bash
  chmod 0755 deploy/push.sh
  ```

- [ ] **Step 3.3: Syntax-check**

  Run: `bash -n deploy/push.sh`
  Expected: no output (exit 0).

- [ ] **Step 3.4: Confirm mode is `0755` and the pushed name is the registry image**

  Run: `stat -c '%a %n' deploy/push.sh` → Expected: `755 deploy/push.sh`
  Run: `grep -n 'REGISTRY\|IMAGE_NAME\|podman push' deploy/push.sh`
  Expected: `REGISTRY="git.wheeli.ca/brian"` and it ends in `podman push "${REGISTRY}/${IMAGE_NAME}"` (⇒ `git.wheeli.ca/brian/stocker-informer:latest`).

---

### Task 4: Rewrite `deploy/quadlet/stocker-informer.container` (the single quadlet)

**Why (D1):** Replace the two-concern split with the single rootless quadlet that consumes the registry image. Mirror the reference structure (`[Unit]`, `AutoUpdate=registry`, `RestartSec=10`) **and** retain the `--read-only` / `tmpfs` hardening. Same filename, mode `0644`; the only remaining file in `deploy/quadlet/`.

- [ ] **Step 4.1: Overwrite the file with EXACT content** (copy verbatim; note the two distinct `PodmanArgs=` array lines and the blank line between sections):

  ```ini
  [Unit]
  After=network-online.target

  [Container]
  Image=git.wheeli.ca/brian/stocker-informer:latest
  EnvironmentFile=%h/.config/stocker-informer/.env.podman
  PodmanArgs=--read-only
  PodmanArgs=--tmpfs=/tmp:rw,noexec,nosuid,size=64m
  AutoUpdate=registry

  [Service]
  Restart=always
  RestartSec=10

  [Install]
  WantedBy=default.target
  ```

- [ ] **Step 4.2: Ensure mode is `0644`**

  ```bash
  chmod 0644 deploy/quadlet/stocker-informer.container
  ```

- [ ] **Step 4.3: Verify the image name matches `push.sh` and the hardening lines are present**

  Run: `grep -n 'Image=\|PodmanArgs=\|AutoUpdate=' deploy/quadlet/stocker-informer.container`
  Expected, exactly:
  - `Image=git.wheeli.ca/brian/stocker-informer:latest`
  - `PodmanArgs=--read-only`
  - `PodmanArgs=--tmpfs=/tmp:rw,noexec,nosuid,size=64m`
  - `AutoUpdate=registry`

  Run: `stat -c '%a' deploy/quadlet/stocker-informer.container` → Expected: `644`

  Run: `git status deploy/quadlet/` → Expected: `modified:   deploy/quadlet/stocker-informer.container` (only file present).

**Cross-check (consistency, requirement f):** The `Image=` value `git.wheeli.ca/brian/stocker-informer:latest` is identical to the name `push.sh` pushes (Task 3). ✔

---

### Task 5: Delete shipped `.env.podman` (repo root)

**Why:** The reference `stocker-store` does **not** ship a `.env.podman`. The operator creates `~/.config/stocker-informer/.env.podman` themselves; the README will document a sample in-line (Task 14). Deleting the tracked root file enforces that the config is operator-owned and never accidentally committed with real secrets.

- [ ] **Step 5.1: Delete the file**

  ```bash
  git rm .env.podman
  ```

- [ ] **Step 5.2: Confirm**

  Run: `git status .env.podman` → Expected: `deleted:    .env.podman`
  Run: `ls .env.podman` → Expected: `No such file or directory`

> The quadlet still references `%h/.config/stocker-informer/.env.podman` (the operator's home dir) — that path is **not** the deleted repo-root `.env.podman`. No conflict.

---

### Task 6: `README.md` — Prerequisites

**Why:** Remove the `install.sh` (root) and `publish.sh` prerequisite bullets (both now obsolete) and state the correct rootless model.

- [ ] **Step 6.1: Replace the prerequisites section.** The current two lines 63–64:

  ```markdown
  - `deploy/publish.sh` requires an existing `podman login` to `git.wheeli.ca`.
  - `deploy/install.sh` requires root (`sudo`).
  ```

  Replace them (and only those two bullets) with the following three bullets:

  ```markdown
  - `deploy/push.sh` requires an existing `podman login` to `git.wheeli.ca` (no root/sudo).
  - The service is deployed **rootlessly** as a user systemd quadlet (`systemctl --user`) — no root/sudo at deploy time.
  - The operator hands-writes `~/.config/stocker-informer/.env.podman` (the env file is **not** shipped in this repo).
  ```

**Verification:** `grep -n 'install.sh\|publish.sh' README.md` → no matches remaining in the Prerequisites section.

---

### Task 7: `README.md` — Installation (remove root install; keep the `go.sum` note, softened)

**Why:** The `### System install (deploy/install.sh)` subsection documents a now-deleted script (install to `/opt` + `stocker-informer-build.service`). Remove that subsection. Keep the `go.sum` note but rephrase it away from `install.sh` (D5). Also update line 74's note and the lead-in.

- [ ] **Step 7.1: Rewrite the "Installation" section (lines ~66–89) to the target.** Replace everything from the `## Installation` heading through the end of the `### System install (deploy/install.sh)` subsection (i.e., up to just before `## Building`) with:

  ````markdown
  ## Installation

  ### Clone the source

  ```bash
  git clone https://git.wheeli.ca/brian/stocker-informer
  ```

  > **Note:** `go.sum` is not committed to git. Run `go mod download` / `go mod tidy` once locally before building the container image — the build resolves module hashes, and a missing `go.sum` in the build context can make the container build fail. (See `deploy/push.sh` for the registry build+push.)

  Deployment is a single rootless quadlet plus a registry push — see **Publishing to the registry** (`deploy/push.sh`) and **Running the service → systemd / Podman quadlet**.
  ````

  (Nested ```bash fence inside the target above is intentional markdown.)

  > Practical instruction for the developer: delete the old `### System install (deploy/install.sh)` subsection and its numbered next-steps, and replace the line-74 note text with the new note. Everything else in this section (clone) is unchanged.

**Verification:** `grep -n 'System install\|/opt/stocker-informer\|stocker-informer-build\|daemon-reload' README.md` → no matches.

---

### Task 8: `README.md` — "Example env file" (inlined, not "shipped")

**Why (D4/ref):** The env file is operator-owned and **not shipped**, so the heading/intro must change from "shipped template" to an inlined sample the operator copies into `~/.config/stocker-informer/.env.podman`.

- [ ] **Step 8.1: Replace the intro sentence (line 152)** — the sentence before the code fence:

  Current:
  ```markdown
  ### Example env file

  The shipped `.env.podman` template:
  ```

  Replace with:
  ```markdown
  ### Example env file

  You author this file yourself (it is **not** shipped in this repo) at `~/.config/stocker-informer/.env.podman`. Sample:
  ```

  The fenced `# --- Kafka ---` sample block itself (lines 154–167) and the closing sentence on line 169 ("The Kafka trio ships with development example values …") are otherwise fine; optionally adjust the closing sentence to:

  ```markdown
  Fill in the GoToSocial trio before the service will start; the Kafka value `localhost:9092` is a development-only placeholder.
  ```

**Verification:** `grep -n 'The shipped .env.podman template' README.md` → no match; `grep -n '~/.config/stocker-informer/.env.podman' README.md` → present.

---

### Task 9: `README.md` — Running → Container

**Why:** The example `podman run ... --env-file .env.podman` referenced a repo-root `.env.podman` that no longer exists. Change it to use the operator's home-dir env file (the same path the quadlet uses).

- [ ] **Step 9.1: Replace the two `podman run` examples (lines ~234–247).** Replace the block:

  **Current (replace this block):**

  ```bash
  podman run -it --rm --env-file .env.podman --read-only stocker-informer:latest
  ```

  The quadlet hardened form also adds a scratch `--tmpfs`:

  ```bash
  podman run -it --rm --env-file .env.podman \
    --read-only \
    --tmpfs=/tmp:rw,noexec,nosuid,size=64m \
    stocker-informer:latest
  ```

  `--read-only` makes the container root filesystem read-only; the `--tmpfs` provides a small scratch `/tmp`.`

  **Replace it with:**

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

**Verification:** `grep -n 'env-file' README.md` → all use `$HOME/.config/stocker-informer/.env.podman`, none use a bare `env-file .env.podman`.

---

### Task 10: `README.md` — Running → "systemd / Podman quadlet" (the core rewrite)

**Why:** This section currently documents **two** units (`stocker-informer.build` + `stocker-informer.container`), a `stocker-informer-build.service` bring-up, and "installed to `/etc/containers/systemd/` by `install.sh`". Rewrite to the **single** rootless quadlet model, matching the reference and the actual `deploy/quadlet/stocker-informer.container` (Task 4).

- [ ] **Step 10.1: Replace the entire `### systemd / Podman quadlet` subsection (lines ~249–263) with the target below.** (Replace from the `### systemd / Podman quadlet` heading through the closing ```bash fence of the bring-up sequence, i.e., up to just before `## Publishing to the registry`) with:

  **Replace with the following (target content):**

  ````markdown
  ### systemd / Podman quadlet (rootless)

  A **single** unit lives in `deploy/quadlet/`: `stocker-informer.container`. It consumes the registry image (`AutoUpdate=registry` re-pulls a new `:latest` on auto-update) and runs it read-only with a scratch `/tmp`:

  - `[Unit]` `After=network-online.target`.
  - `[Container]` `Image=git.wheeli.ca/brian/stocker-informer:latest`, `EnvironmentFile=%h/.config/stocker-informer/.env.podman`, `PodmanArgs=--read-only`, `PodmanArgs=--tmpfs=/tmp:rw,noexec,nosuid,size=64m`, `AutoUpdate=registry`.
  - `[Service]` `Restart=always`, `RestartSec=10`.
  - `[Install]` `WantedBy=default.target` (starts on user login).

  There is **no** separate `stocker-informer.build` unit and **no** root install script: the image is built and published by `deploy/push.sh` (below).

  Setup (rootless, no sudo; the env file is **not** shipped — you create it):

  ```bash
  # 1. Author the config (fill in GOTOSOCIAL_* + KAFKA_* before starting)
  mkdir -p ~/.config/stocker-informer
  nano ~/.config/stocker-informer/.env.podman   # see "Environment Variables" / "Example env file"

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

  ````

**Verification:**
- `grep -n 'stocker-informer-build\|stocker-informer\.build\|installed to /etc/containers' README.md` → no matches.
- `grep -n 'AutoUpdate=registry\|systemctl --user enable --now stocker-informer' README.md` → both present.
- The section names exactly **one** quadlet unit.

---

### Task 11: `README.md` — "Publishing to the registry" → `deploy/push.sh`

**Why:** The section documents `deploy/publish.sh` (two tags: `:latest` + a UTC timestamp). Replace with the `deploy/push.sh` single-`:latest` model (D2).

- [ ] **Step 11.1: Replace the `## Publishing to the registry` section (lines ~265–272) with:**

  **Replace with the following (target content):**

  ````markdown
  ## Publishing to the registry

  `deploy/push.sh` (no root required) builds the image from the repo-root `Containerfile` and pushes the `:latest` tag to the `git.wheeli.ca/brian` registry:

  ```bash
  ./deploy/push.sh
  ```

  It runs, equivalently:

  - `podman build -t git.wheeli.ca/brian/stocker-informer:latest <repo-root>`
  - `podman push git.wheeli.ca/brian/stocker-informer:latest`

  It requires an existing `podman login` to `git.wheeli.ca` beforehand. The pushed `:latest` is exactly the image the quadlet consumes (see **Running the service → systemd / Podman quadlet**).
  ````

  > To keep the quickstart (Task 12) consistent, note that step 2 there should say "**push** the image (which builds it): `./deploy/push.sh`" (the build is folded into the push).

**Verification:** `grep -n 'publish.sh\|tag_and_push\|YYYYMMDD' README.md` → no matches. `grep -n 'deploy/push.sh' README.md` → present.

---

### Task 12: `README.md` — add "Quickstart (Quadlets, end-to-end)"

**Why (ref):** The reference exposes a top-level Quickstart. Mirror it for the single-quadlet, end-to-end path. **Insert this new section immediately after the `## Quick start` (native) section and before `## Prerequisites`**, OR right after `## Publishing to the registry`. Recommended placement: **after `## Publishing to the registry`**, as its own top-level section.

- [ ] **Step 12.1: Insert the new section:**

  ```markdown
  ## Quickstart (Quadlets, end-to-end)

  The shortest path to a running, rootless service:

  1. **Write the env file** (the file is not shipped — you author it):
     `mkdir -p ~/.config/stocker-informer` and create `~/.config/stocker-informer/.env.podman` with the six required vars (see **Environment Variables**); fill `GOTOSOCIAL_INSTANCE` / `GOTOSOCIAL_USER` / `GOTOSOCIAL_TOKEN` and `KAFKA_*`.

  2. **Build + push the image** to the registry (no root):
     `./deploy/push.sh`

  3. **Register & enable the single quadlet**:
     `ln -s "$PWD/deploy/quadlet/stocker-informer.container" "$HOME/.config/systemd/user/stocker-informer.container"` then `systemctl --user daemon-reload` then `systemctl --user enable --now stocker-informer`

  4. **Verify**:
     `systemctl --user status stocker-informer` and `podman logs stocker-informer`
  ```

**Verification:** A top-level `## Quickstart (Quadlets, end-to-end)` heading exists with the four numbered steps.

---

### Task 13: `README.md` — Repository structure

**Why (ref, requirement c):** The tree currently lists `.env.podman` (root), `install.sh`, `publish.sh`, and `stocker-informer.build`. Rewrite to the target inventory: push.sh + single quadlet, no shipped env file, and a `docs/` entry.

- [ ] **Step 13.1: Replace the `## Repository structure` code tree (lines ~332–351) with:**

  ```
  stocker-informer/
  ├── AGENTS.md
  ├── Containerfile
  ├── review.md
  ├── go.mod
  ├── README.md
  ├── docs/deployment.md
  ├── cmd/server/main.go
  ├── internal/config/config.go
  ├── internal/kafka/consumer.go
  ├── internal/messenger/messenger.go
  ├── internal/messenger/gotosocial_publisher.go
  ├── internal/messenger/stock_event_formatter.go
  └── deploy/
      ├── push.sh
      └── quadlet/
                     └── stocker-informer.container
  ```

> The tree above is the target repository structure (note `deploy/push.sh` + a single `deploy/quadlet/stocker-informer.container`, no `install.sh`/`publish.sh`/`.build`, no shipped `.env.podman`). **Note to the developer (ordering):** list `docs/deployment.md` in the tree even though it is created by this plan's deliverable step (see "After all tasks" below) so the documented tree matches the repo.

**Verification:** `grep -n 'install.sh\|publish.sh\|stocker-informer\.build' README.md` → no matches anywhere in the file (outside `review.md`, which is a separate historical file).

---

### Task 14: `README.md` — Known limitations (de-instal / de-publish)

**Why (requirement e):** Two bullets reference the removed artifacts / shipped file and must be made consistent with the new single-quadlet, operator-owned model.

- [ ] **Step 14.1: Replace the first bullet (line 355)**:

  Current:
  ```markdown
  - `go.sum` is not committed; `deploy/install.sh` copies it and the Containerfile build relies on module hashes, so run `go mod download` / `go mod tidy` once locally before using `install.sh` or the container build, or the copy step can fail.
  ```
  New:
  ```markdown
  - `go.sum` is not committed; run `go mod download` / `go mod tidy` once locally before building the container image (via `deploy/push.sh` or `podman build`), or the container build can fail on unresolved module hashes (see D5).
  ```

- [ ] **Step 14.2: Replace the last bullet (line 362)**:

  Current:
  ```markdown
  - `KAFKA_BOOTSTRAP_SERVERS` in the shipped `.env.podman` defaults to `localhost:9092` (development-only) and must be changed for real deployments.
  ```
  New:
  ```markdown
  - The operator-authored `~/.config/stocker-informer/.env.podman` sample shows `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` (development-only); it is not shipped in this repo and must be changed for real deployments.
  ```

  (All other known-limitation bullets are unchanged.)

**Verification:** `grep -n 'deploy/install.sh\|shipped \.env\.podman\|in the shipped' README.md` → no matches.

---

### Task 15: `README.md` — "At a glance" & feature bullet (consistency)

**Why:** These lines reference the deployment and should read accurately under the new model. (No `install.sh`/`publish.sh` are named here, but the "Build" line and the features "Podman quadlet … deployment" line should be consistent.)

- [ ] **Step 15.1: Update the "Features" bullet (line 13)** from:

  ```markdown
  - Podman quadlet / systemd-user deployment for production.
  ```
  to:
  ```markdown
  - Single rootless podman quadlet (systemd `--user`) deployment; image published to `git.wheeli.ca/brian` via `deploy/push.sh`.
  ```

- [ ] **Step 15.2: Update the "At a glance" Build line (line 24)** (optional polish; keeps consistency with D2). Current:

  ```markdown
  | Build | `go build ./cmd/server/` / `podman build -f Containerfile .` |
  ```
  New:
  ```markdown
  | Build / publish | `go build ./cmd/server/` / `./deploy/push.sh` (→ `podman build` + `podman push` to `git.wheeli.ca/brian/stocker-informer:latest`) |
  ```

  The "Registry image" line (line 26) already reads `git.wheeli.ca/brian/stocker-informer:latest` — **keep as-is**.

**Verification:** `grep -n 'deploy/push.sh' README.md` appears in the features/At-a-glance/Quickstart/Publishing sections.

---

### Task 16: `README.md` — final sweep for stale references

**Why (requirement f):** Catch any remaining `install.sh` / `publish.sh` / `.build` / bare `env-file .env.podman` references across the whole file that Tasks 6–15 might have missed.

- [ ] **Step 16.1: Grep the file for every stale reference and fix each hit to the new model:**

  Run:
  ```bash
  grep -n 'install\.sh\|publish\.sh\|stocker-informer\.build\|stocker-informer-build\|env-file .env\.podman\|in the shipped' README.md
  ```
  Expected after Tasks 6–15: **zero** matches (each should have been replaced). If any remain, replace using the target wording established in the relevant task (Prerequisites → Task 6; Installation → Task 7; env file → Task 8; container run → Task 9; quadlet → Task 10; publishing → Task 11; repo tree → Task 13; known limitations → Task 14).

- [ ] **Step 16.2: Confirm `AGENTS.md` and `review.md` need no edit** (D3, D4):

  Run:
  ```bash
  grep -n 'install\.sh\|publish\.sh' AGENTS.md        # Expected: no matches (AGENTS unchanged)
  grep -n 'install\.sh\|publish\.sh\|stocker-informer\.build' review.md   # Expected: matches only inside historical findings #22/#23/#31/#32 (leave as-is)
  ```

**Verification:** Whole-README sweep is clean; `AGENTS.md` untouched; `review.md` references are intentionally historical.

---

### Task 17: Create `docs/deployment.md` (this plan) — deliverable

**Why:** The deliverable is the plan document itself, saved under `docs/`. It records the target deployment model, the step list, the file inventory, the D1–D5 decisions, and a how-to-verify section.

- [ ] **Step 17.1: Confirm the `docs/` directory exists** (create if not present):

  ```bash
  mkdir -p docs
  ```

- [ ] **Step 17.2: Write `docs/deployment.md`.** It contains:
  - The header (Goal / Architecture / Tech Stack / Spec / Global Constraints).
  - The **Decision Log** (D1–D5), especially D1's chosen single-quadlet content table.
  - The **Target File Inventory** (keep / add / modify / delete) with the verbatim `deploy/push.sh` and `deploy/quadlet/stocker-informer.container`.
  - The numbered **Implementation Steps** (Tasks 1–18).
  - The **How to verify** section (below).

> This step is the document you are reading; it is produced as the plan's own deliverable. Do not hand-edit `README.md` or any other file in this task — it is documentation only.

**Verification:** `ls docs/deployment.md` → exists; it references the single quadlet and `deploy/push.sh`; it lists `deploy/install.sh`, `deploy/quadlet/stocker-informer.build`, `deploy/publish.sh`, and `.env.podman` as delete targets.

---

### Task 18: Final commit + verification gate

**Why (requirement f):** One gate that confirms the tree is consistent, the quadlet and push.sh are internally consistent, modes are correct, and no stray files are left behind.

- [ ] **Step 18.1: Review the full diff of every change:**

  Run: `git status` and `git diff -- . ':!deploy'` (non-deploy) then `git diff -- deploy` 
  Expected: README.md modified; `deploy/install.sh`, `deploy/quadlet/stocker-informer.build`, `deploy/publish.sh`, `.env.podman` deleted; `deploy/push.sh` added; `deploy/quadlet/stocker-informer.container` modified; `docs/deployment.md` added. No other files touched.

- [ ] **Step 18.2: `bash -n` syntax check on the only executable:**

  Run: `bash -n deploy/push.sh`  → Expected: no output (exit 0).

- [ ] **Step 18.3: Consistency — quadlet `Image=` equals push.sh pushed name:**

  Run: `grep -h '^Image=' deploy/quadlet/stocker-informer.container` → `git.wheeli.ca/brian/stocker-informer:latest`
  Run: `grep -h 'podman push' deploy/push.sh` → pushes `"${REGISTRY}/${IMAGE_NAME}"` with `REGISTRY="git.wheeli.ca/brian"`, `IMAGE_NAME="stocker-informer:latest"` ⇒ `git.wheeli.ca/brian/stocker-informer:latest`. Must be identical to the quadlet's `Image=`. ✔

- [ ] **Step 18.4: Modes:**

  Run: `stat -c '%a %n' deploy/push.sh deploy/quadlet/stocker-informer.container`
  Expected: `755 deploy/push.sh` and `644 deploy/quadlet/stocker-informer.container`.

- [ ] **Step 18.5: No stray files; the two required docs are present:**

  Run: `git status --porcelain`
  Expected: only the intended changes (M README.md, D deploy/install.sh, D deploy/quadlet/stocker-informer.build, D deploy/publish.sh, D .env.podman, A deploy/push.sh, M deploy/quadlet/stocker-informer.container, A docs/deployment.md).

- [ ] **Step 18.6: Repo-wide `.md` check for stale references (excluding historical `review.md`):**

  Run:
  ```bash
  grep -rn --exclude=review.md 'install\.sh\|publish\.sh\|stocker-informer\.build' --include=*.md . || echo "OK: no stale references outside review.md"
  ```
  Expected: `OK: no stale references outside review.md`.

- [ ] **Step 18.7: Stage and commit:**

  ```bash
  git add -A
  git status --porcelain
  git commit -m "deploy: single rootless quadlet + push.sh; drop install.sh/.build/publish.sh/.env.podman"
  ```

**Final verification checklist:**
- [ ] `git status` clean after commit.
- [ ] Single quadlet matches the reference's *structure* and includes the retained hardening (D1); mode `0644`.
- [ ] `deploy/push.sh` mode `0755`; pushes `git.wheeli.ca/brian/stocker-informer:latest` (= quadlet `Image=`).
- [ ] No `install.sh`, `publish.sh`, or `stocker-informer.build` referenced in any non-`review.md` doc.
- [ ] `README.md` documents: single quadlet, `deploy/push.sh`, an inlined sample `~/.config/stocker-informer/.env.podman`, a Quickstart, repository structure, and consistent known-limitations.
- [ ] `AGENTS.md` and `review.md` intentionally unmodified.
