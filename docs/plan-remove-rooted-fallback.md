# Remove the Obsolete Rooted GET Fallback — Implementation Plan

**Goal:** Delete the rooted-key compatibility GET from
`internal/summarystore` so `ReadSummary` issues exactly one `GetObject` per
call, at the canonical bucket-relative key, and update tests and docs to
match.

**Architecture:** The production change is a deletion: `ReadSummary` stops
retrying `"/" + key` after a definitive canonical not-found and wraps
`ErrNotFound` directly. Everything else — timeout, body handling,
validation, `S3_PATH_PREFIX` normalization, error wrapping — is untouched.
Tests replace the three fallback-specific cases with regression coverage that
locks in one-GET-per-call and bucket-relative keys, and the docs stop
describing a fallback and the false "older summarizer layout" claim.

**Tech Stack:** Go 1.25, AWS SDK for Go v2 (`s3`, `types`, `smithy-go`),
`testing`, Garage S3 (path-style), AWS S3.

**Spec:** `docs/stock-summary-gotosocial.md` D2/D3 (this plan supersedes the
fallback half of D2); sibling fix plan
`stocker-summarizer/docs/plan-garage-key-prefix.md` (producer side).

Line references are against branch `fix/garage-rooted-prefix` at `c4080d5`
(= `origin/main`). Re-locate them with `grep -rn` after edits.

## Global Constraints

- This repository has **no listing bug**: it performs no `ListObjectsV2`,
  `PutObject`, or `HeadObject` — only explicit-key `GetObject`. The change is
  cleanup, not a correctness fix.
- After the change, `ReadSummary` performs at most **one** `GetObject` per
  call, always at the canonical unrooted key from `BuildKey`
  (`internal/summarystore/store.go:188-214`).
- On a definitive not-found, the returned error still satisfies
  `errors.Is(err, ErrNotFound)`; `internal/kafka/consumer.go:139` depends on
  it for the `Warn`/commit path.
- Timeout (`S3_TIMEOUT_SECONDS`), body read/close, nil-body rejection,
  identity validation, prefix normalization, and error wrapping are
  unchanged.
- All tests stay offline: fakes implement the existing `API` seam
  (`store.go:46-50`); no test contacts a real Garage or S3 endpoint.
- No data migration. The default deployment is Garage, and Garage ≤ v2.4.1
  trims the leading `/` from URL-path GETs, so the summarizer's historical
  rooted writes were always stored under unrooted keys — there is no rooted
  object to migrate.
- Conventional Commits for every commit. Run `gofmt`, `go vet ./...`, and
  `go test ./... -count=1` before claiming completion.

---

## Problem statement

### Observed

`ReadSummary` (`internal/summarystore/store.go:113-143`) reads the canonical
key and, on a definitive not-found, retries once with `"/" + key`
(`store.go:133-142`). The package doc (`store.go:8-10`), the `ErrNotFound`
doc (`store.go:32-34`), and the `ReadSummary` doc (`store.go:108-112`) all
mandate this fallback, and the docs and tests encode it.

### Operator impact

- **Double GET on every miss.** Every signal for a stock whose summary is
  absent (not yet summarized, expired, wrong prefix) costs two S3 round trips
  instead of one, on the hot consumer path.
- **Regression masking on AWS / newer Garage.** On AWS S3 a rooted key is a
  distinct object; on newer Garage (upstream PR #1465, `next-v3`) rooted keys
  can also become addressable. There the fallback can silently satisfy a read
  from a legacy rooted object, hiding a producer that regressed to rooted
  keys. Unrooted canonical reads make the regression loud instead.
- **Dead code on the deployed backend.** On Garage ≤ v2.4.1 (the deployed
  version, per the sibling plan's version note) path-GET trimming makes both
  spellings address the *same* object, so the second GET can never succeed
  when the first 404'd. It is pure overhead.

## Root cause / history

1. **The fallback was correct when written.** `stocker-summarizer`'s
   `KeyPath` really did emit rooted keys (`"/" + body`), so objects landed
   under (Garage-trimmed) unrooted keys while the summarizer *believed* it
   wrote rooted ones. The evaluator and this repo's store copied a
   canonical-read + rooted-fallback pair (design D2,
   `docs/stock-summary-gotosocial.md:83-94`) to straddle both spellings.

2. **The producer is now fixed.** `stocker-summarizer` was corrected to
   emit bucket-relative keys and list prefixes everywhere
   (`stocker-summarizer/docs/plan-garage-key-prefix.md`). No new rooted
   writes exist, so the fallback has nothing legitimate to catch.

3. **On Garage the fallback cannot succeed when the canonical GET 404s.**
   Garage's S3 router derives the key for GET/PUT from the URL path and runs
   `uri.path().trim_start_matches('/')` (see the sibling plan, Root cause
   §2). `GET /bucket/NASDAQ/AAPL/summary.md` and
   `GET /bucket//NASDAQ/AAPL/summary.md` therefore resolve to the **same**
   stored key `NASDAQ/AAPL/summary.md`. If the first request reports
   `NoSuchKey`, the second addresses an identical, equally absent object —
   the retry is structurally incapable of returning data.

4. **On AWS the fallback is actively harmful.** AWS S3 preserves a leading
   slash as a key byte, so `/NASDAQ/AAPL/summary.md` is a different object
   from `NASDAQ/AAPL/summary.md`. The fallback will happily serve a stale
   legacy object and mask a producer regression, and it makes behavior
   backend-dependent — the exact class of bug the sibling fix eliminated.

5. **Docs and tests lock the obsolete contract in.** Fallback-specific tests:
   `TestReadSummaryRootedFallback` (`store_test.go:181-202`),
   `TestReadSummaryBothMissing` (`store_test.go:204-221`, asserts two GETs),
   `TestReadSummaryRootedStorageError` (`store_test.go:241-260`). Docs:
   `README.md:10,143,208,398,468`; `AGENTS.md:19`;
   `docs/stock-summary-gotosocial.md:50-51,64,83-94,98,501-505,521-524,841,861,877,1097-1098`.
   Several of these falsely state that the summarizer ships rooted keys.

## Decision

### Chosen: canonical-only reads, fallback deleted

`ReadSummary` issues one GET at the canonical unrooted key. A definitive
not-found returns an error wrapping `ErrNotFound` immediately; every other
error is wrapped and returned as today.

| Aspect | Before | After |
|---|---|---|
| GETs on a canonical hit | 1 | 1 |
| GETs on a miss | 2 (canonical + rooted) | **1** (canonical) |
| GETs on canonical 500/timeout | 1 (no fallback) | 1 (unchanged) |
| `errors.Is(err, ErrNotFound)` on miss | yes | yes (unchanged) |
| AWS legacy rooted object | silently readable | **not readable** (caveat below) |

**AWS legacy-object caveat.** If the bucket is (or ever was) AWS S3 and the
old summarizer actually stored literal rooted objects, those objects become
unreachable after this change and need a one-time copy to unrooted keys.
Inventory them with:

```bash
aws s3api list-objects-v2 --bucket "$S3_BUCKET" --prefix "/" \
  --query 'Contents[].Key' --output text
```

The default deployment is Garage, where old rooted writes were stored
unrooted (path trimming), so **no migration is needed** there.

### Alternatives considered

| Option | Verdict |
|---|---|
| Delete the fallback entirely | **Chosen.** One representation, one GET per call, portable to AWS, no regression masking. |
| Keep the fallback behind an opt-in flag | Rejected. A config knob for a layout that no longer exists; more surface to validate and document. |
| Keep the fallback only when `S3_ENDPOINT` is non-AWS | Rejected. Backend sniffing is fragile and reintroduces backend-dependent behavior. |
| Keep the fallback until the summarizer fix is deployed everywhere | Rejected. It already cannot succeed on the deployed Garage, so deferring buys nothing. |

---

## Step 1: Remove the fallback from production code (one developer)

**Files:**
- Modify: `internal/summarystore/store.go`
- Modify: `internal/summarystore/store_test.go`

**Required behavior:**
- `ReadSummary` performs one `c.get(ctx, key)` at the canonical key. On
  success return the body; on `isNotFound(err)` return
  `fmt.Errorf("summarystore: read %q: %w", key, ErrNotFound)`; otherwise
  `wrapReadError(ctx, key, err)`. Delete the rooted retry block
  (`store.go:133-142`).
- Timeout application (`store.go:119-123`), `get` (body close, nil-body
  check), `wrapReadError`, `BuildKey`, `normalizePrefix`, `isNotFound`, and
  the `API`/`Store` interfaces are unchanged.
- Update the package doc (`store.go:8-10`: drop the rooted-key sentence; state
  the canonical key is the only key read), the `ErrNotFound` doc
  (`store.go:32-33`: drop "nor at the rooted compatibility key"), and the
  `ReadSummary` doc (`store.go:108-112`: one canonical read; not-found wraps
  `ErrNotFound`).

**Tests (`internal/summarystore/store_test.go`):**
- Delete `TestReadSummaryRootedFallback` (`:181-202`).
- Rewrite `TestReadSummaryBothMissing` (`:204-221`) as
  `TestReadSummaryNotFound`: the fake always returns `NoSuchKey`; assert
  `errors.Is(err, ErrNotFound)` and **exactly one** GET, at
  `NASDAQ/AAPL/summary.md`.
- Delete `TestReadSummaryRootedStorageError` (`:241-260`) — its
  canonical-404-then-rooted-500 scenario no longer exists. Its
  "non-not-found error is not `ErrNotFound`" intent stays covered by
  `TestReadSummaryStorageErrorNoFallback` (`:223-239`).
- Keep unchanged: `TestReadSummaryCanonical` (`:155-179`), the
  storage-error/read-error/nil-body/timeout tests (`:223-319`),
  `TestIsNotFound`, the identity-rejection and construction tests.
- Remove the now-unused `"slices"` import if nothing references it
  (`go build` fails on unused imports; `gofmt` does not remove them).

**Acceptance criteria:**
- `grep -n 'rooted\|"/" + key' internal/summarystore/store.go` matches only
  historical-free text (no production fallback remains).
- `go test ./internal/summarystore -count=1` passes; the missing-summary test
  proves one GET and `ErrNotFound`.

**Commit:**
```bash
git add internal/summarystore/store.go internal/summarystore/store_test.go
git commit -m "refactor(summarystore): drop obsolete rooted GET fallback"
```

## Step 2: Regression coverage (one developer)

**Files:**
- Modify: `internal/summarystore/store_test.go`
- Verify: `internal/kafka/consumer_test.go` (likely no change)

**Required tests:**
1. **Bucket-relative keys under every prefix spelling.** A table test that
   drives `ReadSummary` with `S3_PATH_PREFIX` values `""`, `"/"`, and a
   doubled/rooted form such as `"/corp//stocks/"`, and asserts every recorded
   `GetObjectInput.Key` has no leading `/` and equals the expected canonical
   key (`NASDAQ/AAPL/summary.md` or
   `corp/stocks/NASDAQ/AAPL/summary.md`). The existing `fakeAPI` already
   records inputs (`store_test.go:24-41`); reuse it.
2. **One GET on a miss.** (May live in the Step 1 rewrite of
   `TestReadSummaryNotFound`.) Missing summary → exactly one canonical GET,
   error satisfies `errors.Is(err, ErrNotFound)`.
3. **One GET on a hit.** `TestReadSummaryCanonical` already asserts exactly
   one key; keep the single-GET assertion explicit.
4. **Garage-semantics GET fake (small, focused).** A fake implementing
   `pathKey(key) = strings.TrimLeft(key, "/")` for `GetObject`, seeded with
   the unrooted object. Assert `ReadSummary` succeeds with one GET, and in a
   comment note what it proves: on Garage ≤ v2.4.1 the removed rooted retry
   (`/NASDAQ/...`) resolved to the same stored object, i.e. it was
   redundant; whereas a verbatim-key (AWS-style) fake seeded only with the
   canonical object shows a rooted GET would miss — a rooted key is a
   distinct, non-portable object on AWS. Keep this to one small test with a
   verbatim-key sub-case; do not build a full bucket simulator.

**Consumer check:** `TestHandleMessageSummaryNotFoundSkips`
(`internal/kafka/consumer_test.go:167-181`) feeds
`fakeSummaries{err: summarystore.ErrNotFound}` directly — it depends on the
sentinel, not on GET counts — so it needs no change. Verify it still passes
and only touch it if it encodes two-call assumptions.

**Acceptance criteria:**
- New tests fail if anyone reintroduces a second GET or roots the key.
- `go test ./internal/summarystore ./internal/kafka -count=1` passes.

**Commit:**
```bash
git add internal/summarystore/store_test.go internal/kafka/consumer_test.go
git commit -m "test(summarystore): lock in single canonical GET and unrooted keys"
```

## Step 3: Docs/AGENTS alignment and full verification (one developer)

**Files:**
- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `docs/stock-summary-gotosocial.md`
- Verify only: `docs/deployment.md` (grep shows no fallback/rooted mentions —
  no change needed). Note: `docs/review.md` does not exist in this
  repository; nothing to do.

**Required edits:**
- `README.md:10` — key-features bullet: drop "with a one-shot rooted-key
  (`/{key}`) fallback for objects written by the older summarizer layout";
  state reads use the canonical bucket-relative key only.
- `README.md:143` — `S3_TIMEOUT_SECONDS` row: "covers the canonical read plus
  any fallback" → "covers the summary read".
- `README.md:207-210` — object-key section: keep the unrooted canonical
  statement; replace the two fallback bullets with "The canonical key is the
  only key read; a missing summary returns `ErrNotFound`."
- `README.md:398` — log table row "Summary not found at canonical and rooted
  keys" → "Summary not found".
- `README.md:468` — known-limitations bullet about the fallback: remove (or
  replace with "summary reads issue a single GET at the canonical key").
- `AGENTS.md:19` — "(canonical key + rooted fallback)" → "(canonical
  unrooted key only)".
- `docs/stock-summary-gotosocial.md`:
  - `:50-51` data-flow diagram: delete the
    `GET /NASDAQ/AAPL/summary.md (only on definitive not-found)` line.
  - `:64` package table: "canonical + rooted fallback" → "canonical read".
  - `:83-94` D2: rewrite the decision to canonical-only; keep the historical
    note (summarizer once wrote rooted keys, fixed in
    stocker-summarizer; the fallback became redundant on Garage and
    regression-masking on AWS) so the record of the decision survives.
  - `:98` D3: "canonical + fallback" → "the summary read".
  - `:501-505` `ReadSummary` expected behavior: drop steps 5–6 (rooted retry)
    and renumber; not-found wraps `ErrNotFound` after the single canonical
    GET.
  - `:521-524` test table: drop the `TestReadSummaryRootedFallback` and
    `TestReadSummaryRootedStorageError` rows; update the `BothMissing` row to
    "one call, `errors.Is(err, ErrNotFound)`".
  - `:841` "S3 summary read with rooted-key fallback" → "single canonical S3
    summary read".
  - `:861` summary-source bullet: drop "rooted fallback only on not-found;
    other storage errors do not fall back"; state canonical-only reads.
  - `:877` known-limitations bullet: remove the fallback bullet.
  - `:1097-1098` manual check: drop the comment "If only the rooted key
    exists, the fallback will handle /NASDAQ/AAPL/summary.md."

**Verification gate:**
```bash
gofmt -l .
go build ./...
go vet ./...
go test ./... -count=1
go test -race ./... -count=1   # optional; no Makefile race target exists
grep -rn -i 'rooted\|fallback' README.md AGENTS.md docs/ internal/
```
Expected: `gofmt -l .` empty; build/vet clean; all packages `ok`; the final
grep returns only historical framing inside the rewritten D2 decision (if
kept) — no doc claims the service reads a rooted key or that the summarizer
ships rooted keys.

**Acceptance criteria:**
- No document or comment describes a fallback read or claims the summarizer
  writes rooted keys.
- `docs/deployment.md` verified unchanged.
- The full verification gate passes.

**Commit:**
```bash
git add README.md AGENTS.md docs/stock-summary-gotosocial.md
git commit -m "docs: document canonical-only unrooted summary reads"
```

---

## Out of scope

- Sibling repositories (`stocker-summarizer`, `stocker-news`,
  `stocker-summary-evaluator`); they own their own key fixes.
- Adding any `ListObjectsV2`, `PutObject`, or `HeadObject` functionality —
  this service stays read-only, explicit-key GET.
- Garage server configuration or version pinning.
- Data migration or deletion of any S3 objects.
- Changes to `internal/config`, the `S3_PATH_PREFIX` surface, Kafka handling,
  or the GoToSocial publisher.

## Rollback

Each step is an independent, stateless commit; revert in reverse order with
`git revert <sha>`.

- **Step 1 revert** restores the fallback: on Garage this only reintroduces
  the extra GET on misses (no correctness loss); on an AWS bucket with
  legacy rooted objects it restores the ability to read them — that is the
  one scenario where the fallback has value, and the inventory command above
  decides whether it applies.
- **Step 2/3 reverts** touch tests and docs only; revert together with
  Step 1 or not at all (a Step-1 revert alone would fail the Step-2 tests,
  which is the intended signal).
- No schema, state, or object layout is changed by the forward or reverse
  migration.

## Whole-change acceptance criteria

1. **Single GET:** `ReadSummary` issues exactly one `GetObject` per call — on
   a hit, a miss, or any storage error — always at
   `BuildKey(prefix, exchange, symbol)`.
2. **No rooted keys:** every recorded `GetObjectInput.Key` is bucket-relative
   for empty, rooted, and doubled `S3_PATH_PREFIX` spellings (test-enforced).
3. **Sentinel preserved:** a definitive not-found still satisfies
   `errors.Is(err, ErrNotFound)`; `internal/kafka`'s warn-and-commit path is
   unaffected (`go test ./internal/kafka -count=1` passes).
4. **Behavior preserved:** timeout, body close, nil-body rejection,
   identity/prefix validation, and error wrapping are byte-for-byte
   equivalent for the single-GET paths.
5. **Garage:** no data migration required; the removed fallback was provably
   redundant on Garage ≤ v2.4.1 (path trimming collapses both spellings).
6. **AWS caveat documented:** legacy literal rooted objects (if any) are
   inventoried via `aws s3api list-objects-v2 --prefix "/"` and copied to
   unrooted keys before serving from AWS.
7. **Verification gate:** `gofmt -l .` empty; `go build ./...`,
   `go vet ./...`, `go test ./... -count=1` (and `-race`) all pass.
8. **Docs:** README, AGENTS.md, and `docs/stock-summary-gotosocial.md`
   describe canonical-only unrooted reads; no doc mentions a fallback as
   current behavior or claims the summarizer writes rooted keys.
