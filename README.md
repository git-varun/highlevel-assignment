# Opportunities — Bulk Stage Move

A CRM backend focused on one hard operation: **move up to 50,000 deals to another stage in one action**, as a background job that is **idempotent, resumable, observable, correct under concurrent edits, and well-behaved** towards other traffic and tenants.

Node.js 22 · Express 5 · PostgreSQL 16 (the only datastore — it is also the queue).

| | |
|---|---|
| 🌐 Deployed | _TODO: add URL_ |
| 🎥 Video walkthrough | _TODO: add link_ |
| 📐 Design decisions | [DESIGN.md](DESIGN.md) |
| 📊 Measurements | [BENCHMARKS.md](BENCHMARKS.md) |

---

## Quick start (one command)

```bash
docker compose up --build
```

What happens, in order:

1. `db`: Postgres 16 starts (host port 5432).
2. `setup`: applies migrations → seeds a **small** dataset (20,000 + 5 × 500 deals) → **runs the test suite**.
3. `api` (http://localhost:3000) and `worker` start **only if every test passed**.

> `setup` re-seeds (truncates) on every `up`. If host port 5432 is taken, change the `db` port mapping.

```bash
docker compose down -v      # stop and remove the database volume
```

## Running without Docker

Needs Node ≥ 20 and a Postgres you can reach via `DATABASE_URL`
(default `postgres://postgres:postgres@localhost:5432/bulkmove`).

```bash
npm ci
npm run migrate
npm run seed               # full: 500,000 deals in 1 large + 5 small workspaces (~7 s)
npm run seed -- --small    # small: 20,000 + 5 × 500
npm run api                # http://localhost:3000
npm run worker             # in another terminal (run several to see claiming/fencing)
```

The seed is deterministic (`setseed(0.42)`): 12 stages with a funnel-shaped distribution, ~18 months of `created_at`, 25 owners per workspace. Fixed ids for examples:

- large workspace `00000000-0000-0000-0000-000000000001` (stages 1–12)
- small workspaces `…0002` to `…0006`

---

## API

Every route except `/health` needs `X-Workspace-Id: <uuid>` (tenant scope; no auth, per the brief).

```bash
WS=00000000-0000-0000-0000-000000000001
H=(-H "X-Workspace-Id: $WS" -H "Content-Type: application/json")
```

### Part 1 — foundation

```bash
# create
curl -s "${H[@]}" -X POST localhost:3000/opportunities \
  -d '{"name":"Acme renewal","value":"1250.00","stageId":1,"ownerId":"aaaaaaaa-0000-0000-0001-000000000001"}'

# move one deal (expectedVersion optional: send it to get a 409 if someone changed the deal meanwhile)
curl -s "${H[@]}" -X POST localhost:3000/opportunities/15/move -d '{"toStageId":6,"expectedVersion":1}'

# list a stage, keyset pagination (pass nextCursor back as cursor)
curl -s "${H[@]}" "localhost:3000/opportunities?stageId=4&limit=50"
curl -s "${H[@]}" "localhost:3000/opportunities?stageId=4&limit=50&cursor=<nextCursor>"
```

### Part 2 — bulk move

```bash
# submit: returns 202 immediately with the job; poll the Location header
curl -si "${H[@]}" -H "Idempotency-Key: 6f1c-move-qualified" -X POST localhost:3000/bulk-moves \
  -d '{"targetStageId":6,"filter":{"stageId":4,"status":"open","ownerId":"aaaaaaaa-0000-0000-0001-000000000007"}}'

# progress
curl -s "${H[@]}" localhost:3000/bulk-moves/<job-id>
```

**Filter fields** (all optional, AND-combined):

| Field | Format |
|---|---|
| `stageId` | integer |
| `ownerId` | uuid |
| `status` | `open` / `won` / `lost` / `abandoned` |
| `valueMin`, `valueMax` | `"123.45"` |
| `createdFrom`, `createdTo` | `YYYY-MM-DD` (UTC) or a timestamp with `Z`/offset |

Unknown fields are rejected, so a typo can't turn into "match everything". Deals already in the target stage are excluded.

**Retrying:** send the **same `Idempotency-Key`** and you get the same job back (`202`, header `Idempotent-Replayed: true`). Never 409.

**Progress response** (all values are committed database state):

```jsonc
{
  "status": "running",            // queued | running | completed | failed
  "health": "running",            // see table below
  "progress": { "total": 49990, "applied": 30000, "skipped": 12, "pending": 19978, "percent": 60.03 },
  "retry": { "failures": 0, "maxFailures": 5, "nextRetryAt": null, "lastError": null },
  "claims": 1,
  "lastProgressAt": "…", "secondsSinceHeartbeat": 0, "secondsSinceProgress": 0
}
```

| `health` | Meaning |
|---|---|
| `queued` | waiting for a free worker |
| `waiting_for_workspace` | another job of this workspace is running (one at a time) |
| `retrying` | the last attempt failed; backing off until `retry.nextRetryAt` |
| `running` | committing chunks |
| `slow` | worker alive, but no chunk committed for `SLOW_AFTER_SECONDS` |
| `stalled` | worker stopped heartbeating (crashed); the job will be reclaimed and resumed |
| `completed` / `failed` | terminal. `completed` is only set after verifying no item is pending |

`percent` is floored: it never shows 100 while work remains. `skipped` = deals changed by someone after you submitted; they are left as the person set them.

### Status codes

| Code | When |
|---|---|
| 200 / 201 / 202 | ok / created / bulk job accepted (including idempotent replays) |
| 400 | invalid input, missing `Idempotency-Key` or `X-Workspace-Id` |
| 404 | not found **or belongs to another workspace** (existence is not revealed) |
| 409 | single move with a stale `expectedVersion` (a genuine conflict only, never a retry) |
| 413 | body over 100 kB |
| 422 | `TOO_MANY_MATCHES` (> 50,000), `IDEMPOTENCY_KEY_REUSED`, `INVALID_STAGE` |
| 429 | `TOO_MANY_PENDING_JOBS` (10 per workspace), with `Retry-After` |
| 503 | `BUSY`: the record or workspace lock is held (e.g. by a running chunk) — retry after `Retry-After` |

---

## Tests

```bash
npm test                      # 29 tests against a real Postgres, ~15 s
sh scripts/prove-tests.sh     # mutation proof: breaks each safety mechanism, requires its test to fail
node scripts/verify-job.js <job-id>   # proves one job's outcome from the raw tables
```

Every test creates and drops its **own** workspace; worker tests refuse to run if the queue has foreign jobs.

| File | What it proves |
|---|---|
| `idempotency.test.js` | 10 concurrent identical submits → 1 job; replay → 202; canonical equality; key reuse → 422; cap rollback doesn't burn the key; snapshot frozen at submit; 429 under a burst; submit lock serialises (deterministic) and times out to 503; single-move retry ≠ 409 |
| `worker.test.js` | happy path; manual edit wins; zombie worker fenced; one running job per workspace; livelock regression (single slot); round-robin fairness; backoff doesn't block; retry budget + reaper; completion invariant; health states; progress floor |
| `crash.test.js` | real worker process: `SIGKILL` → resumes at the exact cursor; all DB connections killed mid-job → survives, 0 failures charged; `SIGTERM` → graceful hand-off, no failure |
| `api.test.js` | tenancy (404/422 across workspaces); hostile input is never a 500; **filter matrix** (snapshot == an independent SQL query, both directions); move blocked by a chunk → 503; keyset pagination |

`prove-tests.sh` removes **17** mechanisms one at a time (version check, fencing, livelock fix, request hash, submit lock, lock timeouts, state-before-version, completion check, floor %, unknown-key rejection, target exclusion, tenant scoping, fairness, strict dates, failures-vs-claims, DB-kill error handler). Each must turn its test red. It first requires a green baseline and finishes with a green suite.

## Benchmarks

```bash
npm run bench                                                  # reseeds 500k, writes BENCHMARKS.md (~1 min)
BENCH_TAG=1k-run1 BENCH_RPS=200 BENCH_PACED_RATE=1000 npm run bench   # extra stress run → bench/results-1k-run1.json
node scripts/bench.js --render                                 # rebuild BENCHMARKS.md from saved results
```

API, worker and load generator run as separate processes; the load is open-loop, and a settled baseline (VACUUM + warm-up) is measured first. Results and hardware are in [BENCHMARKS.md](BENCHMARKS.md).

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/bulkmove` | |
| `PORT` | `3000` | API port |
| `API_POOL_MAX` / `WORKER_POOL_MAX` | `20` / `4` | separate connection budgets |
| `WORKER_CONCURRENCY` | `2` | jobs one worker process runs in parallel (different workspaces) |
| `CHUNK_SIZE` | `500` | deals per transaction |
| `BULK_RATE_PER_SEC` | `5000` | per-job pacing (`0` = unlimited); tune per hardware, see BENCHMARKS |
| `LEASE_SECONDS` | `30` | crash detection; must exceed the worst chunk |
| `POLL_MS` | `500` | idle claim interval |
| `MAX_JOB_SIZE` | `50000` | max deals per job |
| `MAX_PENDING_JOBS` | `10` | queued + running per workspace |
| `SLOW_AFTER_SECONDS` | `15` | progress age that reports `health: slow` |

## Implemented / not implemented

**Implemented:**
- Part 1: create, single move with a transition, list by stage with pagination.
- Part 2, all five properties:
  - idempotent submit, end to end
  - resumable after crash, from a committed cursor
  - progress from committed state with health states
  - manual edits always win, with a snapshot at submit
  - isolation: one running job per workspace, round-robin, pacing, separate pools, pending cap, lock timeouts
- Seeding, benchmarks, tests with a mutation proof, `docker compose`.

**Not implemented — out of scope per the brief:** UI, authentication, board/pipeline summaries, per-stage counts, rich filtering beyond the bulk filter, downstream events, deployment/CI/Kubernetes, custom fields, contacts.

**Not implemented — deliberately deferred** (see DESIGN.md §7, §9):
- cancel and per-job item listing
- skip reasons
- adaptive pacing
- background snapshot for >50k
- retention/TTL
- worker liveness in `/health`

## Project layout

```
db/migrations/     001…008, forward-only; 003→004 records a reverted design (see DESIGN.md §4)
src/db.js          two pools + withTransaction (handles killed connections)
src/api/           Express app, tenant middleware, error mapping, routes
src/domain/        business rules + SQL (no Express): opportunities, bulk submit/progress, money
src/worker/        claim / chunk / complete / release / reap, retry, token bucket, process loop
scripts/           migrate, seed, verify-job, bench, prove-tests
test/              node:test suites against real Postgres
```
