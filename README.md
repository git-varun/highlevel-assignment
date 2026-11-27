# Opportunities — Bulk Stage Move

Moves up to 50,000 deals to a new stage as a background job that is **idempotent, resumable, observable, safe under concurrent edits, and isolated** from other traffic.

Node.js 22 · Express 5 · PostgreSQL 16, which is both the datastore and the queue.

| | |
|---|---|
| 🌐 Deployed | _TODO: add URL_ |
| 🎥 Video | _TODO: add link_ |
| 📐 Design | [DESIGN.md](DESIGN.md) |
| 📊 Benchmarks | [BENCHMARKS.md](BENCHMARKS.md) |

## Quick start

```bash
docker compose up --build
```

1. `db` starts Postgres 16 on port 5432.
2. `setup` migrates, seeds a small dataset (20,000 + 5 × 500 deals) and runs the test suite.
3. `api` (http://localhost:3000) and `worker` start only if every test passes.

`setup` reseeds on every `up`. Remove everything with `docker compose down -v`.

## Run without Docker

Requires Node ≥ 20 and Postgres at `DATABASE_URL` (default `postgres://postgres:postgres@localhost:5432/bulkmove`).

```bash
npm ci
npm run migrate
npm run seed              # 500,000 + 5 × 3,000 deals, ~7 s
npm run seed -- --small   # 20,000 + 5 × 500
npm run api               # http://localhost:3000
npm run worker            # separate terminal; run several to exercise claiming
```

The seed is deterministic (`setseed(0.42)`): 12 funnel-weighted stages, 18 months of `created_at`, 25 owners per workspace.
- Large workspace: `00000000-0000-0000-0000-000000000001`, stages 1–12.
- Small workspaces: `…0002` to `…0006`.

## API

Every route except `/health` requires `X-Workspace-Id`.

```bash
WS=00000000-0000-0000-0000-000000000001
H=(-H "X-Workspace-Id: $WS" -H "Content-Type: application/json")
```

**Part 1**

```bash
curl "${H[@]}" -X POST localhost:3000/opportunities \
  -d '{"name":"Acme renewal","value":"1250.00","stageId":1,"ownerId":"aaaaaaaa-0000-0000-0001-000000000001"}'

curl "${H[@]}" -X POST localhost:3000/opportunities/15/move -d '{"toStageId":6,"expectedVersion":1}'

curl "${H[@]}" "localhost:3000/opportunities?stageId=4&limit=50"            # follow nextCursor via &cursor=
```

**Part 2**

```bash
curl -i "${H[@]}" -H "Idempotency-Key: move-qualified-1" -X POST localhost:3000/bulk-moves \
  -d '{"targetStageId":6,"filter":{"stageId":4,"status":"open","ownerId":"aaaaaaaa-0000-0000-0001-000000000007"}}'

curl "${H[@]}" localhost:3000/bulk-moves/<job-id>
```

**Filter fields** (all optional, combined with AND; unknown fields are rejected):

| Field | Format |
|---|---|
| `stageId` | integer |
| `ownerId` | uuid |
| `status` | `open` · `won` · `lost` · `abandoned` |
| `valueMin` / `valueMax` | `"123.45"` |
| `createdFrom` / `createdTo` | `YYYY-MM-DD` (UTC) or a timestamp with `Z` or an offset |

**Retries:** resend the same `Idempotency-Key` and you get the same job, `202` with `Idempotent-Replayed: true`. Never 409.

**Progress:**

```jsonc
{
  "status": "running",
  "health": "running",
  "progress": { "total": 49990, "applied": 30000, "skipped": 12, "pending": 19978, "percent": 60.03 },
  "retry": { "failures": 0, "maxFailures": 5, "nextRetryAt": null, "lastError": null },
  "claims": 1, "secondsSinceHeartbeat": 0, "secondsSinceProgress": 0
}
```

| `health` | Meaning |
|---|---|
| `queued` | Waiting for a worker |
| `waiting_for_workspace` | Another job in this workspace is running |
| `retrying` | Backing off until `retry.nextRetryAt` |
| `running` | Committing chunks |
| `slow` | Worker alive, no chunk for `SLOW_AFTER_SECONDS` |
| `stalled` | Worker gone; the job resumes once its lease expires |
| `completed` / `failed` | Final. `completed` is set only after verifying no item is pending |

Notes:
- All values are committed state.
- `percent` is floored, so it never shows 100 early.
- `skipped` = deals someone changed after submit; they keep their new values.

**Status codes**

| Code | Meaning |
|---|---|
| 200 / 201 / 202 | OK / created / job accepted (including replays) |
| 400 | Invalid input; missing `Idempotency-Key` or `X-Workspace-Id` |
| 404 | Not found, or in another workspace |
| 409 | Stale `expectedVersion` on a single move — never a retry |
| 413 | Body over 100 kB |
| 422 | `TOO_MANY_MATCHES` · `IDEMPOTENCY_KEY_REUSED` · `INVALID_STAGE` |
| 429 | `TOO_MANY_PENDING_JOBS`, with `Retry-After` |
| 503 | `BUSY`: row or workspace lock held; retry after `Retry-After` |

## Tests

```bash
npm test                              # 29 tests on real Postgres, ~15 s
sh scripts/prove-tests.sh             # removes 17 safety mechanisms one by one; each must fail its test
node scripts/verify-job.js <job-id>   # proves one job's result from raw tables
```

Each test owns its workspace. Worker tests refuse to run against a busy queue.

| File | Proves |
|---|---|
| `idempotency.test.js` | Concurrent submits → 1 job; replay → 202; canonical equality; key reuse → 422; cap rollback keeps the key; snapshot frozen at submit; 429 under burst; submit lock serialises and times out to 503; single-move retry ≠ 409 |
| `worker.test.js` | Happy path; manual edit wins; zombie fenced; one running job per workspace; livelock regression; round-robin fairness; backoff doesn't block; retry budget and reaper; completion invariant; health states; floored percent |
| `crash.test.js` | Real process: `SIGKILL` resumes at the cursor; killed DB connections → no crash, 0 failures; `SIGTERM` hands off cleanly |
| `api.test.js` | Tenancy; hostile input never 500; filter matrix matches independent SQL both ways; move blocked by chunk → 503; pagination |

## Benchmarks

```bash
npm run bench                                                        # ~1 min, writes BENCHMARKS.md
BENCH_TAG=1k-run1 BENCH_RPS=200 BENCH_PACED_RATE=1000 npm run bench   # extra stress run
node scripts/bench.js --render                                       # rebuild BENCHMARKS.md
```

Separate processes, open-loop load, settled baseline. Results and hardware: [BENCHMARKS.md](BENCHMARKS.md).

## Configuration

All variables are optional. [`.env.example`](.env.example) lists every one with its default: copy it to `.env` and run with `node --env-file=.env …`.

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/bulkmove` | Database |
| `PORT` | `3000` | API port |
| `API_POOL_MAX` / `WORKER_POOL_MAX` | `20` / `4` | Separate connection budgets |
| `WORKER_CONCURRENCY` | `2` | Parallel jobs per worker process |
| `CHUNK_SIZE` | `500` | Deals per transaction |
| `BULK_RATE_PER_SEC` | `5000` | Per-job pacing; `0` = unlimited |
| `LEASE_SECONDS` | `30` | Crash detection |
| `POLL_MS` | `500` | Idle claim interval |
| `MAX_JOB_SIZE` | `50000` | Deals per job |
| `MAX_PENDING_JOBS` | `10` | Queued + running per workspace |
| `SLOW_AFTER_SECONDS` | `15` | Threshold for `health: slow` |

## Scope

**Implemented:**
- Part 1 in full.
- All five Part 2 properties.
- Seeding, benchmarks, tests with a mutation proof, `docker compose`.

**Out of scope (per brief):** UI, auth, pipeline views and counts, extra filtering, downstream events, deployment/CI, custom fields, contacts.

**Deferred:** cancel, item listing, skip reasons, adaptive pacing, background snapshots, retention. See [DESIGN.md §7](DESIGN.md#7-next-week-ranked).

## Layout

```
db/migrations/   001–008, forward-only (003→004 records a reverted design)
src/db.js        pools + withTransaction
src/api/         app, tenant middleware, errors, routes
src/domain/      business rules and SQL
src/worker/      claim, chunk, complete, release, reap; retry; token bucket; loop
scripts/         migrate, seed, verify-job, bench, prove-tests
test/            node:test suites on real Postgres
```
