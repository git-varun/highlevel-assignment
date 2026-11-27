# DESIGN — Bulk Stage Move

Each section follows the brief's question: **asked → designed → assumed → why this, not that.**
Numbers are from [BENCHMARKS.md](BENCHMARKS.md).

## 0. Architecture

```
client ─► API (Express, pool 20) ─┐
                                  ├─► PostgreSQL: opportunities · stage_transitions
worker process(es) (pool 4) ──────┘              bulk_jobs (the queue) · bulk_job_items (the snapshot)
```

Postgres is the data store **and** the queue. Job state, cursor and deal updates commit in one transaction.

- **Not Kafka/Redis:** a second system creates a dual write that can disagree after a crash. Kafka fits downstream fan-out (out of scope), fed by an outbox table.
- **Raw `pg`, not an ORM:** the design depends on `FOR UPDATE SKIP LOCKED`, `SET LOCAL` timeouts, advisory locks and `unnest` batch updates.

## 1. Chunking and the cursor

**Asked:** how the job is chunked, how the cursor survives a restart, which index makes it cheap.

**Designed:**
- **Index.** `bulk_job_items` holds the snapshot. Its primary key `(job_id, opportunity_id)` is the keyset index: `opportunity_id > $cursor ORDER BY opportunity_id LIMIT 500` is a constant-cost range scan.
- **One chunk = one transaction**, in this order:
  1. fence the job row
  2. fetch the next 500 items
  3. lock the deals in id order
  4. version-checked `UPDATE … FROM unnest(ids, versions)`
  5. transitions for moved deals
  6. item outcomes
  7. cursor + counters + lease
- **Restart.** The cursor commits with the work, so a crash rolls back only the open chunk. Every chunk reads the cursor from the database.
- **"Correct"** = the 8 invariants in `scripts/verify-job.js`: no pending items; applied + skipped = total; one transition per applied deal; no duplicates; applied deals are in the target stage (or moved again later); every skip was a real change.
  `SIGKILL` at 30,000 of 50,000 → completed 14.2 s later, resumed at the exact cursor, all invariants hold.
- **A half-applied job** shows `health: stalled` with real counts until its lease expires and a worker resumes it. A `failed` job keeps its applied part. Resubmitting the same filter with a new key moves the remainder: moved deals are already in the target stage and are excluded.

| Chose | Over | Because |
|---|---|---|
| Keyset cursor | `OFFSET` | `OFFSET` re-reads every skipped row on every chunk |
| Commit per chunk | One transaction | One transaction holds 50k locks, blocks users and vacuum, loses all work on a crash |
| Cursor in the DB | In memory | Memory dies with the process |

## 2. Idempotency

**Asked:** the key, where it lives, what it protects, where a retry still slips through.

**Designed:**
- **The key** is a required `Idempotency-Key` header, stored in `bulk_jobs` under `UNIQUE (workspace_id, idempotency_key)`. It survives every restart.
- **Request identity** is the sha256 of the canonicalised body: key order, money format and date format normalise to one hash.
- **Submit** runs in one transaction under a per-workspace advisory lock:

  | Situation | Response |
  |---|---|
  | Replay of an accepted key | **202, same job**. Checked before the pending cap: a retry never gets 409 or 429 |
  | Same key, different body | **422** `IDEMPOTENCY_KEY_REUSED` |
  | 10 concurrent identical submits | 1 job, 10 × 202 |
  | More than 50,000 matches | **422**; full rollback, the key stays usable |

- **Through the job's life:** fencing on `(id, attempt)` blocks zombie workers; `UNIQUE (job_id, opportunity_id)` on transitions blocks double applies.
- **Single move** is idempotent by state: a retry returns **200 `changed:false`**. A 409 means only a stale `expectedVersion`.

**Where a retry still slips through:**
- a client that sends a new key per retry
- a single move without `expectedVersion` after someone moved the deal back (ABA)
- `POST /opportunities` (Part 1 scope)

**Assumed:** keys never expire; max 200 chars; scoped per workspace.

| Chose | Over | Because |
|---|---|---|
| Client-supplied key | Body hash as the key | Only the client knows a request is a retry |
| 422 on key reuse | Return the old job | A wrong success is worse than an error |
| Postgres row | Redis `SETNX` | Commits atomically with the job |

## 3. Concurrency on one deal

**Asked:** the control on one deal, and what happens when a manual move and the job collide.

**Designed:**
- **Manual move:** `SELECT … FOR UPDATE`, `lock_timeout 2s`, version bump on every stage change.
- **Chunk:** locks deals in id order (no deadlocks) and updates only where `version = expected_version`.

| The user moves the deal | Mechanism | Result |
|---|---|---|
| Before the job reaches it | Version check | Job skips it (`skipped_modified`); the user's stage stays |
| During its chunk | Row lock | User waits up to 2 s, then gets 503 BUSY + Retry-After |
| After the job moved it | Normal move | The user's move applies on top |

**The manual edit always wins.**

**Assumed:** only this API writes deals; stage and status are independent.

| Chose | Over | Because |
|---|---|---|
| Version check | Locking all 50k up front | Holding 50k locks blocks every user |
| Version | `stage_id = from_stage` | A deal moved away and back keeps its stage but has a new version (ABA) |
| 503 after 2 s | Unbounded wait | User requests never hang behind a chunk |

## 4. Snapshot vs live set

**Asked:** snapshot or live set, and the consequence.

**Designed:** snapshot at submission. One `INSERT … SELECT` in the submit transaction captures `(id, stage, version)`. It excludes deals already in the target stage and uses `LIMIT 50001` to detect overflow cheaply.

**Consequences:**
- deals that match later are not moved
- changed deals are skipped
- the total is fixed, so the percentage is honest

**History:** `c5cd784` tried snapshot-at-start with strict FIFO. `5227f2f` reverted it, because a queued job could move deals an earlier job had pushed into its filter — deals the user never selected. Independent snapshots made FIFO pointless, so it went too.

| Chose | Over | Because |
|---|---|---|
| Snapshot at submit | Live set | A live set has a moving total and touches deals nobody approved |
| Snapshot at submit | Snapshot at start | The job changes exactly what the user approved |

## 5. Isolation

**Asked:** don't starve the same workspace; don't affect others; show the mechanism.

**Designed** (each one is tested):

| Mechanism | Effect |
|---|---|
| One running job per workspace (partial unique index) | Bounds a tenant's bulk load on its own traffic |
| Round-robin claims (`claimed_at`) | No tenant runs a backlog ahead of others |
| Per-job token bucket (`BULK_RATE_PER_SEC`) | Caps write rate. In-process is correct: one running job = one budget owner |
| Separate pools (API 20 / worker 4) | The worker never takes the API's connections |
| Pending cap: 10 per workspace → 429 | Stops queue flooding |
| `lock_timeout` on the submit lock → 503 | A burst in one workspace can't hold the API pool |

**Crash handling:**
- **Lease + heartbeat** detects dead owners.
- **Fencing on `attempt`** rejects zombie workers (proven with `SIGSTOP`).
- **`idle_in_transaction_session_timeout` < lease** frees a worker frozen mid-transaction.
- **`failures` is counted separately from `attempt`:** a deploy (`SIGTERM`) costs nothing; a crash or error backs off exponentially; 5 failures → `failed`, enforced by the reaper. A job that crashes every worker stops after 5.
- **Killed DB connections** don't crash any process.

**The hole it leaves (measured):**
- All tenants share one Postgres. A bulk job raises interactive p95 by a few ms in every workspace.
- At 5,000/s on 2 cores, pacing doesn't change that.
- At 1,000/s it does: +17–27% vs +44–120% unpaced, at 9× the duration.
- `BULK_RATE_PER_SEC` is a deployment knob.

## 6. What breaks at 10× (500k-deal move, 20M deals)

1. **Snapshot in the HTTP request** fails first: ~4 s for 500k rows, holding the submit lock. Status- and value-only filters scan the workspace.
   *Fix:* build the snapshot in the worker in batches, from a database snapshot exported at submit (`pg_export_snapshot`).
2. **The 50k cap.** Lifting it requires (1).
3. **`bulk_job_items` growth** (500k rows per job) → retention job, monthly partitions.
4. **MVCC churn:** dead tuples, WAL bursts, autovacuum lag → pace on vacuum/replication lag, partition `opportunities` by workspace.
5. **One shared database** → read replicas for list traffic; dedicated databases for the largest tenants.

## 7. Next week, ranked

1. Adaptive pacing driven by DB latency
2. Per-job item listing and cancel
3. Background snapshot for large jobs
4. Skip reasons (`modifiedByUser` / `modifiedByJob`)
5. `SKIP LOCKED` retry for hot deals: today a deal locked for more than 2 s fails its chunk until the retry budget runs out
6. Retention and TTL for items and keys
7. Worker liveness and queue age in `/health`, plus metrics
8. A multi-tenant concurrent-bulk benchmark with repeated runs

## 8. Assumptions

| Area | Assumption |
|---|---|
| Tenancy | `X-Workspace-Id` is trusted (no auth, per brief). Composite FKs block cross-tenant references |
| Pipeline | One ordered pipeline per workspace |
| Writes | Only this API changes deals; `version` bumps on stage change |
| Domain | Stage and status are independent; owners are opaque UUIDs; nothing is deleted |
| Filters | AND-combined. Date range = `created_at`, inclusive. Date-only = 00:00 UTC; timestamps carry an offset |
| Money | `numeric(14,2)`, ≥ 0, one currency, strings end to end |
| Limits | 50,000 per job; 10 pending and 1 running per workspace |
| Timing | DB clock only. Lease 30 s; chunk statement timeout 5 s; 5 failures → failed |
| Retention | Items, transitions and keys are kept forever |

## 9. Known limits

| Limit | Type |
|---|---|
| Single-move ABA without `expectedVersion` | Intentional |
| `POST /opportunities` not idempotent | Intentional (Part 1 scope) |
| `version` bumps only on stage change | Intentional |
| Shared-Postgres isolation; static pacing | Intentional, measured |
| No cancel or item listing; resubmitting completes a failed job | Deferred (§7.2) |
| A permanently locked deal exhausts a job's retries | Deferred (§7.5) |
| One `skipped_modified` reason | Deferred (§7.4) |
| No retention or TTL | Deferred (§7.6) |
| `/health` omits worker liveness | Deferred (§7.7) |
