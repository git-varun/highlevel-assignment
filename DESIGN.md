# DESIGN — Bulk Stage Move

Each section answers the brief's question in the same shape: **asked → designed → assumed → why this, not that.**
Numbers come from [BENCHMARKS.md](BENCHMARKS.md) (2-core Xeon, everything on one machine).

## 0. Shape of the system

```
client ─► API (Express, pool 20) ─┐
                                  ├─► PostgreSQL: opportunities · stage_transitions
worker process(es) (pool 4) ──────┘              bulk_jobs (= the queue) · bulk_job_items (= the snapshot)
```

- **One Postgres is both the data and the queue.** Job state, cursor and deal updates commit in **one transaction**.
- **Why not Kafka/Redis:** a second system adds a dual write (deals in PG, offset/queue state elsewhere) that can disagree after a crash. At 50k rows per job that buys nothing. Kafka becomes right for fan-out to downstream consumers (out of scope), and would be fed from an outbox table.
- **Why raw `pg`, not an ORM:** the design depends on `FOR UPDATE SKIP LOCKED`, `SET LOCAL` timeouts, advisory locks and `unnest` batch updates, and an ORM would hide them.

## 1. Chunking, cursor, and the index that makes it cheap

**Asked:** how the job is chunked, how the cursor survives a restart, which index makes chunking cheap.

**Designed:**
- **The snapshot** is the list of (deal id, version) rows in `bulk_job_items`. Its primary key `(job_id, opportunity_id)` **is the keyset index**: `WHERE job_id = $1 AND opportunity_id > $cursor ORDER BY opportunity_id LIMIT 500` is a short range scan at any depth.
- **One chunk = one transaction**, in this order:
  1. lock + fence the job row
  2. fetch the next 500 items
  3. lock those deals in id order
  4. version-checked `UPDATE … FROM unnest(ids, versions)`
  5. write transitions for the moved deals only
  6. mark each item `applied` or `skipped_modified`
  7. **write cursor + counters + lease renewal in the same commit**
- **The cursor survives a restart** because it is a column written in the same commit as the work. A crash rolls back the open chunk; everything before it is committed, so the cursor and the data can never disagree. Each chunk reads the cursor back from the DB, never from memory.
- **"Finishes correctly" means** the 8 invariants in `scripts/verify-job.js` hold:
  - no pending items
  - applied + skipped = total
  - one transition per applied deal
  - no duplicate transitions
  - applied deals are in the target stage, or were moved again later
  - every skip was a real concurrent change

  Measured: `SIGKILL` at 30,000 of 50,000 → done **14.2 s** later (10 s of it is the lease), resumed at the exact cursor, all checks ✅.
- **A half-applied job looks like this to the user:** `status: running`, `health: stalled`, real `applied/skipped/pending`, `secondsSinceHeartbeat` growing. It is reclaimed when the lease expires. A job that ends `failed` keeps its applied part. **Resubmitting with a new key completes the remainder**, because already-moved deals are in the target stage and are excluded from the new snapshot.

**Assumed:** 500 per chunk. Larger chunks give more throughput but hold row locks longer, so manual moves wait longer.

| Chose | Over | Because |
|---|---|---|
| Keyset cursor | `OFFSET` | `OFFSET 30000` re-reads 30k rows per chunk; keyset is constant cost |
| Commit per chunk | One 50k-row transaction | One transaction holds 50k locks, blocks users, stops vacuum, loses everything on a crash, and can't report progress |
| Cursor in the DB | In memory | Memory dies with the process |

## 2. Idempotency model

**Asked:** what the key is, where it is stored, what it protects, where a retry could still slip through. (Recruiter: *"retrying the same request must not return 409".*)

**Designed:**
- **The key:** a client-supplied `Idempotency-Key` header, **required**. It is stored in `bulk_jobs` under `UNIQUE (workspace_id, idempotency_key)`, a Postgres row, so it survives any restart.
- **Request identity:** sha256 of a **canonicalised** body. Key order, `"100"` vs `"100.00"`, and date formats normalise to the same hash.
- **Submit runs in one transaction under a per-workspace advisory lock:**

  | Situation | Response |
  |---|---|
  | Key already accepted | **202, same job** (`Idempotent-Replayed: true`), checked **before** the pending cap, so a retry is never 409 or 429 |
  | Same key, different body | **422** `IDEMPOTENCY_KEY_REUSED` (returning the old job would make the client believe its new request was accepted) |
  | Concurrent duplicates | They queue on the lock or the unique index: 10 parallel → 1 job, 10 × 202 |
  | Too many matches | **422**; the whole transaction rolls back, so **the key is not burned** |

- **Throughout the job's life:**
  - fencing on `(id, attempt)` stops a zombie worker from committing
  - `UNIQUE (job_id, opportunity_id)` on transitions makes a double apply impossible at the DB level
- **Single move:** idempotent by **state**. A retry of a move that already happened returns **200 `changed:false`**, never 409. A 409 means only a genuine stale `expectedVersion`.

**Where a retry can still slip through:**
1. The client generates a **new key per retry** (e.g. on page reload). The key is the identity, so the client must persist it.
2. A single move with no `expectedVersion` can re-apply after someone moved the deal back (an ABA situation; intentional — `expectedVersion` exists for that).
3. `POST /opportunities` is not idempotent (Part 1, "do not polish").

**Assumed:** keys never expire (no TTL), max 200 chars, scoped per workspace.

| Chose | Over | Because |
|---|---|---|
| Client key | Hash of the body as the key | The same move on Monday and Friday are two intents; only the client knows "this is a retry" |
| Key + hash, 422 on mismatch | Return the old job | A wrong success is worse than an error |
| Postgres row | Redis `SETNX` | It commits atomically with the job; Redis can lose the key or keep a key with no job |

## 3. Concurrency control on a single opportunity

**Asked:** the control on one deal, and what happens when a manual move and the job hit the same record.

**Designed:**
- **Single move:** `SELECT … FOR UPDATE` (read-check-write is atomic), `SET LOCAL lock_timeout = '2s'`, and a version bump on every stage change.
- **Job chunk:** locks its deals in **id order** (no lock cycles, so no deadlocks) and updates only where `version = expected_version` (captured at submit).

| When the user acts | Mechanism | Outcome |
|---|---|---|
| **Before** the job reaches the deal | version check | the job **skips it** (`skipped_modified`); the user's stage stays |
| **During** that chunk (ms window) | row lock | the user waits briefly, or gets **503 BUSY + Retry-After** after 2 s; nothing is overwritten |
| **After** the job moved it | normal move | the user's later move applies on top (`bulk`, then `manual` in the history) |

**The manual edit always wins.** A bulk action must never silently undo a person's decision.

**Assumed:**
- only this API writes deals (nothing bypasses `version`)
- `version` bumps only on stage changes (the only edit endpoint)
- stage and status are independent (moving to "Closed Won" leaves status as is)

| Chose | Over | Because |
|---|---|---|
| Optimistic version check | Pessimistic lock on all 50k up front | Holding 50k locks for the whole job blocks every user |
| Version | `stage_id = from_stage` | ABA: a deal moved away and back has the same stage but a new version, and the user's intent must win |
| 503 after 2 s | Unbounded wait | A user request must not hang behind a slow chunk |

## 4. Snapshot vs live set

**Asked:** snapshot or live set, and the consequence.

**Designed:** a **snapshot at submission**, taken by one `INSERT … SELECT` inside the submit transaction (one statement = one consistent view, even under Read Committed). Deals already in the target stage are excluded. `LIMIT 50001` detects "too many" without counting everything.

**Consequences, stated to the user:**
- deals that start matching the filter **after** submit are **not** moved
- deals changed after submit are **skipped**, not overwritten
- the total is fixed from the first response, so the progress % is honest

**Decision history:**
- **`c5cd784`** tried *snapshot at job start + strict FIFO*, so a queued job would see earlier jobs' results.
- **`5227f2f`** reverted it: a queued job could then move deals **the user never saw or selected** (job A moves deals into job B's filter before B starts).
- With per-job snapshots, jobs are independent, so FIFO only added head-of-line blocking and was dropped too.

| Chose | Over | Because |
|---|---|---|
| Snapshot at submit | Live set | A live set has a moving total (dishonest %) and moves deals nobody approved |
| Snapshot at submit | Snapshot at start | "What you approved is what changes" |

## 5. Isolation mechanism, and the hole it still leaves

**Asked:** don't starve interactive requests in the same workspace; don't affect other workspaces; show the mechanism.

**Designed (implemented, each covered by a test):**

| Mechanism | Protects |
|---|---|
| **One running job per workspace** (partial unique index + claim check) | bounds the load one tenant's bulk work puts on its own traffic |
| **Round-robin claims** (least-recently-served workspace first, `claimed_at`) | a tenant that queued 10 jobs can't run all 10 before another tenant's first |
| **Per-job token bucket** (`BULK_RATE_PER_SEC`), lease renewed while waiting | caps write rate; in-process is valid because one job per workspace means one owner of that budget |
| **Separate pools** (API 20 / worker 4) | the worker can never take the API's connections |
| **Pending cap** (10 queued+running per workspace → 429 + Retry-After) | queue flooding |
| **`lock_timeout` 2 s on the submit lock** | a submit burst in one workspace can't hold the whole API pool and starve other tenants |

**Crash handling:**
- **Lease + heartbeat:** a dead owner is detected; its job shows `stalled`, then is reclaimed.
- **Fencing on `attempt`:** a frozen worker that wakes up can't commit. Proven with `SIGSTOP` and in a test.
- **`idle_in_transaction_session_timeout` < lease:** a worker frozen *mid-transaction* can't pin the job row forever (`SKIP LOCKED` claimers would skip it indefinitely).
- **`failures` is separate from `attempt`:** a graceful `SIGTERM` hand-off (deploy) costs nothing; crashes and errors back off exponentially; after 5 → `failed`, which the reaper enforces, so no job stays stuck. This also stops a job that crashes every worker (a poison job) from looping forever.
- **Killed DB connections** don't crash processes: the `'error'` event on checked-out clients is handled. A test found and pinned this.

**The hole it still leaves (measured):**
- Every tenant shares **one Postgres** and, here, 2 CPUs. A bulk job raises interactive p95 by a few ms in **its own and other workspaces alike**.
- Pacing at 5,000/s is not slow enough on this hardware to change that. **1,000/s** reduces the impact measurably (+17–27% vs +44–120% p95 unpaced) at ~9× the duration.
- An earlier, polluted measurement suggested 5,000/s was enough; it didn't reproduce and is withdrawn in BENCHMARKS.md.

## 6. What breaks at 10× (500k-row move, 20M deals)

1. **First: the snapshot inside the HTTP request.** 500k items is ~4 s of `INSERT … SELECT` while holding the workspace submit lock. Status/value-only filters scan the whole workspace (stage, owner and date filters are indexed).
   *Fix:* accept the job, build the snapshot in the worker in batches, from a database snapshot exported at submit time (`pg_export_snapshot`) so it still reflects what the user saw; or have the client send the exact ids it displayed.
2. **The 50k cap** becomes a product limit. Lifting it needs (1).
3. **`bulk_job_items` grows 500k rows per job, forever** → retention job + key TTL; partition by month.
4. **MVCC and write volume:** 500k updates → 500k dead tuples, WAL bursts, autovacuum lag that slows everyone → pace on replication/vacuum lag, tune autovacuum for this table, partition `opportunities` by workspace.
5. **A single shared database** → read replicas for list traffic; move the largest tenants to their own database/shard.

## 7. With another week, ranked

1. **Adaptive pacing** driven by DB latency instead of a fixed rate (the measured hole).
2. **Per-job item listing + cancel** (`GET /bulk-moves/:id/items?status=…`, `POST …/cancel`).
3. **Background snapshot for large jobs** (§6.1), which removes the 10× bottleneck.
4. **Skip reasons** (`modifiedByUser` / `modifiedByJob`) instead of one `skipped_modified`.
5. **Hot-deal handling:** a deal locked longer than the chunk's 2 s `lock_timeout` currently fails the chunk and is retried until `failures` runs out → `SKIP LOCKED` and retry those ids at the end.
6. **Retention / TTL** for items and idempotency keys.
7. **Worker liveness and queue age in `/health`**, plus metrics (claims, chunk latency, skips).
8. A multi-tenant concurrent-bulk benchmark, and repeated runs with confidence intervals.

## 8. Assumptions

| Area | Assumption |
|---|---|
| Tenancy | `X-Workspace-Id` is trusted (no auth, per brief); every query filters by it; composite FKs make cross-tenant references impossible |
| Pipeline | One pipeline (ordered stages) per workspace |
| Writes | Only this API changes deals; `version` bumps on stage change (the only edit) |
| Domain | Stage and status are independent; won/lost deals may be bulk-moved; owners are opaque UUIDs |
| Deletion | Deals and workspaces are never deleted |
| Filters | AND-combined, one value each; "date range" = `created_at`, inclusive; date-only = 00:00 UTC; timestamps must carry an offset |
| Money | `numeric(14,2)`, ≥ 0, single currency, handled as strings (never JS floats) |
| Limits | 50,000 per job (422); 10 pending per workspace (429); 1 running per workspace |
| Ordering | No order between jobs; round-robin across workspaces |
| Timing | DB clock only; lease 30 s ≫ chunk (statement timeout 5 s); 5 failures → failed |
| Retention | Items, transitions and idempotency keys are kept forever |
| Scale | ids < 2^53 (parsed as JS numbers); one Postgres primary |

## 9. Known limits (disclosed, not hidden)

| Limit | Status |
|---|---|
| Single-move ABA without `expectedVersion` | intentional (§2) |
| `POST /opportunities` not idempotent | intentional (Part 1 scope) |
| `version` bumps only on stage change | intentional; revisit when other field edits exist |
| Shared-Postgres isolation hole; static pacing | intentional, measured (§5) |
| No item listing / cancel; failed jobs stay partially applied (resubmit completes them) | deferred (§7.2) |
| A permanently hot deal can exhaust a job's retries | deferred (§7.5) |
| `skipped_modified` has no reason | deferred (§7.4) |
| No retention / TTL | deferred (§7.6) |
| `/health` doesn't report worker liveness | deferred (§7.7) |
| Benchmarks: single machine, few runs per setting | stated in BENCHMARKS.md |
