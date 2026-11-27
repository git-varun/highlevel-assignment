// Job lifecycle SQL. Each function is one step: claim → (chunk)* → complete | release | fail.
// Every write after claim is fenced on (id, attempt): a worker that lost ownership
// can never change a job it no longer owns.
import { withTransaction } from '../db.js';

export const LEASE_SECONDS = Number(process.env.LEASE_SECONDS ?? 30);

export class LostOwnershipError extends Error {
  constructor(jobId) { super(`lost ownership of job ${jobId}`); this.name = 'LostOwnershipError'; }
}

const txGuards = async (c, statementTimeout) => {
  await c.query(`SET LOCAL lock_timeout = '2s'`);
  await c.query(`SET LOCAL statement_timeout = '${statementTimeout}'`);
  // A worker frozen MID-transaction (GC pause, SIGSTOP, VM stall) would hold the job-row
  // lock forever, and SKIP LOCKED claimers would skip it forever. Postgres kills a session
  // that sits idle inside an open transaction longer than this → rollback → locks freed.
  // Must be shorter than the lease so the job is reclaimable once the lease expires.
  await c.query(`SET LOCAL idle_in_transaction_session_timeout = '${Math.max(1, Math.floor(LEASE_SECONDS / 2))}s'`);
};

/**
 * Claim one job. No ordering promise within a workspace: jobs are independent (each one
 * only touches the deals its user selected), so a job backing off after an error must not
 * hold up the others. Across the whole queue, oldest-first is just a sensible default.
 *
 * Candidates:
 *   - queued and past its backoff time, or
 *   - running with an expired lease (owner crashed) → costs one failure.
 * NOT EXISTS: skip workspaces that already have a live running job — one running job per
 * workspace bounds the load a tenant's bulk work puts on its own interactive traffic.
 * The partial unique index is the hard guarantee if two claims race past this check.
 * FOR UPDATE SKIP LOCKED: concurrent workers skip rows another worker is claiming
 * instead of waiting, so two workers never claim the same job.
 */
export async function claimJob(pool, workerId) {
  try {
    const { rows } = await pool.query(
      `WITH candidate AS (
         SELECT j.id
         FROM bulk_jobs j
         -- Reclaiming a crashed job costs one failure, so it needs failures + 1 < max.
         -- The reaper takes exactly the complement (failures + 1 >= max): no overlap, no race.
         WHERE ( (j.status = 'queued'  AND j.next_run_at <= now() AND j.failures < j.max_attempts)
              OR (j.status = 'running' AND j.lease_until < now() AND j.failures + 1 < j.max_attempts) )
           AND NOT EXISTS (
             SELECT 1 FROM bulk_jobs r
             WHERE r.workspace_id = j.workspace_id AND r.status = 'running'
               AND r.id <> j.id AND r.lease_until >= now())
         ORDER BY j.created_at
         LIMIT 1
         FOR UPDATE SKIP LOCKED
       )
       UPDATE bulk_jobs j
       SET failures     = j.failures + CASE WHEN j.status = 'running' THEN 1 ELSE 0 END,
           status       = 'running',
           worker_id    = $1,
           attempt      = j.attempt + 1,
           lease_until  = now() + make_interval(secs => $2),
           heartbeat_at = now(),
           started_at   = COALESCE(j.started_at, now())
       FROM candidate c
       WHERE j.id = c.id
       RETURNING j.id, j.workspace_id, j.target_stage_id, j.attempt, j.failures, j.cursor_id, j.total_count`,
      [workerId, LEASE_SECONDS],
    );
    return rows[0] ?? null;
  } catch (err) {
    // 23505 on bulk_jobs_one_running_per_ws: another worker won this workspace a moment ago.
    if (err.code === '23505') return null;
    throw err;
  }
}

/**
 * Process ONE chunk in ONE transaction. Returns { processed, applied, skipped, done }.
 *
 * a) Lock + fence the job row: if (id, attempt, running) no longer matches, we lost the job.
 *    Holding this row lock for the chunk also means nobody can reclaim the job mid-chunk.
 * b) Next N items after the cursor (PK range scan — the index that makes chunking cheap).
 * c) Lock those deals in id order (consistent order → no lock cycles → no deadlocks).
 * d) Move only deals whose version still equals the snapshot version (manual edit wins).
 * e) Record transitions for exactly the moved deals.
 * f) Mark each item applied / skipped_modified.
 * g) Advance cursor + counters + lease — the same commit as the work, so they can never disagree.
 */
export async function runChunk(pool, job, chunkSize) {
  return withTransaction(pool, async (c) => {
    await txGuards(c, '10s');

    // a) fence
    const fence = await c.query(
      `SELECT cursor_id FROM bulk_jobs
       WHERE id = $1 AND attempt = $2 AND status = 'running' FOR UPDATE`,
      [job.id, job.attempt],
    );
    if (!fence.rowCount) throw new LostOwnershipError(job.id);
    const cursor = fence.rows[0].cursor_id; // DB is the source of truth, not worker memory

    // b) next chunk
    const { rows: items } = await c.query(
      `SELECT opportunity_id, expected_version FROM bulk_job_items
       WHERE job_id = $1 AND opportunity_id > $2
       ORDER BY opportunity_id LIMIT $3`,
      [job.id, cursor, chunkSize],
    );
    if (!items.length) return { processed: 0, applied: 0, skipped: 0, done: true };

    const ids = items.map((i) => i.opportunity_id);
    const versions = items.map((i) => i.expected_version);

    // c) lock in id order
    await c.query(
      `SELECT id FROM opportunities WHERE id = ANY($1::bigint[]) AND workspace_id = $2
       ORDER BY id FOR UPDATE`,
      [ids, job.workspace_id],
    );

    // d) version-checked move
    const { rows: moved } = await c.query(
      `UPDATE opportunities o
       SET stage_id = $3, version = o.version + 1, updated_at = now()
       FROM unnest($1::bigint[], $2::int[]) AS u(id, expected_version)
       WHERE o.id = u.id
         AND o.workspace_id = $4
         AND o.version = u.expected_version
         AND o.stage_id <> $3
       RETURNING o.id`,
      [ids, versions, job.target_stage_id, job.workspace_id],
    );
    const movedIds = moved.map((r) => r.id);

    // e) transitions (unique (job_id, opportunity_id) makes a double write impossible)
    if (movedIds.length) {
      await c.query(
        `INSERT INTO stage_transitions (workspace_id, opportunity_id, from_stage_id, to_stage_id, source, job_id)
         SELECT $1, i.opportunity_id, i.from_stage_id, $2, 'bulk', $3
         FROM bulk_job_items i
         WHERE i.job_id = $3 AND i.opportunity_id = ANY($4::bigint[])`,
        [job.workspace_id, job.target_stage_id, job.id, movedIds],
      );
    }

    // f) item outcomes
    await c.query(
      `UPDATE bulk_job_items
       SET status = CASE WHEN opportunity_id = ANY($3::bigint[]) THEN 'applied' ELSE 'skipped_modified' END
       WHERE job_id = $1 AND opportunity_id = ANY($2::bigint[])`,
      [job.id, ids, movedIds],
    );

    // g) progress + lease renewal, fenced again
    const applied = movedIds.length;
    const skipped = ids.length - applied;
    const upd = await c.query(
      `UPDATE bulk_jobs
       SET cursor_id = $3, applied_count = applied_count + $4, skipped_count = skipped_count + $5,
           last_progress_at = now(), heartbeat_at = now(),
           lease_until = now() + make_interval(secs => $6)
       WHERE id = $1 AND attempt = $2`,
      [job.id, job.attempt, ids.at(-1), applied, skipped, LEASE_SECONDS],
    );
    if (!upd.rowCount) throw new LostOwnershipError(job.id);

    return { processed: ids.length, applied, skipped, done: false };
  });
}

/** Renew the lease without doing work (used when pacing makes us wait). */
export async function heartbeat(pool, job) {
  const { rowCount } = await pool.query(
    `UPDATE bulk_jobs SET heartbeat_at = now(), lease_until = now() + make_interval(secs => $3)
     WHERE id = $1 AND attempt = $2 AND status = 'running'`,
    [job.id, job.attempt, LEASE_SECONDS],
  );
  if (!rowCount) throw new LostOwnershipError(job.id);
}

/**
 * Mark completed — but only if it is TRUE: no pending items left and the counters add up.
 * "Completed" is a claim the system makes to the user; we verify it in the same statement.
 */
export async function completeJob(pool, job) {
  const { rows } = await pool.query(
    `UPDATE bulk_jobs j
     SET status = 'completed', finished_at = now(), lease_until = NULL, worker_id = NULL
     WHERE j.id = $1 AND j.attempt = $2 AND j.status = 'running'
       AND j.applied_count + j.skipped_count = j.total_count
       AND NOT EXISTS (SELECT 1 FROM bulk_job_items i WHERE i.job_id = j.id AND i.status = 'pending')
     RETURNING j.id`,
    [job.id, job.attempt],
  );
  if (!rows.length) {
    // Either we lost ownership, or the invariant is broken. Distinguish, never guess.
    const { rows: [j] } = await pool.query('SELECT attempt, status FROM bulk_jobs WHERE id = $1', [job.id]);
    if (j.attempt !== job.attempt || j.status !== 'running') throw new LostOwnershipError(job.id);
    await failJob(pool, job, 'completion check failed: counters do not match items');
    return false;
  }
  return true;
}

/** Hand the job back. graceful=true (shutdown): no failure counted, available immediately.
 *  graceful=false (error): failure counted, exponential backoff before the next claim. */
export async function releaseJob(pool, job, { graceful, error }) {
  await pool.query(
    `UPDATE bulk_jobs
     SET status = CASE WHEN NOT $3 AND failures + 1 >= max_attempts THEN 'failed' ELSE 'queued' END,
         failures = failures + CASE WHEN $3 THEN 0 ELSE 1 END,
         finished_at = CASE WHEN NOT $3 AND failures + 1 >= max_attempts THEN now() END,
         next_run_at = now() + CASE WHEN $3 THEN interval '0'
                       ELSE make_interval(secs => LEAST(300, 2 ^ failures * 2)) END,
         error = $4, lease_until = NULL, worker_id = NULL
     WHERE id = $1 AND attempt = $2 AND status = 'running'`,
    [job.id, job.attempt, graceful, error ?? null],
  );
}

export async function failJob(pool, job, error) {
  await pool.query(
    `UPDATE bulk_jobs SET status = 'failed', error = $3, finished_at = now(), lease_until = NULL, worker_id = NULL
     WHERE id = $1 AND attempt = $2`,
    [job.id, job.attempt, error],
  );
}

/**
 * Reaper: a job whose owner keeps dying is never claimable again (failures >= max_attempts),
 * so without this it would sit in 'running' forever. Mark it failed — never stuck.
 */
export async function reapExhausted(pool) {
  const { rows } = await pool.query(
    `UPDATE bulk_jobs
     SET status = 'failed', finished_at = now(), lease_until = NULL, worker_id = NULL,
         error = COALESCE(error, 'retry budget exhausted (worker kept dying)')
     WHERE status = 'running' AND lease_until < now() AND failures + 1 >= max_attempts
     RETURNING id`,
  );
  return rows.map((r) => r.id);
}
