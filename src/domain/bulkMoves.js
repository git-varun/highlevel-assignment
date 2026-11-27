// Bulk move: submit (idempotent, snapshot at submission) and progress read.
import { createHash } from 'node:crypto';
import { withTransaction } from '../db.js';
import { badRequest, notFound, unprocessable, tooManyRequests } from '../api/errors.js';
import { canonicalMoney, moneyCents } from './money.js';

export const MAX_JOB_SIZE = Number(process.env.MAX_JOB_SIZE ?? 50_000);
// Queue-flood guard: max unfinished (queued + running) jobs per workspace.
export const MAX_PENDING_JOBS = Number(process.env.MAX_PENDING_JOBS ?? 10);

const FILTER_KEYS = ['stageId', 'ownerId', 'status', 'valueMin', 'valueMax', 'createdFrom', 'createdTo'];
const STATUSES = ['open', 'won', 'lost', 'abandoned'];
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
// Date or full ISO timestamp, as a string (new Date(12345) or "2024" must not sneak through).
const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}(T[\d:.]+(Z|[+-]\d{2}:\d{2})?)?$/;

/**
 * Validate + canonicalise the request. The canonical form is what we hash, so
 * {"a":1,"b":2} and {"b":2,"a":1} — or "100" vs "100.00" — count as the SAME request.
 */
export function normalizeRequest(body) {
  if (!body || typeof body !== 'object') throw badRequest('JSON body required');
  const { targetStageId, filter = {} } = body;

  if (!Number.isSafeInteger(targetStageId) || targetStageId <= 0) throw badRequest('targetStageId must be a positive integer');
  // typeof null === 'object' in JS — reject it explicitly or Object.keys(null) throws a 500.
  if (filter === null || typeof filter !== 'object' || Array.isArray(filter)) throw badRequest('filter must be an object');

  const unknown = Object.keys(filter).filter((k) => !FILTER_KEYS.includes(k));
  if (unknown.length) throw badRequest(`Unknown filter keys: ${unknown.join(', ')}`); // typo'd filter must not silently match everything

  const f = {};
  if (filter.stageId != null) {
    if (!Number.isSafeInteger(filter.stageId) || filter.stageId <= 0) throw badRequest('filter.stageId must be a positive integer');
    f.stageId = filter.stageId;
  }
  if (filter.ownerId != null) {
    // typeof check first: RegExp.test(['<uuid>']) coerces the array to a string and passes.
    if (typeof filter.ownerId !== 'string' || !UUID_RE.test(filter.ownerId)) throw badRequest('filter.ownerId must be a UUID');
    f.ownerId = filter.ownerId.toLowerCase();
  }
  if (filter.status != null) {
    if (!STATUSES.includes(filter.status)) throw badRequest(`filter.status must be one of ${STATUSES.join('/')}`);
    f.status = filter.status;
  }
  for (const k of ['valueMin', 'valueMax']) {
    if (filter[k] != null) {
      const m = canonicalMoney(filter[k]);  // "100" and "100.00" → same canonical value, no floats
      if (!m) throw badRequest(`filter.${k} must be a non-negative amount (max 12 digits, 2 decimals)`);
      f[k] = m;
    }
  }
  for (const k of ['createdFrom', 'createdTo']) {
    if (filter[k] != null) {
      const d = typeof filter[k] === 'string' && ISO_DATE_RE.test(filter[k]) ? new Date(filter[k]) : null;
      if (!d || Number.isNaN(d.getTime())) throw badRequest(`filter.${k} must be an ISO date string`);
      f[k] = d.toISOString();
    }
  }
  if (f.valueMin && f.valueMax && moneyCents(f.valueMin) > moneyCents(f.valueMax)) throw badRequest('valueMin > valueMax');
  if (f.createdFrom && f.createdTo && f.createdFrom > f.createdTo) throw badRequest('createdFrom > createdTo');

  // Fixed key order → deterministic JSON → deterministic hash.
  const canonical = { targetStageId, filter: Object.fromEntries(FILTER_KEYS.filter((k) => k in f).map((k) => [k, f[k]])) };
  const requestHash = createHash('sha256').update(JSON.stringify(canonical)).digest('hex');
  return { ...canonical, requestHash };
}

/** Build the snapshot WHERE clause. Only filters actually given appear, so the planner
 *  sees a plain predicate it can match to opp_ws_stage_id. Values are always bind params. */
function filterSql(ws, f, targetStageId) {
  const where = ['workspace_id = $1', 'stage_id <> $2'];  // already in target → nothing to do
  const params = [ws, targetStageId];
  const add = (sql, v) => { params.push(v); where.push(sql.replace('?', `$${params.length}`)); };
  if (f.stageId) add('stage_id = ?', f.stageId);
  if (f.ownerId) add('owner_id = ?', f.ownerId);
  if (f.status) add('status = ?', f.status);
  if (f.valueMin) add('value >= ?', f.valueMin);
  if (f.valueMax) add('value <= ?', f.valueMax);
  if (f.createdFrom) add('created_at >= ?', f.createdFrom);
  if (f.createdTo) add('created_at <= ?', f.createdTo);
  return { where: where.join(' AND '), params };
}

/**
 * Submit. Returns { job, replayed }.
 *
 * Idempotency, step by step:
 *  - INSERT … ON CONFLICT DO NOTHING on UNIQUE(workspace_id, idempotency_key).
 *  - Two identical requests at the same instant: the second INSERT blocks on the unique
 *    index until the first transaction finishes. If the first COMMITs, the second sees
 *    the conflict and returns the same job (202, never 409). If the first ROLLs BACK
 *    (e.g. too many matches), the second simply proceeds as a fresh request.
 *  - Same key + different body → 422: returning the old job would make the client
 *    believe its NEW request was accepted.
 */
export async function submitBulkMove(pool, ws, idempotencyKey, body) {
  const req = normalizeRequest(body);

  return withTransaction(pool, async (c) => {
    await c.query(`SET LOCAL statement_timeout = '30s'`);

    // Serialize submits PER WORKSPACE for the rest of this transaction. Without it, two
    // concurrent submits could both count 9 pending jobs, both pass the cap, and leave 11.
    // Other workspaces use a different lock key, so they are never blocked by this.
    await c.query(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, [`bulk-submit:${ws}`]);

    // Replays must win over the cap: a client retrying a request we ALREADY accepted
    // gets its job back (202), never a 429 — otherwise a retry would look like a failure.
    const prior = await c.query(
      'SELECT id, request_hash FROM bulk_jobs WHERE workspace_id = $1 AND idempotency_key = $2',
      [ws, idempotencyKey],
    );
    if (prior.rowCount) {
      if (prior.rows[0].request_hash !== req.requestHash) {
        throw unprocessable('IDEMPOTENCY_KEY_REUSED', 'This Idempotency-Key was already used with a different request');
      }
      return { job: await readJob(c, ws, prior.rows[0].id), replayed: true };
    }

    // Validate before the cap, so a bad request gets the error that's actually about IT
    // (422 invalid stage), not a 429 that would invite the client to retry a hopeless request.
    const stage = await c.query('SELECT 1 FROM stages WHERE id = $1 AND workspace_id = $2', [req.targetStageId, ws]);
    if (!stage.rowCount) throw unprocessable('INVALID_STAGE', 'targetStageId does not exist in this workspace');

    const { rows: [{ pending }] } = await c.query(
      `SELECT count(*)::int AS pending FROM bulk_jobs
       WHERE workspace_id = $1 AND status IN ('queued', 'running')`, [ws]);
    if (pending >= MAX_PENDING_JOBS) {
      throw tooManyRequests('TOO_MANY_PENDING_JOBS',
        `This workspace already has ${pending} bulk moves pending; wait for some to finish, then retry`,
        { pending, max: MAX_PENDING_JOBS }, 10);
    }

    // ON CONFLICT stays as the hard guarantee even though the advisory lock makes a
    // conflict here practically impossible: never rely on a single layer.
    // created_at = clock_timestamp() (the moment of insert, after the lock), not now()
    // (transaction START), so created_at is the true acceptance time. The claim query's
    // oldest-first preference and any audit/support timeline read it that way.
    const ins = await c.query(
      `INSERT INTO bulk_jobs (workspace_id, idempotency_key, request_hash, target_stage_id, filter, created_at)
       SELECT $1, $2, $3, $4, $5, clock_timestamp()
       WHERE EXISTS (SELECT 1 FROM stages WHERE id = $4 AND workspace_id = $1)
       ON CONFLICT (workspace_id, idempotency_key) DO NOTHING
       RETURNING id`,
      [ws, idempotencyKey, req.requestHash, req.targetStageId, req.filter],
    );

    if (!ins.rowCount) {
      const existing = await c.query(
        'SELECT id, request_hash FROM bulk_jobs WHERE workspace_id = $1 AND idempotency_key = $2',
        [ws, idempotencyKey],
      );
      if (!existing.rowCount) throw unprocessable('INVALID_STAGE', 'targetStageId does not exist in this workspace');
      if (existing.rows[0].request_hash !== req.requestHash) {
        throw unprocessable('IDEMPOTENCY_KEY_REUSED', 'This Idempotency-Key was already used with a different request');
      }
      return { job: await readJob(c, ws, existing.rows[0].id), replayed: true };
    }

    const jobId = ins.rows[0].id;
    const { where, params } = filterSql(ws, req.filter, req.targetStageId);

    // The snapshot, taken NOW — at submission — because the job must change exactly the
    // deals the user saw and approved, nothing they never selected. ONE statement → one
    // consistent view of the data even under Read Committed. LIMIT cap+1 detects
    // "too many" without counting the whole match set.
    const snap = await c.query(
      `INSERT INTO bulk_job_items (job_id, opportunity_id, from_stage_id, expected_version)
       SELECT $${params.length + 1}, id, stage_id, version
       FROM opportunities WHERE ${where}
       ORDER BY id LIMIT ${MAX_JOB_SIZE + 1}`,
      [...params, jobId],
    );
    if (snap.rowCount > MAX_JOB_SIZE) {
      // Throwing rolls back the job row too, so the key is NOT burned: the client can
      // narrow the filter and retry with the same key.
      throw unprocessable('TOO_MANY_MATCHES', `Filter matches more than ${MAX_JOB_SIZE} opportunities; narrow it`, { max: MAX_JOB_SIZE });
    }

    // Empty match set: nothing to do, so the job is complete at birth (honest 100% of 0).
    await c.query(
      `UPDATE bulk_jobs SET total_count = $2,
         status = CASE WHEN $2 = 0 THEN 'completed' ELSE 'queued' END,
         finished_at = CASE WHEN $2 = 0 THEN now() END
       WHERE id = $1 AND workspace_id = $3`,
      [jobId, snap.rowCount, ws],
    );
    return { job: await readJob(c, ws, jobId), replayed: false };
  });
}

/**
 * Progress view. Every number comes from committed rows — nothing in worker memory —
 * so it stays truthful across restarts. Time comparisons use the DB clock (now()),
 * never the API server's clock, so clock skew between machines can't mislabel a job.
 */
async function readJob(q, ws, id) {
  const { rows } = await q.query(
    `SELECT id, status, target_stage_id, filter, total_count, applied_count, skipped_count,
            attempt, max_attempts, error, created_at, started_at, finished_at,
            heartbeat_at, last_progress_at, lease_until,
            (status = 'running' AND lease_until < now()) AS lease_expired,
            extract(epoch FROM now() - heartbeat_at)::int AS seconds_since_heartbeat
     FROM bulk_jobs WHERE id = $1 AND workspace_id = $2`,
    [id, ws],
  );
  const j = rows[0];
  if (!j) return null;
  const done = j.applied_count + j.skipped_count;
  return {
    id: j.id,
    status: j.status,
    // 'stalled' = the owner stopped heartbeating; the job will be reclaimed. Not the same as failed.
    health: j.status === 'running' && j.lease_expired ? 'stalled' : j.status,
    targetStageId: j.target_stage_id,
    filter: j.filter,
    progress: {
      total: j.total_count,
      applied: j.applied_count,
      skipped: j.skipped_count,
      pending: j.total_count - done,
      // floor, never round: 99.996% must not display as 100% while work remains
      percent: j.total_count === 0 ? 100 : Math.floor((done / j.total_count) * 10_000) / 100,
    },
    attempt: j.attempt,
    maxAttempts: j.max_attempts,
    error: j.error,
    createdAt: j.created_at,
    startedAt: j.started_at,
    finishedAt: j.finished_at,
    lastHeartbeatAt: j.heartbeat_at,
    lastProgressAt: j.last_progress_at,
    secondsSinceHeartbeat: j.seconds_since_heartbeat,
  };
}

export async function getBulkMove(pool, ws, id) {
  if (!UUID_RE.test(id)) throw notFound('Job not found');
  const job = await readJob(pool, ws, id);
  if (!job) throw notFound('Job not found');
  return job;
}
