// Worker correctness: concurrency, fencing, scheduling, recovery. States are set up
// explicitly (no reliance on timing luck), and scheduling tests run with ONE slot —
// two racing slots once hid a livelock.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { ready, pool, api, makeWorkspace, dropWorkspace, submit, one, worker, jobStatus, waitDone, waitFor } from './helpers.js';
import { claimJob, runChunk, completeJob, releaseJob, reapExhausted, LostOwnershipError } from '../src/worker/jobs.js';
import { verifyJob } from '../scripts/verify-job.js';

const made = [];
const ws = async (o) => { const w = await makeWorkspace(o); made.push(w.ws); return w; };
before(async () => {
  await ready();
  // These tests run real workers that claim ANY job: refuse to run next to foreign work.
  const { n } = await one(`SELECT count(*)::int n FROM bulk_jobs WHERE status IN ('queued','running')`);
  assert.equal(n, 0, 'database has pending bulk jobs from elsewhere; worker tests need an idle queue');
});
after(async () => { for (const w of made) await dropWorkspace(w); await pool.end(); });

const submitMove = async (W, from, to, extra = {}) => {
  const r = await submit(W.ws, { targetStageId: W.stages[to], filter: { stageId: W.stages[from], ...extra } });
  assert.equal(r.status, 202);
  return r.body.id;
};
const runToCompletion = async (jobId, opts) => {
  const w = worker(opts);
  try { await waitDone(jobId); } finally { await w.stop(); }
  return jobStatus(jobId);
};

test('happy path: every snapshotted deal moves once; all invariants hold', async () => {
  const W = await ws({ deals: 1000 });
  const id = await submitMove(W, 0, 1);
  const job = await runToCompletion(id);
  assert.equal(job.status, 'completed');
  const v = await verifyJob(pool, id);
  assert.ok(v.ok, JSON.stringify(v.checks));
  assert.equal(job.applied_count, 250);
});

// Mechanism: version check in the chunk UPDATE (manual edit wins).
test('a deal changed after the snapshot is skipped, never overwritten', async () => {
  const W = await ws({ deals: 200 });
  const id = await submitMove(W, 0, 1);
  const { opportunity_id: dealId } = await one('SELECT opportunity_id FROM bulk_job_items WHERE job_id = $1 ORDER BY 1 DESC LIMIT 1', [id]);
  const res = await api().post(`/opportunities/${dealId}/move`).set('X-Workspace-Id', W.ws).send({ toStageId: W.stages[3] });
  assert.equal(res.status, 200);

  const job = await runToCompletion(id);
  assert.equal(job.status, 'completed');
  assert.equal(job.skipped_count, 1);
  const deal = await one('SELECT stage_id FROM opportunities WHERE id = $1', [dealId]);
  assert.equal(deal.stage_id, W.stages[3], "the user's choice stands");
  const item = await one('SELECT status FROM bulk_job_items WHERE job_id = $1 AND opportunity_id = $2', [id, dealId]);
  assert.equal(item.status, 'skipped_modified');
  const bulkMoves = await one(`SELECT count(*)::int n FROM stage_transitions WHERE job_id = $1 AND opportunity_id = $2`, [id, dealId]);
  assert.equal(bulkMoves.n, 0, 'no bulk transition written for the skipped deal');
  assert.ok((await verifyJob(pool, id)).ok);
});

// Mechanism: fencing on (id, attempt) with the job row locked in every chunk.
test('a zombie worker (lost its lease) cannot commit a chunk', async () => {
  const W = await ws({ deals: 400 });
  const id = await submitMove(W, 0, 1);
  const zombie = await claimJob(pool, 'A');
  assert.equal(zombie.id, id);
  await pool.query(`UPDATE bulk_jobs SET lease_until = now() - interval '1 second' WHERE id = $1`, [id]); // A froze
  const owner = await claimJob(pool, 'B');
  assert.equal(owner.id, id);
  assert.equal(owner.attempt, zombie.attempt + 1);

  await assert.rejects(runChunk(pool, zombie, 100), LostOwnershipError);
  const after = await jobStatus(id);
  assert.equal(after.applied_count, 0, 'zombie changed nothing');
  const { n } = await one(`SELECT count(*)::int n FROM stage_transitions WHERE job_id = $1`, [id]);
  assert.equal(n, 0);

  let r; do { r = await runChunk(pool, owner, 100); } while (!r.done);
  assert.ok(await completeJob(pool, owner));
  assert.ok((await verifyJob(pool, id)).ok);
});

// Mechanism: one running job per workspace (claim check + partial unique index).
test('two jobs in one workspace never run at the same time; other workspaces still run', async () => {
  const W = await ws({ deals: 200 });
  const W2 = await ws({ deals: 40 });
  const a = await submitMove(W, 0, 1);
  const b = await submitMove(W, 2, 3);
  const c = await submitMove(W2, 0, 1);
  const first = await claimJob(pool, 'w1');
  const second = await claimJob(pool, 'w2');
  assert.equal(first.id, a);
  assert.equal(second.id, c, "W's second job must wait; W2's job is free to run");
  assert.equal((await jobStatus(b)).status, 'queued');
  await assert.rejects(
    pool.query(`UPDATE bulk_jobs SET status = 'running' WHERE id = $1`, [b]),
    (e) => e.code === '23505', 'the database itself refuses a second running job');
  await releaseJob(pool, first, { graceful: true });
  await releaseJob(pool, second, { graceful: true });
});

// Regression (found in review): an expired running job + an older queued job in the
// same workspace livelocked the claim and starved EVERY workspace. One slot, no luck.
test('a crashed job does not livelock the queue (single slot)', async () => {
  const W = await ws({ deals: 120 });
  const K = await ws({ deals: 40 });
  const j1 = await submitMove(W, 0, 1);
  const j2 = await submitMove(W, 2, 3);
  const k = await submitMove(K, 0, 1);
  await pool.query(`UPDATE bulk_jobs SET created_at = now() - interval '3 min' WHERE id = $1`, [j1]);
  await pool.query(`UPDATE bulk_jobs SET status = 'running', attempt = 1, lease_until = now() - interval '1 min',
                    created_at = now() - interval '2 min' WHERE id = $1`, [j2]);            // owner crashed
  const w = worker({ concurrency: 1 });
  try {
    for (const id of [j1, j2, k]) await waitDone(id, 10_000);
  } finally { await w.stop(); }
  for (const id of [j1, j2, k]) assert.ok((await verifyJob(pool, id)).ok, `job ${id} verified`);
});

// Mechanism: no ordering within a workspace — backoff must not block independent jobs.
test('a job backing off does not block a later job in the same workspace', async () => {
  const W = await ws({ deals: 200 });
  const slow = await submitMove(W, 0, 1);
  const fast = await submitMove(W, 2, 3);
  await pool.query(`UPDATE bulk_jobs SET next_run_at = now() + interval '1 hour', failures = 1 WHERE id = $1`, [slow]);
  const job = await runToCompletion(fast);
  assert.equal(job.status, 'completed');
  assert.equal((await jobStatus(slow)).status, 'queued');
  await pool.query(`UPDATE bulk_jobs SET next_run_at = now() WHERE id = $1`, [slow]);
  assert.equal((await runToCompletion(slow)).status, 'completed');
});

// Mechanism: failures counter + backoff + reaper (no job stuck forever).
test('retry budget: graceful hand-off is free, errors back off, exhausted jobs are reaped', async () => {
  const W = await ws({ deals: 60 });
  const id = await submitMove(W, 0, 1);

  let j = await claimJob(pool, 'x');
  await releaseJob(pool, j, { graceful: true });
  let row = await jobStatus(id);
  assert.equal(row.failures, 0, 'SIGTERM hand-off costs nothing');
  assert.equal(row.status, 'queued');

  j = await claimJob(pool, 'x');
  await releaseJob(pool, j, { graceful: false, error: 'boom' });
  row = await jobStatus(id);
  assert.equal(row.failures, 1);
  assert.equal(row.error, 'boom');
  assert.ok(row.next_run_at > new Date(Date.now() + 1000), 'backs off before the next attempt');
  assert.equal(await claimJob(pool, 'x'), null, 'not claimable during backoff');

  // Crashed on its last allowed attempt: claim must refuse it and the reaper must fail it.
  await pool.query(`UPDATE bulk_jobs SET status = 'running', failures = max_attempts - 1, attempt = attempt + 1,
                    lease_until = now() - interval '1 second' WHERE id = $1`, [id]);
  assert.equal(await claimJob(pool, 'x'), null, 'claim and reaper never both want the same job');
  assert.deepEqual(await reapExhausted(pool), [id]);
  row = await jobStatus(id);
  assert.equal(row.status, 'failed');
  assert.match(row.error, /retry budget exhausted|boom/);
});

// Mechanism: completion is verified, not assumed.
test('a job whose counters do not add up is failed, never reported completed', async () => {
  const W = await ws({ deals: 40 });
  const id = await submitMove(W, 0, 1);
  const j = await claimJob(pool, 'x');
  let r; do { r = await runChunk(pool, j, 100); } while (!r.done);
  await pool.query('UPDATE bulk_jobs SET applied_count = applied_count - 1 WHERE id = $1', [id]); // corrupt
  assert.equal(await completeJob(pool, j), false);
  const row = await jobStatus(id);
  assert.equal(row.status, 'failed');
  assert.match(row.error, /completion check failed/);
});

// Mechanism: progress from committed state; floor; DB-clock lease expiry.
test('progress: never 100% while work remains; stalled when the lease expired', async () => {
  const W = await ws({ deals: 20 });
  const id = await submitMove(W, 0, 1);
  await pool.query(`UPDATE bulk_jobs SET status = 'running', total_count = 50000, applied_count = 49999,
                    lease_until = now() - interval '5 seconds', heartbeat_at = now() - interval '35 seconds'
                    WHERE id = $1`, [id]);
  const res = await api().get(`/bulk-moves/${id}`).set('X-Workspace-Id', W.ws);
  assert.equal(res.body.progress.percent, 99.99, '49,999 / 50,000 must not round to 100');
  assert.equal(res.body.health, 'stalled');
  assert.ok(res.body.secondsSinceHeartbeat >= 35);
  await pool.query(`UPDATE bulk_jobs SET status = 'failed' WHERE id = $1`, [id]);
});
