// Real process crashes: spawn the actual worker binary and kill it with real signals.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { ready, pool, makeWorkspace, dropWorkspace, submit, one, jobStatus, waitDone, waitFor } from './helpers.js';
import { verifyJob } from '../scripts/verify-job.js';

const made = [];
before(async () => {
  await ready();
  const { n } = await one(`SELECT count(*)::int n FROM bulk_jobs WHERE status IN ('queued','running')`);
  assert.equal(n, 0, 'worker tests need an idle queue');
});
after(async () => { for (const w of made) await dropWorkspace(w); await pool.end(); });

const spawnWorker = () => {
  const p = spawn(process.execPath, ['src/worker/index.js'], {
    env: { ...process.env, LEASE_SECONDS: '2', WORKER_CONCURRENCY: '1', CHUNK_SIZE: '100', BULK_RATE_PER_SEC: '2000', POLL_MS: '50' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  p.log = '';
  p.stdout.on('data', (d) => { p.log += d; });
  p.stderr.on('data', (d) => { p.log += d; });
  return p;
};

async function bigJob() {
  const W = await makeWorkspace({ deals: 12_000 });
  made.push(W.ws);
  const r = await submit(W.ws, { targetStageId: W.stages[1], filter: { stageId: W.stages[0] } });
  assert.equal(r.status, 202);
  return r.body.id; // 3,000 deals at 2,000/s ≈ 1.5s of work
}

test('SIGKILL mid-job: a new worker resumes from the committed cursor, nothing applied twice', async () => {
  const id = await bigJob();
  const w1 = spawnWorker();
  await waitFor(async () => (await jobStatus(id)).applied_count >= 800, { what: '800 applied' });
  w1.kill('SIGKILL');
  await once(w1, 'exit');
  const atKill = await jobStatus(id);
  assert.equal(atKill.status, 'running', 'a crashed job still looks running…');

  await waitFor(async () => (await jobStatus(id)).lease_until < new Date(), { what: 'lease expiry' }); // …until its lease expires
  const w2 = spawnWorker();
  try { await waitDone(id); } finally { w2.kill('SIGTERM'); await once(w2, 'exit'); }

  const job = await jobStatus(id);
  assert.equal(job.status, 'completed');
  assert.equal(job.attempt, 2);
  assert.equal(job.failures, 1, 'a crash costs one unit of retry budget');
  assert.match(w2.log, new RegExp(`resume_from=${atKill.cursor_id}\\b`), 'resumed exactly at the committed cursor');
  const v = await verifyJob(pool, id);
  assert.ok(v.ok, JSON.stringify(v.checks));
});

test('SIGTERM mid-job: graceful hand-off keeps progress and costs no retry budget', async () => {
  const id = await bigJob();
  const w1 = spawnWorker();
  await waitFor(async () => (await jobStatus(id)).applied_count >= 500, { what: '500 applied' });
  w1.kill('SIGTERM');
  const [code] = await once(w1, 'exit');
  assert.equal(code, 0, 'clean exit');
  const handedBack = await jobStatus(id);
  assert.equal(handedBack.status, 'queued', 'available immediately — no lease wait');
  assert.equal(handedBack.failures, 0);
  assert.ok(handedBack.applied_count >= 500, 'committed progress kept');

  const w2 = spawnWorker();
  try { await waitDone(id); } finally { w2.kill('SIGTERM'); await once(w2, 'exit'); }
  assert.ok((await verifyJob(pool, id)).ok);
});
