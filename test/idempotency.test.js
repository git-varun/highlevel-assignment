// Idempotency & submit-time rules. Each test names the mechanism it would catch removing.
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { ready, pool, api, makeWorkspace, dropWorkspace, submit, one } from './helpers.js';

let W, W2;
before(async () => {
  await ready();
  W = await makeWorkspace({ deals: 400 });
  W2 = await makeWorkspace({ deals: 50 });
});
after(async () => { await dropWorkspace(W.ws); await dropWorkspace(W2.ws); await pool.end(); });

const body = () => ({ targetStageId: W.stages[1], filter: { stageId: W.stages[0] } });

// Mechanism: UNIQUE(workspace_id, idempotency_key) + ON CONFLICT + per-workspace lock.
test('10 concurrent identical submits create exactly one job, all 202', async () => {
  const res = await Promise.all(Array.from({ length: 10 }, () => submit(W.ws, body(), 'same-key')));
  assert.deepEqual(res.map((r) => r.status), Array(10).fill(202));
  assert.equal(new Set(res.map((r) => r.body.id)).size, 1, 'every caller gets the same job');
  const { n } = await one('SELECT count(*)::int n FROM bulk_jobs WHERE workspace_id = $1 AND idempotency_key = $2', [W.ws, 'same-key']);
  assert.equal(n, 1);
});

// Mechanism: replay lookup — a retry must look like success, never 409.
test('retrying an accepted request returns 202 with the same job and a replay header', async () => {
  const first = await submit(W.ws, body(), 'retry-key');
  const again = await submit(W.ws, body(), 'retry-key');
  assert.equal(again.status, 202);
  assert.equal(again.body.id, first.body.id);
  assert.equal(again.headers['idempotent-replayed'], 'true');
});

// Mechanism: canonicalisation before hashing.
test('semantically equal bodies (key order, money format) are the same request', async () => {
  const a = await submit(W.ws, { targetStageId: W.stages[2], filter: { stageId: W.stages[3], valueMin: '0100', valueMax: '200.5' } }, 'canon');
  const b = await submit(W.ws, { filter: { valueMax: '200.50', valueMin: '100', stageId: W.stages[3] }, targetStageId: W.stages[2] }, 'canon');
  assert.equal(a.status, 202);
  assert.equal(b.status, 202);
  assert.equal(b.body.id, a.body.id);
});

// Mechanism: request_hash comparison.
test('same key with a different body is rejected with 422, not silently replayed', async () => {
  await submit(W.ws, body(), 'reuse');
  const res = await submit(W.ws, { targetStageId: W.stages[3], filter: { stageId: W.stages[0] } }, 'reuse');
  assert.equal(res.status, 422);
  assert.equal(res.body.error.code, 'IDEMPOTENCY_KEY_REUSED');
});

// Mechanism: snapshot LIMIT cap+1 + throw → full rollback (key not burned).
test('more matches than the cap → 422, and the same key still works afterwards', async () => {
  const huge = await makeWorkspace({ deals: 0 });
  try {
    await pool.query(
      `INSERT INTO opportunities (workspace_id, stage_id, name, value, owner_id)
       SELECT $1, $2, 'x', 1, gen_random_uuid() FROM generate_series(1, 50001)`, [huge.ws, huge.stages[0]]);
    const tooMany = await submit(huge.ws, { targetStageId: huge.stages[1], filter: { stageId: huge.stages[0] } }, 'cap-key');
    assert.equal(tooMany.status, 422);
    assert.equal(tooMany.body.error.code, 'TOO_MANY_MATCHES');
    const { n } = await one('SELECT count(*)::int n FROM bulk_jobs WHERE workspace_id = $1', [huge.ws]);
    assert.equal(n, 0, 'nothing persisted');
    const narrowed = await submit(huge.ws, { targetStageId: huge.stages[1], filter: { stageId: huge.stages[0], valueMin: '2' } }, 'cap-key');
    assert.equal(narrowed.status, 202, 'key was not burned by the rejected attempt');
  } finally { await dropWorkspace(huge.ws); }
});

// Mechanism: snapshot taken at submission, inside the submit transaction.
test('snapshot is frozen at submit: deals entering the filter later are not part of the job', async () => {
  const S = await makeWorkspace({ deals: 40 });
  try {
    const res = await submit(S.ws, { targetStageId: S.stages[1], filter: { stageId: S.stages[0] } });
    const total = res.body.progress.total;
    await pool.query(`UPDATE opportunities SET stage_id = $2, version = version + 1
                      WHERE workspace_id = $1 AND stage_id = $3`, [S.ws, S.stages[0], S.stages[2]]);
    const items = await one('SELECT count(*)::int n FROM bulk_job_items WHERE job_id = $1', [res.body.id]);
    assert.equal(items.n, total);
    const job = await one('SELECT total_count FROM bulk_jobs WHERE id = $1', [res.body.id]);
    assert.equal(job.total_count, total, 'total never changes after submit');
  } finally { await dropWorkspace(S.ws); }
});

// Mechanism: per-workspace advisory lock + replay-before-cap ordering.
test('pending cap: a concurrent burst of 12 → exactly 10 accepted, 2 × 429; replay at cap still 202', async () => {
  const C = await makeWorkspace({ deals: 50 });
  try {
    const res = await Promise.all(Array.from({ length: 12 }, (_, i) =>
      submit(C.ws, { targetStageId: C.stages[1], filter: { stageId: C.stages[0], valueMin: String(i) } }, `burst-${i}`)));
    const codes = res.map((r) => r.status).sort();
    assert.deepEqual(codes, [...Array(10).fill(202), 429, 429]);
    const rejected = res.find((r) => r.status === 429);
    assert.equal(rejected.headers['retry-after'], '10');
    const accepted = res.findIndex((r) => r.status === 202);
    const replay = await submit(C.ws, { targetStageId: C.stages[1], filter: { stageId: C.stages[0], valueMin: String(accepted) } }, `burst-${accepted}`);
    assert.equal(replay.status, 202, 'a retry of an accepted job is never 429');
    const other = await submit(W2.ws, { targetStageId: W2.stages[1], filter: { stageId: W2.stages[0] } });
    assert.equal(other.status, 202, 'other workspaces are unaffected');
  } finally { await dropWorkspace(C.ws); }
});

// Mechanism: the per-workspace advisory lock, tested DETERMINISTICALLY. The burst test
// above catches a missing lock only when the race happens to interleave; this one doesn't
// depend on timing: we hold the workspace's submit lock ourselves and prove a submit waits.
test('submits in one workspace are serialised; other workspaces are not blocked', async () => {
  const L = await makeWorkspace({ deals: 20 });
  const c = await pool.connect();
  try {
    await c.query('BEGIN');
    await c.query(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, [`bulk-submit:${L.ws}`]);
    let settled = false;
    const blocked = submit(L.ws, { targetStageId: L.stages[1], filter: { stageId: L.stages[0] } })
      .then((r) => { settled = true; return r; });
    const free = await submit(W2.ws, { targetStageId: W2.stages[2], filter: { stageId: W2.stages[3] } });
    assert.equal(free.status, 202, 'another workspace is not blocked by this lock');
    await new Promise((r) => setTimeout(r, 300));
    assert.equal(settled, false, 'submit must wait while another submit holds the workspace lock');
    await c.query('COMMIT');
    assert.equal((await blocked).status, 202, 'and proceeds once the lock is released');
  } finally {
    // If an assertion failed before COMMIT, this client still holds an open transaction
    // (and the lock). Releasing it as-is would poison the pool for every later query.
    await c.query('ROLLBACK').catch(() => {});
    c.release();
    await dropWorkspace(L.ws);
  }
});

// Mechanism: state check before version check in moveOpportunity.
test('single move: a retry after success is 200 changed=false; a stale version is a real 409', async () => {
  const { id, version } = await one('SELECT id, version FROM opportunities WHERE workspace_id = $1 AND stage_id = $2 LIMIT 1', [W.ws, W.stages[0]]);
  const move = (b) => api().post(`/opportunities/${id}/move`).set('X-Workspace-Id', W.ws).send(b);
  const first = await move({ toStageId: W.stages[2], expectedVersion: version });
  assert.equal(first.status, 200);
  assert.equal(first.body.changed, true);
  const retry = await move({ toStageId: W.stages[2], expectedVersion: version });
  assert.equal(retry.status, 200, 'retry must not be 409');
  assert.equal(retry.body.changed, false);
  const stale = await move({ toStageId: W.stages[3], expectedVersion: version });
  assert.equal(stale.status, 409);
  const { n } = await one(`SELECT count(*)::int n FROM stage_transitions WHERE opportunity_id = $1`, [id]);
  assert.equal(n, 1, 'exactly one transition recorded');
});
