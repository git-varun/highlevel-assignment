// Tenancy, input safety, and the filter matrix (snapshot == exactly what the filter means).
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { ready, pool, api, makeWorkspace, dropWorkspace, submit, one, ownerId } from './helpers.js';

let A, B;
before(async () => { await ready(); A = await makeWorkspace({ deals: 2000 }); B = await makeWorkspace({ deals: 50 }); });
after(async () => { await dropWorkspace(A.ws); await dropWorkspace(B.ws); await pool.end(); });

test('tenancy: another workspace sees 404 for deals and jobs, 422 for foreign stages', async () => {
  const { id: dealId } = await one('SELECT id FROM opportunities WHERE workspace_id = $1 LIMIT 1', [A.ws]);
  const job = await submit(A.ws, { targetStageId: A.stages[1], filter: { stageId: A.stages[0], valueMax: '5' } });
  assert.equal(job.status, 202);

  const r1 = await api().post(`/opportunities/${dealId}/move`).set('X-Workspace-Id', B.ws).send({ toStageId: B.stages[1] });
  assert.equal(r1.status, 404, "B can't touch (or detect) A's deal");
  const r2 = await api().get(`/bulk-moves/${job.body.id}`).set('X-Workspace-Id', B.ws);
  assert.equal(r2.status, 404);
  const r3 = await submit(A.ws, { targetStageId: B.stages[1], filter: {} });
  assert.equal(r3.status, 422, "A can't target B's stage");
  const r4 = await api().post('/opportunities').set('X-Workspace-Id', A.ws)
    .send({ name: 'x', value: '1', stageId: B.stages[0], ownerId: ownerId(1) });
  assert.equal(r4.status, 422);
  const r5 = await api().get('/opportunities?stageId=1');
  assert.equal(r5.status, 400, 'no tenant header, no data');
});

test('hostile or malformed input is a 4xx, never a 500', async () => {
  const bad = [
    [{ targetStageId: A.stages[1], filter: null }, 400],
    [{ targetStageId: A.stages[1], filter: { stageID: A.stages[0] } }, 400],            // typo must not mean "everything"
    [{ targetStageId: A.stages[1], filter: { ownerId: [ownerId(1)] } }, 400],
    [{ targetStageId: A.stages[1], filter: { createdFrom: 12345 } }, 400],
    [{ targetStageId: A.stages[1], filter: { createdFrom: '2024-02-31' } }, 400],          // JS would roll into March
    [{ targetStageId: A.stages[1], filter: { createdFrom: '2024-01-01T00:00' } }, 400],    // no offset = host-dependent
    [{ targetStageId: A.stages[1], filter: { valueMin: '9007199254740993' } }, 400],
    [{ targetStageId: A.stages[1], filter: { valueMin: '10', valueMax: '9' } }, 400],
    [{ targetStageId: A.stages[1], filter: { stageId: "1; DROP TABLE opportunities" } }, 400],
    [{ targetStageId: '1' }, 400],
  ];
  for (const [b, code] of bad) {
    const r = await submit(A.ws, b);
    assert.equal(r.status, code, `${JSON.stringify(b)} → ${r.status} ${JSON.stringify(r.body)}`);
  }
  const noKey = await api().post('/bulk-moves').set('X-Workspace-Id', A.ws).send({ targetStageId: A.stages[1] });
  assert.equal(noKey.status, 400, 'Idempotency-Key is mandatory');
  const big = await api().post('/opportunities').set('X-Workspace-Id', A.ws).set('Content-Type', 'application/json')
    .send(JSON.stringify({ name: 'a'.repeat(200_000) }));
  assert.equal(big.status, 413);
});

// Every filter, alone and combined, must snapshot EXACTLY the deals the equivalent
// hand-written SQL selects — no more (moving deals the user didn't pick), no fewer.
test('filter matrix: snapshot matches an independent SQL count for each combination', async () => {
  const from = '2000-01-01T00:00:00Z';
  const since = new Date(Date.now() - 500 * 3600_000).toISOString();      // created ≤ 500h ago
  const cases = [
    [{ stageId: A.stages[0] }, `stage_id = ${A.stages[0]}`],
    [{ ownerId: ownerId(2) }, `owner_id = '${ownerId(2)}'`],
    [{ status: 'won' }, `status = 'won'`],
    [{ valueMin: '100', valueMax: '300.50' }, `value BETWEEN 100 AND 300.50`],
    [{ createdFrom: since }, `created_at >= '${since}'`],
    [{ createdFrom: from, createdTo: since }, `created_at BETWEEN '${from}' AND '${since}'`],
    [{ stageId: A.stages[2], ownerId: ownerId(1), status: 'open', valueMin: '50' },
      `stage_id = ${A.stages[2]} AND owner_id = '${ownerId(1)}' AND status = 'open' AND value >= 50`],
  ];
  const target = A.stages[3];
  for (const [filter, sql] of cases) {
    const r = await submit(A.ws, { targetStageId: target, filter });
    assert.equal(r.status, 202, JSON.stringify(r.body));
    const { expected } = await one(
      `SELECT count(*)::int expected FROM opportunities WHERE workspace_id = $1 AND stage_id <> $2 AND ${sql}`, [A.ws, target]);
    const { mismatched } = await one(
      `SELECT count(*)::int mismatched FROM bulk_job_items i
       WHERE i.job_id = $1 AND i.opportunity_id NOT IN (
         SELECT id FROM opportunities WHERE workspace_id = $2 AND ${sql})`, [r.body.id, A.ws]);
    assert.ok(expected > 0, `case ${JSON.stringify(filter)} should match something`);
    assert.equal(r.body.progress.total, expected, `count for ${JSON.stringify(filter)}`);
    assert.equal(mismatched, 0, `every snapshotted deal satisfies ${JSON.stringify(filter)}`);
    await pool.query(`UPDATE bulk_jobs SET status = 'failed' WHERE id = $1`, [r.body.id]); // keep under the pending cap
  }
});

test('list by stage: keyset pagination walks every deal exactly once', async () => {
  const stage = A.stages[1];
  const seen = [];
  let cursor = null;
  do {
    const r = await api().get(`/opportunities?stageId=${stage}&limit=97${cursor ? `&cursor=${cursor}` : ''}`).set('X-Workspace-Id', A.ws);
    assert.equal(r.status, 200);
    seen.push(...r.body.items.map((i) => i.id));
    cursor = r.body.nextCursor;
  } while (cursor);
  const { n } = await one('SELECT count(*)::int n FROM opportunities WHERE workspace_id = $1 AND stage_id = $2', [A.ws, stage]);
  assert.equal(seen.length, n);
  assert.equal(new Set(seen).size, n, 'no duplicates across pages');
  assert.deepEqual(seen, [...seen].sort((a, b) => a - b), 'stable order');
});
