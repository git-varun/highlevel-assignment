// Shared test helpers. Every test builds its OWN workspace (random UUID) and removes it
// afterwards — no test depends on seed data or on what another test left behind.
import { randomUUID } from 'node:crypto';
import request from 'supertest';
import { createPool } from '../src/db.js';
import { createApp } from '../src/api/app.js';
import { migrate } from '../scripts/migrate.js';
import { startWorker } from '../src/worker/index.js';

export const pool = createPool('test');
export const app = createApp(pool);
export const api = () => request(app);

export const ownerId = (n) => `bbbbbbbb-0000-0000-0000-${String(n).padStart(12, '0')}`;

let migrated = false;
export async function ready() {
  if (!migrated) { await migrate(pool); migrated = true; }
  // Safety net: a test process that crashed mid-run can't run its after() hooks.
  // Test files run one at a time, so any 'test …' workspace left now is garbage.
  const { rows } = await pool.query(`SELECT id FROM workspaces WHERE name LIKE 'test %'`);
  for (const { id } of rows) await dropWorkspace(id);
}

/**
 * A fresh workspace with `stages` stages and `deals` deals spread deterministically:
 * deal g → stage (g % stages), owner (g % 3), status cycle, value g % 1000 + .50,
 * created g hours ago. Deterministic so tests can compute expected matches in SQL.
 */
export async function makeWorkspace({ deals = 300, stages = 4 } = {}) {
  const ws = randomUUID();
  await pool.query('INSERT INTO workspaces (id, name) VALUES ($1, $2)', [ws, `test ${ws.slice(0, 8)}`]);
  const { rows } = await pool.query(
    `INSERT INTO stages (workspace_id, name, position)
     SELECT $1, 'S' || p, p FROM generate_series(1, $2) p ORDER BY p RETURNING id`,
    [ws, stages],
  );
  const stageIds = rows.map((r) => r.id);
  if (deals > 0) {
    await pool.query(
      `INSERT INTO opportunities (workspace_id, stage_id, name, value, status, owner_id, created_at)
       SELECT $1, ($2::bigint[])[1 + g % $3], 'D' || g, (g % 1000) + 0.50,
              (ARRAY['open','open','won','lost','abandoned'])[1 + g % 5],
              ($4::uuid[])[1 + g % 3], now() - make_interval(hours => g)
       FROM generate_series(1, $5) g`,
      [ws, stageIds, stages, [ownerId(1), ownerId(2), ownerId(3)], deals],
    );
  }
  return { ws, stages: stageIds };
}

/** Remove everything a test workspace created, children first (FK order). */
export async function dropWorkspace(ws) {
  await pool.query(`DELETE FROM stage_transitions WHERE workspace_id = $1`, [ws]);
  await pool.query(`DELETE FROM bulk_job_items WHERE job_id IN (SELECT id FROM bulk_jobs WHERE workspace_id = $1)`, [ws]);
  await pool.query(`DELETE FROM bulk_jobs WHERE workspace_id = $1`, [ws]);
  await pool.query(`DELETE FROM opportunities WHERE workspace_id = $1`, [ws]);
  await pool.query(`DELETE FROM stages WHERE workspace_id = $1`, [ws]);
  await pool.query(`DELETE FROM workspaces WHERE id = $1`, [ws]);
}

export const submit = (ws, body, key = randomUUID()) =>
  api().post('/bulk-moves').set('X-Workspace-Id', ws).set('Idempotency-Key', key).send(body);

export const one = async (sql, params) => (await pool.query(sql, params)).rows[0];

/** Poll fn() until truthy, or fail loudly after timeoutMs (no test can hang forever). */
export async function waitFor(fn, { timeoutMs = 20_000, intervalMs = 50, what = 'condition' } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

/** In-process worker for a test; returns { stop }. Fast defaults, quiet logs. */
export function worker(opts = {}) {
  return startWorker(pool, {
    id: `test-${randomUUID().slice(0, 4)}`, log: () => {},
    concurrency: 1, chunkSize: 100, ratePerSec: 0, pollMs: 20, ...opts,
  });
}

export const jobStatus = (id) => one('SELECT * FROM bulk_jobs WHERE id = $1', [id]);
export const waitDone = (id, ms) =>
  waitFor(async () => ['completed', 'failed'].includes((await jobStatus(id)).status), { timeoutMs: ms, what: `job ${id} to finish` });
