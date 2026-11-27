// Seed script.
//   npm run seed            → 1 large workspace (500,000 deals) + 5 small (3,000 each)
//   npm run seed -- --small → same shape, scaled down (20,000 + 5 × 500) for docker-compose/tests
//
// Rows are generated INSIDE Postgres with generate_series: one statement per batch
// instead of hundreds of thousands of round trips from Node.
// setseed() makes the dataset reproducible, so reviewers get the same distribution we benchmarked.
import { createPool, withTransaction } from '../src/db.js';

export const BIG_WS = '00000000-0000-0000-0000-000000000001';
export const SMALL_WS = [2, 3, 4, 5, 6].map((n) => `00000000-0000-0000-0000-00000000000${n}`);

// 12 stages with funnel-shaped weights (sum = 100): lots of early deals, few late ones.
export const STAGES = [
  ['New Lead', 22], ['Contacted', 16], ['Discovery', 12], ['Qualified', 10],
  ['Demo Scheduled', 8], ['Proposal Sent', 7], ['Negotiation', 5], ['Contract Sent', 3],
  ['Verbal Commit', 2], ['Closed Won', 6], ['Closed Lost', 7], ['On Hold', 2],
];

const OWNERS_PER_WS = 25;
const BATCH = 100_000;

async function createWorkspace(client, id, name) {
  await client.query('INSERT INTO workspaces (id, name) VALUES ($1, $2)', [id, name]);
  const { rows } = await client.query(
    `INSERT INTO stages (workspace_id, name, position)
     SELECT $1, s.name, s.pos FROM unnest($2::text[]) WITH ORDINALITY AS s(name, pos)
     RETURNING id, name`,
    [id, STAGES.map(([n]) => n)],
  );
  return Object.fromEntries(rows.map((r) => [r.name, r.id]));
}

async function seedDeals(client, wsId, stageIds, count) {
  // Weighted pick without a per-row subquery: an array of 100 stage ids where each
  // stage appears `weight` times; a random index into it is a weighted random choice.
  const weighted = STAGES.flatMap(([name, w]) => Array(w).fill(stageIds[name]));
  // Deterministic owner ids, e.g. aaaaaaaa-0000-0000-0001-000000000007 = owner 7 of workspace 1,
  // so benchmarks and tests can filter by a known owner.
  const owners = Array.from({ length: OWNERS_PER_WS }, (_, i) =>
    `aaaaaaaa-0000-0000-${wsId.slice(-4)}-${String(i + 1).padStart(12, '0')}`);

  for (let done = 0; done < count; done += BATCH) {
    const n = Math.min(BATCH, count - done);
    await client.query(
      `INSERT INTO opportunities (workspace_id, stage_id, name, value, status, owner_id, created_at, updated_at)
       SELECT $1, d.stage_id,
              'Deal #' || ($5::int + g),
              -- right-skewed values: many small deals, a long tail of big ones
              round((exp(random() * 6) * 150)::numeric, 2),
              CASE d.stage_id WHEN $6 THEN 'won' WHEN $7 THEN 'lost'
                   ELSE (CASE WHEN random() < 0.08 THEN 'abandoned' ELSE 'open' END) END,
              ($3::uuid[])[1 + floor(random() * $4)::int],
              d.created_at,
              d.created_at + random() * (now() - d.created_at)
       FROM generate_series(1, $8::int) AS g
       -- "+ 0 * g" references the outer row so Postgres re-evaluates random() per row
       CROSS JOIN LATERAL (
         SELECT ($2::bigint[])[1 + floor(random() * 100 + 0 * g)::int] AS stage_id,
                now() - random() * interval '540 days' AS created_at
       ) d`,
      [wsId, weighted, owners, owners.length, done, stageIds['Closed Won'], stageIds['Closed Lost'], n],
    );
  }
}

export async function seed(pool, { small = false } = {}) {
  const bigCount = small ? 20_000 : 500_000;
  const smallCount = small ? 500 : 3_000;
  const t0 = Date.now();

  await withTransaction(pool, async (c) => {
    await c.query('SELECT setseed(0.42)'); // reproducible dataset
    await c.query(`TRUNCATE workspaces, stages, opportunities, stage_transitions,
                   bulk_jobs, bulk_job_items RESTART IDENTITY CASCADE`);
    const bigStages = await createWorkspace(c, BIG_WS, 'Large workspace');
    await seedDeals(c, BIG_WS, bigStages, bigCount);
    for (const [i, id] of SMALL_WS.entries()) {
      const stages = await createWorkspace(c, id, `Small workspace ${i + 1}`);
      await seedDeals(c, id, stages, smallCount);
    }
  });
  await pool.query('ANALYZE'); // fresh planner statistics so EXPLAIN reflects reality

  const { rows } = await pool.query(
    `SELECT s.position, s.name, count(o.id)::int AS deals
     FROM stages s LEFT JOIN opportunities o ON o.stage_id = s.id
     WHERE s.workspace_id = $1 GROUP BY s.id ORDER BY s.position`, [BIG_WS]);
  console.log(`seeded ${bigCount + 5 * smallCount} deals in ${((Date.now() - t0) / 1000).toFixed(1)}s`);
  console.table(rows);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const pool = createPool('seed');
  seed(pool, { small: process.argv.includes('--small') })
    .catch((e) => { console.error(e); process.exitCode = 1; })
    .finally(() => pool.end());
}
