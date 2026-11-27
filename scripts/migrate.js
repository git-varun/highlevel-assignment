// Minimal forward-only migration runner.
// Applies db/migrations/*.sql in name order, once each, recorded in schema_migrations.
import { readdir, readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { createPool, withTransaction } from '../src/db.js';

const dir = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'db', 'migrations');

export async function migrate(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
    name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`);

  const files = (await readdir(dir)).filter((f) => f.endsWith('.sql')).sort();
  for (const name of files) {
    const { rowCount } = await pool.query('SELECT 1 FROM schema_migrations WHERE name = $1', [name]);
    if (rowCount) continue;
    const sql = await readFile(path.join(dir, name), 'utf8');
    // Each migration + its bookkeeping row commit together: no half-applied schema.
    await withTransaction(pool, async (c) => {
      await c.query(sql);
      await c.query('INSERT INTO schema_migrations (name) VALUES ($1)', [name]);
    });
    console.log(`applied ${name}`);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const pool = createPool('migrate');
  migrate(pool)
    .then(() => console.log('migrations up to date'))
    .catch((e) => { console.error(e); process.exitCode = 1; })
    .finally(() => pool.end());
}
