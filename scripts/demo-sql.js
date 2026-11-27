// Tiny SQL runner for scripts/demo.sh (so the demo needs no psql).
//   node scripts/demo-sql.js "SELECT … WHERE id = $1" arg1 …   → one line per row, columns joined by |
import { createPool } from '../src/db.js';

const pool = createPool('demo-sql');
try {
  const { rows } = await pool.query(process.argv[2], process.argv.slice(3));
  for (const r of rows) console.log(Object.values(r).map((v) => (v instanceof Date ? v.toISOString() : v)).join('|'));
} catch (e) {
  console.error(e.message);
  process.exitCode = 1;
} finally {
  await pool.end();
}
