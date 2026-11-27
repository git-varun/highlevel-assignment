import pg from 'pg';

const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/bulkmove';

// bigint (int8) comes back as a string by default to avoid precision loss.
// Our ids stay far below 2^53, so parse them as numbers for ergonomic JSON.
pg.types.setTypeParser(20, (v) => Number(v));
// numeric (money) stays a string on purpose — never float money.

/**
 * Two pools = two independent connection budgets.
 * The worker can saturate its own pool without ever taking a connection
 * the API needs to serve interactive requests.
 */
export function createPool(role) {
  const sizes = { api: Number(process.env.API_POOL_MAX ?? 20), worker: Number(process.env.WORKER_POOL_MAX ?? 4) };
  const pool = new pg.Pool({
    connectionString: DATABASE_URL,
    max: sizes[role] ?? 5,
    application_name: role,          // visible in pg_stat_activity
    connectionTimeoutMillis: 5_000,  // fail loudly instead of hanging if the pool is exhausted
    idleTimeoutMillis: 30_000,
  });
  // An idle client erroring (e.g. DB restart) must not crash the process.
  pool.on('error', (err) => console.error(`[${role} pool] idle client error:`, err.message));
  return pool;
}

/**
 * Run fn inside one transaction on ONE checked-out client.
 * (pool.query would spread statements across connections — not a transaction.)
 */
export async function withTransaction(pool, fn) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {}); // connection may already be dead
    throw err;
  } finally {
    client.release();                                 // ALWAYS — or the pool leaks
  }
}
