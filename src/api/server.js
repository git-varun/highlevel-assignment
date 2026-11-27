import { createPool } from '../db.js';
import { createApp } from './app.js';

const pool = createPool('api');
const port = Number(process.env.PORT ?? 3000);
const server = createApp(pool).listen(port, () => console.log(`[api] listening on :${port}`));

// Graceful shutdown: stop accepting, let in-flight requests finish, then close the pool.
for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, () => {
    console.log(`[api] ${sig} received, draining`);
    server.close(() => pool.end().then(() => process.exit(0)));
    setTimeout(() => process.exit(1), 10_000).unref(); // hard stop if something hangs
  });
}
