// Builds the app but does NOT listen — tests mount it with supertest, server.js listens.
import express from 'express';
import { requireWorkspace } from './workspace.js';
import { errorHandler } from './errors.js';
import { opportunitiesRouter } from './routes/opportunities.js';
import { bulkMovesRouter } from './routes/bulkMoves.js';

export function createApp(pool) {
  const app = express();
  app.use(express.json({ limit: '100kb' }));   // cap body size: cheap DoS protection

  app.get('/health', async (_req, res) => {
    await pool.query('SELECT 1');
    res.json({ ok: true });
  });

  // Everything below is tenant-scoped.
  app.use(requireWorkspace(pool));
  app.use('/opportunities', opportunitiesRouter(pool));
  app.use('/bulk-moves', bulkMovesRouter(pool));

  app.use(errorHandler);                        // must be last
  return app;
}
