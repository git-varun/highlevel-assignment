import { Router } from 'express';
import { badRequest } from '../errors.js';
import { submitBulkMove, getBulkMove } from '../../domain/bulkMoves.js';

export function bulkMovesRouter(pool) {
  const r = Router();

  r.post('/', async (req, res) => {
    const key = req.get('Idempotency-Key');
    // Required, not derived from the body: two identical moves on different days are
    // legitimately different intents; only the client knows "this is a retry".
    if (!key || key.length > 200) throw badRequest('Idempotency-Key header is required (max 200 chars)');

    const { job, replayed } = await submitBulkMove(pool, req.workspaceId, key, req.body);
    res.set('Location', `/bulk-moves/${job.id}`);
    if (replayed) res.set('Idempotent-Replayed', 'true');
    res.status(202).json(job);   // same status for first call and retry: indistinguishable to the client
  });

  r.get('/:id', async (req, res) => {
    res.json(await getBulkMove(pool, req.workspaceId, req.params.id));
  });

  return r;
}
