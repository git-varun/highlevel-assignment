// HTTP layer: validate input → call domain → shape response. No SQL here.
import { Router } from 'express';
import { badRequest } from '../errors.js';
import { createOpportunity, moveOpportunity, listByStage } from '../../domain/opportunities.js';
import { canonicalMoney } from '../../domain/money.js';

const STATUSES = ['open', 'won', 'lost', 'abandoned'];
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const posInt = (v, field) => {
  const n = Number(v);
  if (!Number.isSafeInteger(n) || n <= 0) throw badRequest(`${field} must be a positive integer`);
  return n;
};

export function opportunitiesRouter(pool) {
  const r = Router();

  r.post('/', async (req, res) => {
    const { name, value, stageId, ownerId, status } = req.body ?? {};
    if (typeof name !== 'string' || !name.trim() || name.length > 500) throw badRequest('name is required (max 500 chars)');
    const money = canonicalMoney(value);
    if (!money) throw badRequest('value must be a non-negative amount (max 12 digits, 2 decimals)');
    if (typeof ownerId !== 'string' || !UUID_RE.test(ownerId)) throw badRequest('ownerId must be a UUID');
    if (status !== undefined && !STATUSES.includes(status)) throw badRequest(`status must be one of ${STATUSES.join('/')}`);
    const opp = await createOpportunity(pool, req.workspaceId, {
      name: name.trim(), value: money, stageId: posInt(stageId, 'stageId'), ownerId, status,
    });
    res.status(201).json(opp);
  });

  r.post('/:id/move', async (req, res) => {
    const { toStageId, expectedVersion } = req.body ?? {};
    const { opportunity, changed } = await moveOpportunity(pool, req.workspaceId, posInt(req.params.id, 'id'), {
      toStageId: posInt(toStageId, 'toStageId'),
      expectedVersion: expectedVersion === undefined ? undefined : posInt(expectedVersion, 'expectedVersion'),
    });
    res.status(200).json({ ...opportunity, changed });
  });

  r.get('/', async (req, res) => {
    const limit = Math.min(req.query.limit ? posInt(req.query.limit, 'limit') : 50, 200);
    const cursor = req.query.cursor ? posInt(req.query.cursor, 'cursor') : 0;
    res.json(await listByStage(pool, req.workspaceId, { stageId: posInt(req.query.stageId, 'stageId'), limit, cursor }));
  });

  return r;
}
