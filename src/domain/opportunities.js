// Domain layer: SQL + business rules. Knows nothing about Express (req/res),
// so the worker and tests can call it directly.
import { withTransaction } from '../db.js';
import { conflict, notFound, unprocessable } from '../api/errors.js';

const COLUMNS = `id, workspace_id, stage_id, name, value, status, owner_id, version, created_at, updated_at`;

export async function createOpportunity(pool, ws, { name, value, stageId, ownerId, status = 'open' }) {
  // Composite FK (workspace_id, stage_id) rejects a stage from another workspace → 23503 → 422.
  const { rows } = await pool.query(
    `INSERT INTO opportunities (workspace_id, stage_id, name, value, owner_id, status)
     VALUES ($1, $2, $3, $4, $5, $6) RETURNING ${COLUMNS}`,
    [ws, stageId, name, value, ownerId, status],
  );
  return rows[0];
}

/**
 * Move ONE deal to another stage. This is the path that collides with the bulk job,
 * so it is the one place in Part 1 where we are careful.
 *
 * Rules, in order:
 *  1. Lock the row (FOR UPDATE) so read → check → write is atomic.
 *  2. Already in the target stage → 200 no-op. A retried request returns success,
 *     not 409: the desired end state already holds (idempotent by state).
 *  3. Client sent expectedVersion and it's stale → 409 (a real conflict: someone else
 *     changed the deal since the user looked at it).
 *  4. Update stage + bump version + record the transition — same transaction.
 */
export async function moveOpportunity(pool, ws, id, { toStageId, expectedVersion }) {
  return withTransaction(pool, async (c) => {
    // If a bulk chunk holds this row, wait at most 2s, then 55P03 → 503 + Retry-After.
    // Chunks commit in milliseconds, so in practice we wait briefly and proceed.
    await c.query(`SET LOCAL lock_timeout = '2s'`);

    const { rows } = await c.query(
      `SELECT ${COLUMNS} FROM opportunities WHERE id = $1 AND workspace_id = $2 FOR UPDATE`,
      [id, ws],
    );
    const opp = rows[0];
    if (!opp) throw notFound('Opportunity not found');

    if (opp.stage_id === toStageId) return { opportunity: opp, changed: false };

    if (expectedVersion !== undefined && expectedVersion !== opp.version) {
      throw conflict('Opportunity was modified by someone else', { currentVersion: opp.version, currentStageId: opp.stage_id });
    }

    const stage = await c.query('SELECT 1 FROM stages WHERE id = $1 AND workspace_id = $2', [toStageId, ws]);
    if (!stage.rowCount) throw unprocessable('INVALID_STAGE', 'Target stage does not exist in this workspace');

    const updated = await c.query(
      `UPDATE opportunities SET stage_id = $3, version = version + 1, updated_at = now()
       WHERE id = $1 AND workspace_id = $2 RETURNING ${COLUMNS}`,
      [id, ws, toStageId],
    );
    await c.query(
      `INSERT INTO stage_transitions (workspace_id, opportunity_id, from_stage_id, to_stage_id, source)
       VALUES ($1, $2, $3, $4, 'manual')`,
      [ws, id, opp.stage_id, toStageId],
    );
    return { opportunity: updated.rows[0], changed: true };
  });
}

/**
 * List deals in a stage with keyset pagination: WHERE id > cursor ORDER BY id.
 * Uses index (workspace_id, stage_id, id): constant cost per page at any depth,
 * unlike OFFSET which re-reads every skipped row.
 */
export async function listByStage(pool, ws, { stageId, limit, cursor }) {
  const { rows } = await pool.query(
    `SELECT ${COLUMNS} FROM opportunities
     WHERE workspace_id = $1 AND stage_id = $2 AND id > $3
     ORDER BY id LIMIT $4`,
    [ws, stageId, cursor, limit + 1],  // fetch one extra to know if there is a next page
  );
  const hasMore = rows.length > limit;
  const items = hasMore ? rows.slice(0, limit) : rows;
  return { items, nextCursor: hasMore ? items.at(-1).id : null };
}
