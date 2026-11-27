import { badRequest, notFound } from './errors.js';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// Tenant scoping. Every route below this middleware gets req.workspaceId,
// and every SQL statement in the domain layer MUST filter by it.
// (No auth per the brief — in production this id would come from the auth token, not a header.)
export function requireWorkspace(pool) {
  return async (req, res, next) => {
    const id = req.get('X-Workspace-Id');
    if (!id || !UUID_RE.test(id)) throw badRequest('X-Workspace-Id header must be a UUID');
    const { rowCount } = await pool.query('SELECT 1 FROM workspaces WHERE id = $1', [id]);
    if (!rowCount) throw notFound('Workspace not found');
    req.workspaceId = id;
    next();
  };
}
