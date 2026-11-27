-- Snapshot filters without a stage (owner-only, created-range) seq-scanned the whole
-- opportunities table — every tenant's rows — inside the submit request.
-- (Production: CREATE INDEX CONCURRENTLY, outside a transaction, to avoid blocking writes.)
CREATE INDEX opp_ws_owner_id   ON opportunities (workspace_id, owner_id, id);
CREATE INDEX opp_ws_created_id ON opportunities (workspace_id, created_at, id);
-- status/value-only filters still scan the workspace's rows (low selectivity; an index
-- would rarely beat the scan). Documented as a 10x concern rather than indexed here.
