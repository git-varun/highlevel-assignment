-- Fairness across tenants: the claim serves the workspace that was served LEAST RECENTLY
-- (round-robin), not simply the globally oldest job. claimed_at is stamped on every claim.
ALTER TABLE bulk_jobs ADD COLUMN claimed_at timestamptz;
UPDATE bulk_jobs SET claimed_at = started_at;
CREATE INDEX bulk_jobs_ws_claimed ON bulk_jobs (workspace_id, claimed_at);
