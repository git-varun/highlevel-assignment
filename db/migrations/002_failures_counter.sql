-- Split two concepts that were sharing the `attempt` column:
--   attempt  = fencing token. Bumped on EVERY claim, only ever increases.
--   failures = retry budget. Bumped only on crash (expired lease reclaimed) or error.
-- A graceful hand-off (SIGTERM during a deploy) bumps attempt but not failures,
-- so deploys never push a healthy job toward 'failed'.
ALTER TABLE bulk_jobs ADD COLUMN failures int NOT NULL DEFAULT 0;
