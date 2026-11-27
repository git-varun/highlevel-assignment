-- Decision reversed: the snapshot is taken at SUBMISSION again, and there is no FIFO
-- ordering within a workspace.
--
-- Why: snapshot-at-start let a queued job pick up deals an EARLIER job moved into its
-- filter — deals the user never saw or selected. A bulk action must change exactly what
-- the user approved. Jobs are therefore independent, and strict ordering bought nothing
-- but head-of-line blocking. One running job per workspace stays (load isolation).
ALTER TABLE bulk_jobs DROP COLUMN estimated_count, DROP COLUMN snapshot_at;
DROP INDEX bulk_jobs_ws_fifo;
