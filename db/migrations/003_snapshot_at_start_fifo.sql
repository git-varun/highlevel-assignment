-- Snapshot moves from submit time to job start time; FIFO per workspace.
--   estimated_count: cheap count at submit (for the early 422 and for the UI)
--   snapshot_at:     when the worker froze the item list; NULL = not taken yet
ALTER TABLE bulk_jobs
  ADD COLUMN estimated_count int,
  ADD COLUMN snapshot_at timestamptz;

-- Jobs created before this migration were snapshotted at submit.
UPDATE bulk_jobs SET snapshot_at = created_at;

-- FIFO: "oldest unfinished job per workspace" is a DISTINCT ON over this index.
CREATE INDEX bulk_jobs_ws_fifo
  ON bulk_jobs (workspace_id, created_at, id) WHERE status IN ('queued', 'running');
