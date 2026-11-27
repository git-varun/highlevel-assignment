-- 001_init.sql — core schema
-- Every guarantee in this system is a database constraint first, application code second.

CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid()

-- ─── Tenancy ────────────────────────────────────────────────────────────────
CREATE TABLE workspaces (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name        text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- Ordered pipeline stages, per workspace.
CREATE TABLE stages (
  id            bigserial PRIMARY KEY,
  workspace_id  uuid NOT NULL REFERENCES workspaces(id),
  name          text NOT NULL,
  position      int  NOT NULL,
  UNIQUE (workspace_id, position),
  -- Lets other tables use a composite FK (workspace_id, stage_id), so the DB itself
  -- refuses a stage that belongs to a different workspace.
  UNIQUE (workspace_id, id)
);

-- ─── Deals ──────────────────────────────────────────────────────────────────
-- bigint ids (not random UUIDs): a stable total order for keyset cursors, and
-- sequential inserts keep the B-tree compact.
CREATE TABLE opportunities (
  id            bigserial PRIMARY KEY,
  workspace_id  uuid NOT NULL REFERENCES workspaces(id),
  stage_id      bigint NOT NULL,
  name          text NOT NULL,
  value         numeric(14,2) NOT NULL CHECK (value >= 0),
  status        text NOT NULL DEFAULT 'open'
                CHECK (status IN ('open','won','lost','abandoned')),
  owner_id      uuid NOT NULL,
  -- Optimistic concurrency token. Bumped on every stage change.
  version       int  NOT NULL DEFAULT 1,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (workspace_id, stage_id) REFERENCES stages (workspace_id, id)
);

-- Serves: list-by-stage pagination AND the bulk filter's main predicate.
CREATE INDEX opp_ws_stage_id ON opportunities (workspace_id, stage_id, id);

-- ─── Audit trail ────────────────────────────────────────────────────────────
CREATE TABLE stage_transitions (
  id              bigserial PRIMARY KEY,
  workspace_id    uuid   NOT NULL,
  opportunity_id  bigint NOT NULL REFERENCES opportunities(id),
  from_stage_id   bigint NOT NULL,
  to_stage_id     bigint NOT NULL,
  source          text   NOT NULL CHECK (source IN ('manual','bulk')),
  job_id          uuid,                       -- set when source = 'bulk'
  created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX transitions_opp ON stage_transitions (opportunity_id, id);
-- Defence in depth: a job can never record two transitions for one deal,
-- no matter what bug or replay happens in application code.
CREATE UNIQUE INDEX transitions_job_opp_uniq
  ON stage_transitions (job_id, opportunity_id) WHERE job_id IS NOT NULL;

-- ─── Bulk jobs (this table IS the queue) ────────────────────────────────────
CREATE TABLE bulk_jobs (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id      uuid NOT NULL REFERENCES workspaces(id),

  -- Idempotency
  idempotency_key   text NOT NULL,
  request_hash      text NOT NULL,            -- detects same key + different body

  -- What to do
  target_stage_id   bigint NOT NULL,
  filter            jsonb  NOT NULL,

  -- Lifecycle
  status            text NOT NULL DEFAULT 'queued'
                    CHECK (status IN ('queued','running','completed','failed')),
  error             text,

  -- Committed progress (updated in the same transaction as the work)
  total_count       int    NOT NULL DEFAULT 0,
  applied_count     int    NOT NULL DEFAULT 0,
  skipped_count     int    NOT NULL DEFAULT 0,
  cursor_id         bigint NOT NULL DEFAULT 0, -- last opportunity_id fully processed

  -- Ownership / crash recovery
  worker_id         text,
  attempt           int  NOT NULL DEFAULT 0,   -- bumped on every claim = fencing token
  max_attempts      int  NOT NULL DEFAULT 5,
  lease_until       timestamptz,
  heartbeat_at      timestamptz,
  last_progress_at  timestamptz,
  next_run_at       timestamptz NOT NULL DEFAULT now(),  -- retry backoff

  created_at        timestamptz NOT NULL DEFAULT now(),
  started_at        timestamptz,
  finished_at       timestamptz,

  UNIQUE (workspace_id, idempotency_key),     -- the idempotency guarantee
  FOREIGN KEY (workspace_id, target_stage_id) REFERENCES stages (workspace_id, id)
);

-- Isolation: at most ONE running job per workspace, enforced by the DB.
CREATE UNIQUE INDEX bulk_jobs_one_running_per_ws
  ON bulk_jobs (workspace_id) WHERE status = 'running';

-- Keeps the claim query cheap: only live jobs are in this index.
CREATE INDEX bulk_jobs_claimable
  ON bulk_jobs (created_at) WHERE status IN ('queued','running');

-- ─── Snapshot of the deals a job will touch ─────────────────────────────────
CREATE TABLE bulk_job_items (
  job_id            uuid   NOT NULL REFERENCES bulk_jobs(id),
  opportunity_id    bigint NOT NULL,
  from_stage_id     bigint NOT NULL,
  expected_version  int    NOT NULL,          -- version at submit time
  status            text   NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','applied','skipped_modified','skipped_noop')),
  -- PK doubles as the keyset-cursor index: (job_id, opportunity_id > cursor) is a range scan.
  PRIMARY KEY (job_id, opportunity_id)
);
