-- A transition must belong to the same workspace as its deal. A mutation test showed that
-- a bug in tenant scoping could write a transition tagged workspace B for workspace A's
-- deal. Make the database refuse it, like the composite FKs on stages already do.
ALTER TABLE opportunities ADD CONSTRAINT opportunities_ws_id_uniq UNIQUE (workspace_id, id);
ALTER TABLE stage_transitions
  ADD CONSTRAINT stage_transitions_same_workspace
  FOREIGN KEY (workspace_id, opportunity_id) REFERENCES opportunities (workspace_id, id);
