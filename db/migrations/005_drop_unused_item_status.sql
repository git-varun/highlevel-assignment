-- 'skipped_noop' was never produced: deals already in the target stage are excluded from
-- the snapshot at submission, and a deal that reaches the target stage later has a bumped
-- version, so it is 'skipped_modified'. Remove the unreachable state from the contract.
ALTER TABLE bulk_job_items
  DROP CONSTRAINT bulk_job_items_status_check,
  ADD CONSTRAINT bulk_job_items_status_check
    CHECK (status IN ('pending', 'applied', 'skipped_modified'));
