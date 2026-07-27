-- Honest accounting for import jobs: the previous code's writeCount counted
-- conflict no-ops as writes. These three columns let JobCompletionListener
-- surface the real picture per ConflictPolicy.

ALTER TABLE job_tracker
    ADD COLUMN inserted_count BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN updated_count  BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN conflict_count BIGINT NOT NULL DEFAULT 0;
