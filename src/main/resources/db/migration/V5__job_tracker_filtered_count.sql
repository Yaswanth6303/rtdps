-- CustomerProcessor returns null for rows with invalid email — Spring Batch
-- counts those as "filtered" (not "skipped"), so they were invisible on the
-- tracker. This column closes the gap.

ALTER TABLE job_tracker
    ADD COLUMN filtered_count BIGINT NOT NULL DEFAULT 0;
