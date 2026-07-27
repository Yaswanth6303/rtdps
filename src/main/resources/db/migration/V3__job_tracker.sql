CREATE TABLE job_tracker (
    id              VARCHAR(64)  PRIMARY KEY,
    job_type        VARCHAR(16)  NOT NULL,
    file_path       VARCHAR(1024),
    status          VARCHAR(32)  NOT NULL,
    execution_id    BIGINT,
    read_count      BIGINT       NOT NULL DEFAULT 0,
    write_count     BIGINT       NOT NULL DEFAULT 0,
    skip_count      BIGINT       NOT NULL DEFAULT 0,
    duration_ms     BIGINT       NOT NULL DEFAULT 0,
    error_message   TEXT,
    created_at      TIMESTAMP    NOT NULL,
    updated_at      TIMESTAMP
);

CREATE INDEX idx_job_tracker_created_at ON job_tracker (created_at DESC);
CREATE INDEX idx_job_tracker_status     ON job_tracker (status);
