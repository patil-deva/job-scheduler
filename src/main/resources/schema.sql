CREATE TABLE IF NOT EXISTS jobs (
    id               BIGSERIAL PRIMARY KEY,
    organization_id  TEXT        NOT NULL,
    job_type         TEXT        NOT NULL,
    payload          TEXT        NOT NULL DEFAULT '',
    scheduled_at     TIMESTAMPTZ NOT NULL,
    earliest_run_at  TIMESTAMPTZ NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'PENDING',
    attempts         INT         NOT NULL DEFAULT 0,
    max_attempts     INT         NOT NULL DEFAULT 5,
    lease_token      UUID,
    leased_by        TEXT,
    lease_expires_at TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    last_error       TEXT,
    CONSTRAINT jobs_status_check CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS jobs_pending_due_idx
    ON jobs (scheduled_at, organization_id, id)
    WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS jobs_running_lease_idx
    ON jobs (lease_expires_at)
    WHERE status = 'RUNNING';

CREATE TABLE IF NOT EXISTS job_attempts (
    id          BIGSERIAL PRIMARY KEY,
    job_id      BIGINT      NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt     INT         NOT NULL,
    worker_id   TEXT        NOT NULL,
    lease_token UUID        NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    outcome     TEXT,
    CONSTRAINT job_attempts_unique UNIQUE (job_id, attempt)
);

CREATE TABLE IF NOT EXISTS job_effects (
    job_id     BIGINT PRIMARY KEY REFERENCES jobs (id) ON DELETE CASCADE,
    worker_id  TEXT        NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
