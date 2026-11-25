-- Single source of truth for every job: queue, in-flight state, and result store.
CREATE TABLE jobs (
    id               uuid        PRIMARY KEY,
    type             text        NOT NULL,
    payload          jsonb       NOT NULL DEFAULT 'null'::jsonb,
    priority         integer     NOT NULL DEFAULT 0,
    state            text        NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'dead_lettered', 'cancelled')),
    attempts         integer     NOT NULL DEFAULT 0,
    max_retries      integer     NOT NULL,
    timeout_seconds  integer     NOT NULL,
    run_at           timestamptz NOT NULL DEFAULT now(),
    lease_expires_at timestamptz,
    locked_by        text,
    last_error       text,
    result           jsonb,
    idempotency_key  text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    started_at       timestamptz,
    finished_at      timestamptz
);

-- Duplicate-submission detection.
CREATE UNIQUE INDEX jobs_idempotency_key_uq ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Claim path: only claimable rows are indexed, so the index stays small however much history accumulates.
CREATE INDEX jobs_claim_idx ON jobs (priority DESC, run_at, created_at) WHERE state IN ('queued', 'failed');

-- Reaper path: find expired leases.
CREATE INDEX jobs_lease_idx ON jobs (lease_expires_at) WHERE state = 'running';

-- Listing by state.
CREATE INDEX jobs_state_created_idx ON jobs (state, created_at DESC);

-- Dead-letter store: one row per job that exhausted its retries (or failed non-retryably).
CREATE TABLE dead_letters (
    job_id           uuid        PRIMARY KEY REFERENCES jobs (id) ON DELETE CASCADE,
    type             text        NOT NULL,
    payload          jsonb       NOT NULL,
    attempts         integer     NOT NULL,
    last_error       text,
    reason           text        NOT NULL,
    dead_lettered_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX dead_letters_time_idx ON dead_letters (dead_lettered_at DESC);

-- Operational switch read by every worker on every claim.
CREATE TABLE queue_control (
    id         integer     PRIMARY KEY CHECK (id = 1),
    paused     boolean     NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO queue_control (id, paused) VALUES (1, false);

