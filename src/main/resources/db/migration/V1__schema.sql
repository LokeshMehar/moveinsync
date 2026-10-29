-- Time authority: every "is it due / is the lease alive" decision is made with the database clock
-- (clock_timestamp()), never with a node's wall clock, so node clock skew cannot cause early/double fires.

CREATE SEQUENCE fence_seq;

CREATE TABLE tenant (
    id               TEXT PRIMARY KEY,
    name             TEXT        NOT NULL,
    time_zone        TEXT        NOT NULL DEFAULT 'UTC',
    max_concurrency  INT         NOT NULL DEFAULT 100 CHECK (max_concurrency > 0),
    paused           BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE app_user (
    username       TEXT PRIMARY KEY,
    password_hash  TEXT NOT NULL,
    role           TEXT NOT NULL CHECK (role IN ('EMPLOYEE', 'ADMIN', 'OPERATOR')),
    tenant_id      TEXT REFERENCES tenant (id)
);

-- Job definition. One-off jobs have run_at; recurring jobs have cron + next_fire_at.
CREATE TABLE job (
    id               UUID PRIMARY KEY,
    tenant_id        TEXT        NOT NULL REFERENCES tenant (id),
    name             TEXT        NOT NULL,
    type             TEXT        NOT NULL,
    cron             TEXT,
    time_zone        TEXT        NOT NULL,
    run_at           TIMESTAMPTZ,
    payload          JSONB       NOT NULL DEFAULT '{}',
    priority         INT         NOT NULL DEFAULT 5,
    max_run_seconds  INT         NOT NULL DEFAULT 60,
    max_attempts     INT         NOT NULL DEFAULT 5,
    misfire_policy   TEXT        NOT NULL CHECK (misfire_policy IN ('FIRE_NOW', 'SKIP', 'COALESCE')),
    partition_id     INT         NOT NULL,
    next_fire_at     TIMESTAMPTZ,
    active           BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX job_materialize_idx ON job (partition_id, next_fire_at)
    WHERE active AND cron IS NOT NULL AND next_fire_at IS NOT NULL;
CREATE INDEX job_tenant_idx ON job (tenant_id, created_at DESC);

-- One row per occurrence. (job_id, scheduled_at) is the idempotency identity of an occurrence.
CREATE TABLE job_run (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id            UUID        NOT NULL REFERENCES job (id) ON DELETE CASCADE,
    tenant_id         TEXT        NOT NULL,
    partition_id      INT         NOT NULL,
    type              TEXT        NOT NULL,
    payload           JSONB       NOT NULL DEFAULT '{}',
    priority          INT         NOT NULL,
    max_run_seconds   INT         NOT NULL,
    max_attempts      INT         NOT NULL,
    misfire_policy    TEXT        NOT NULL,
    scheduled_at      TIMESTAMPTZ NOT NULL,   -- what the schedule said (fire-lag baseline)
    run_at            TIMESTAMPTZ NOT NULL,   -- earliest time it may be claimed (moves on retry)
    status            TEXT        NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'DEAD', 'SKIPPED')),
    attempts          INT         NOT NULL DEFAULT 0,
    fence             BIGINT      NOT NULL DEFAULT 0,
    lease_owner       TEXT,
    lease_expires_at  TIMESTAMPTZ,
    first_started_at  TIMESTAMPTZ,
    finished_at       TIMESTAMPTZ,
    last_error        TEXT,
    coalesced_count   INT         NOT NULL DEFAULT 1,
    idempotency_key   TEXT        NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (job_id, scheduled_at)
);
CREATE INDEX run_pending_idx ON job_run (tenant_id, partition_id, run_at) WHERE status = 'PENDING';
CREATE INDEX run_running_idx ON job_run (tenant_id, lease_expires_at) WHERE status = 'RUNNING';
CREATE INDEX run_dead_idx    ON job_run (tenant_id, finished_at DESC) WHERE status = 'DEAD';
CREATE INDEX run_job_idx     ON job_run (job_id, scheduled_at DESC);

-- The externally visible side effect of a run. The primary key IS the exactly-once guarantee.
CREATE TABLE effect (
    idempotency_key  TEXT PRIMARY KEY,
    tenant_id        TEXT        NOT NULL,
    run_id           BIGINT      NOT NULL,
    kind             TEXT        NOT NULL,
    detail           TEXT,
    fence            BIGINT      NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX effect_tenant_idx ON effect (tenant_id, kind);

-- Cluster membership + partition ownership. One row per partition: no single hot lock row.
CREATE TABLE node (
    node_id         TEXT PRIMARY KEY,
    last_heartbeat  TIMESTAMPTZ NOT NULL
);

CREATE TABLE partition_lease (
    partition_id  INT PRIMARY KEY,
    owner         TEXT,
    epoch         BIGINT      NOT NULL DEFAULT 0,
    expires_at    TIMESTAMPTZ NOT NULL DEFAULT '-infinity'
);
