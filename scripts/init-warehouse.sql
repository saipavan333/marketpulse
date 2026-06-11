-- ============================================================================
-- Bootstrap the warehouse database and user (runs once on first postgres boot)
-- The same Postgres instance hosts the Airflow metadata DB (database: airflow)
-- and the analytics warehouse (database: marketpulse).
-- ============================================================================
CREATE USER marketpulse WITH PASSWORD 'marketpulse';
CREATE DATABASE marketpulse OWNER marketpulse;
GRANT ALL PRIVILEGES ON DATABASE marketpulse TO marketpulse;

\connect marketpulse

CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION marketpulse;
CREATE SCHEMA IF NOT EXISTS marts AUTHORIZATION marketpulse;
CREATE SCHEMA IF NOT EXISTS ops AUTHORIZATION marketpulse;

-- Data-quality audit trail: every contract check run lands here
CREATE TABLE IF NOT EXISTS ops.dq_results (
    id           BIGSERIAL PRIMARY KEY,
    run_id       TEXT        NOT NULL,
    dataset      TEXT        NOT NULL,
    check_name   TEXT        NOT NULL,
    severity     TEXT        NOT NULL,
    passed       BOOLEAN     NOT NULL,
    observed     TEXT,
    threshold    TEXT,
    checked_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE ops.dq_results OWNER TO marketpulse;

-- Pipeline run ledger for observability / SLA tracking
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    id           BIGSERIAL PRIMARY KEY,
    run_id       TEXT        NOT NULL,
    job_name     TEXT        NOT NULL,
    status       TEXT        NOT NULL,
    rows_in      BIGINT,
    rows_out     BIGINT,
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ
);
ALTER TABLE ops.pipeline_runs OWNER TO marketpulse;
