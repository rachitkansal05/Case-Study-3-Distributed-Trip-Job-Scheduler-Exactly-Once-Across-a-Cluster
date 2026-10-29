-- =====================================================================================
-- V1: notification-sink schema (plan section 05). Lives in its own `sink` schema and is
-- migrated only by the notification-sink service (its own Flyway history table).
-- =====================================================================================

CREATE SCHEMA IF NOT EXISTS sink;

-- One row per delivered effect. The primary key makes delivery exactly-once per idem key.
CREATE TABLE sink.delivery (
  idem_key      TEXT PRIMARY KEY,
  run_id        BIGINT,
  fencing_epoch BIGINT,
  fencing_seq   BIGINT,
  delivered_at  TIMESTAMPTZ
);
CREATE INDEX delivery_run ON sink.delivery (run_id);

-- Highest token seen per run; requests with a lower (epoch, seq) are rejected with 409.
CREATE TABLE sink.run_fence (
  run_id    BIGINT PRIMARY KEY,
  max_epoch BIGINT NOT NULL,
  max_seq   BIGINT NOT NULL
);
-- startup epoch guard: SELECT max(max_epoch) FROM sink.run_fence
CREATE INDEX run_fence_epoch ON sink.run_fence (max_epoch);

-- Every attempt, accepted or not, so tests can show at-least-once attempts vs exactly-once effects.
CREATE TABLE sink.attempt_log (
  id            BIGSERIAL PRIMARY KEY,
  idem_key      TEXT,
  fencing_epoch BIGINT,
  fencing_seq   BIGINT,
  outcome       TEXT,
  at            TIMESTAMPTZ
);
CREATE INDEX attempt_log_idem_key ON sink.attempt_log (idem_key);
