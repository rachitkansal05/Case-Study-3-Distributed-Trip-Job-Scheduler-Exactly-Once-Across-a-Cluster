-- =====================================================================================
-- V1: scheduler schema (plan section 05, including design review fixes in section 21).
-- Owned by the Foundation team. Flyway ranges: Foundation V1-V9, API V100-V199,
-- Engine V200-V299. Never edit an applied migration; add a new one in your range.
--
-- All time comparisons in queries must use clock_timestamp() (not now(), which is the
-- transaction start time). The database clock is the only clock that decides due/lease.
-- =====================================================================================

-- ---------------------------------------------------------------- tenants and slots

CREATE TABLE tenant (
  id             TEXT PRIMARY KEY,
  time_zone      TEXT NOT NULL,                  -- IANA zone, e.g. 'Asia/Kolkata'
  paused         BOOLEAN NOT NULL DEFAULT false, -- read directly by the claim, never cached
  configured_cap INT NOT NULL DEFAULT 32,        -- cap the admin set; effective cap = slot rows (R7)
  version        BIGINT NOT NULL DEFAULT 0,
  CONSTRAINT tenant_id_not_blank     CHECK (length(btrim(id)) > 0),
  CONSTRAINT tenant_configured_cap_ok CHECK (configured_cap >= 0)
);

-- Exactly one row per unit of concurrency. Who holds a slot is recorded ONLY in job_run.slot_no.
CREATE TABLE tenant_slot (
  tenant_id TEXT NOT NULL REFERENCES tenant (id),
  slot_no   SMALLINT NOT NULL,                   -- 0 .. cap-1 (higher only while a reduction drains)
  PRIMARY KEY (tenant_id, slot_no),
  CONSTRAINT tenant_slot_no_non_negative CHECK (slot_no >= 0)
);

-- Single row. Operators raise cluster_generation when rebuilding from a fresh initdb (F5).
-- fencing_epoch = cluster_generation * 2^32 + timeline, so the cap keeps the epoch a non-negative BIGINT.
CREATE TABLE cluster_meta (
  id                 BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
  cluster_generation INT NOT NULL CHECK (cluster_generation BETWEEN 0 AND 2147483647)
);

-- ---------------------------------------------------------------- job definitions

CREATE TABLE job (
  id                UUID PRIMARY KEY,
  tenant_id         TEXT NOT NULL REFERENCES tenant (id),
  kind              TEXT NOT NULL,               -- ONE_OFF | RECURRING
  handler           TEXT NOT NULL,               -- PICKUP_ALERT | ROSTER_GEN | NO_SHOW_CANCEL | BILLING_CLOSE
  cron              TEXT,                        -- recurring only, wall-clock in tenant zone
  run_at            TIMESTAMPTZ,                 -- one-off only
  payload           JSONB NOT NULL,              -- may hold PII; never logged
  priority          SMALLINT NOT NULL DEFAULT 5, -- 1 = most urgent
  max_run_time      INTERVAL NOT NULL,
  misfire           TEXT NOT NULL,               -- FIRE_NOW | SKIP | COALESCE
  misfire_threshold INTERVAL NOT NULL DEFAULT '60 seconds',
  max_attempts      SMALLINT NOT NULL DEFAULT 5,
  partition_id      SMALLINT NOT NULL,           -- murmur3(id) % 128, fixed at creation
  next_fire_at      TIMESTAMPTZ,                 -- materialiser cursor
  version           BIGINT NOT NULL DEFAULT 0,   -- definition version (cache key)
  enabled           BOOLEAN NOT NULL DEFAULT true,
  CONSTRAINT job_kind_ok      CHECK (kind IN ('ONE_OFF', 'RECURRING')),
  CONSTRAINT job_handler_ok   CHECK (handler IN ('PICKUP_ALERT', 'ROSTER_GEN', 'NO_SHOW_CANCEL', 'BILLING_CLOSE')),
  CONSTRAINT job_misfire_ok   CHECK (misfire IN ('FIRE_NOW', 'SKIP', 'COALESCE')),
  CONSTRAINT job_schedule_ok  CHECK (
      (kind = 'ONE_OFF'   AND run_at IS NOT NULL AND cron IS NULL)
   OR (kind = 'RECURRING' AND cron IS NOT NULL   AND run_at IS NULL)),
  CONSTRAINT job_priority_ok      CHECK (priority BETWEEN 1 AND 9),
  CONSTRAINT job_max_run_time_ok  CHECK (max_run_time > interval '0'),
  CONSTRAINT job_misfire_thr_ok   CHECK (misfire_threshold >= interval '0'),
  CONSTRAINT job_max_attempts_ok  CHECK (max_attempts >= 1),
  CONSTRAINT job_partition_ok     CHECK (partition_id BETWEEN 0 AND 127)
);

CREATE INDEX job_tenant ON job (tenant_id);
-- materialiser: recurring jobs whose cursor is due, per partition
CREATE INDEX job_next_fire ON job (partition_id, next_fire_at) WHERE enabled AND kind = 'RECURRING';

-- ---------------------------------------------------------------- runs

CREATE SEQUENCE fence_seq CACHE 1;  -- strictly increasing within an epoch, takes no row lock

CREATE TABLE job_run (
  id               BIGSERIAL PRIMARY KEY,
  job_id           UUID NOT NULL REFERENCES job (id),
  tenant_id        TEXT NOT NULL REFERENCES tenant (id),
  partition_id     SMALLINT NOT NULL,
  priority         SMALLINT NOT NULL,            -- copied from job; claim orders by (priority, due_at)
  job_version      BIGINT NOT NULL,              -- job.version this run was materialised from
  max_run_time     INTERVAL NOT NULL,            -- copied from job at materialisation
  scheduled_at     TIMESTAMPTZ NOT NULL,         -- the occurrence instant
  due_at           TIMESTAMPTZ NOT NULL,         -- scheduled_at, or retry time
  idem_key         TEXT NOT NULL,                -- jobId:scheduledAtUTC | jobId:manual:requestId
  state            TEXT NOT NULL,
  attempt          SMALLINT NOT NULL DEFAULT 0,
  lease_lost_count SMALLINT NOT NULL DEFAULT 0,  -- lease-lost attempts; own limit (20) -> FENCING_CONFLICT
  fencing_epoch    BIGINT,                       -- cluster_generation * 2^32 + timeline (section 07)
  fencing_seq      BIGINT,                       -- nextval('fence_seq') on every claim
  slot_no          SMALLINT,                     -- tenant slot held while RUNNING, NULL otherwise
  owner_node       TEXT,
  lease_until      TIMESTAMPTZ,
  started_at       TIMESTAMPTZ,
  finished_at      TIMESTAMPTZ,
  checkpoint       JSONB,                        -- e.g. {"nextItem": 650} for chunked jobs
  coalesced_count  INT NOT NULL DEFAULT 1,
  last_error       TEXT,                         -- error code + sanitised message only
  CONSTRAINT job_run_occurrence_unique UNIQUE (job_id, scheduled_at),  -- materialisation is idempotent
  CONSTRAINT job_run_idem_key_unique   UNIQUE (idem_key),
  -- a running run's slot must be a real slot row of its own tenant
  CONSTRAINT job_run_slot_fk FOREIGN KEY (tenant_id, slot_no)
      REFERENCES tenant_slot (tenant_id, slot_no) ON DELETE RESTRICT,
  -- a running run always holds a slot and a token
  CONSTRAINT job_run_running_holds_slot
      CHECK (state <> 'RUNNING' OR (slot_no IS NOT NULL AND fencing_seq IS NOT NULL)),
  CONSTRAINT job_run_state_ok CHECK (state IN
      ('SCHEDULED', 'PAUSED', 'RUNNING', 'RETRY_WAIT', 'SUCCEEDED', 'SKIPPED', 'DEAD_LETTERED', 'CANCELLED')),
  CONSTRAINT job_run_priority_ok  CHECK (priority BETWEEN 1 AND 9),
  CONSTRAINT job_run_partition_ok CHECK (partition_id BETWEEN 0 AND 127),
  CONSTRAINT job_run_attempt_ok   CHECK (attempt >= 0 AND lease_lost_count >= 0),
  CONSTRAINT job_run_token_pair   CHECK ((fencing_epoch IS NULL) = (fencing_seq IS NULL))
);

-- no two running runs share a slot: running runs per tenant <= slot rows, enforced by the DB
CREATE UNIQUE INDEX one_run_per_slot ON job_run (tenant_id, slot_no) WHERE state = 'RUNNING';
-- claim: due runs per (partition, tenant), ordered by priority then due time
CREATE INDEX run_due     ON job_run (partition_id, tenant_id, priority, due_at) WHERE state IN ('SCHEDULED', 'RETRY_WAIT');
-- orphan sweep: expired RUNNING runs in any partition
CREATE INDEX run_expired ON job_run (lease_until) WHERE state = 'RUNNING';
CREATE INDEX run_tenant_state ON job_run (tenant_id, state);

-- ---------------------------------------------------------------- cluster

CREATE TABLE partition_lease (
  partition_id SMALLINT PRIMARY KEY,             -- 128 rows, one per partition
  owner_node   TEXT,
  epoch        BIGINT NOT NULL DEFAULT 0,
  lease_until  TIMESTAMPTZ,
  CONSTRAINT partition_lease_id_ok CHECK (partition_id BETWEEN 0 AND 127)
);

CREATE TABLE scheduler_node (
  node_id      TEXT PRIMARY KEY,
  heartbeat_at TIMESTAMPTZ NOT NULL,             -- DB clock
  started_at   TIMESTAMPTZ NOT NULL
);

-- ---------------------------------------------------------------- dead letters, API idempotency

CREATE TABLE dead_letter (
  run_id      BIGINT PRIMARY KEY REFERENCES job_run (id),
  tenant_id   TEXT NOT NULL,
  error_code  TEXT NOT NULL,
  attempts    SMALLINT NOT NULL,
  dead_at     TIMESTAMPTZ NOT NULL,
  replayed_at TIMESTAMPTZ
);
CREATE INDEX dead_letter_tenant ON dead_letter (tenant_id, dead_at);

-- retry-safe API writes (24 h retention)
CREATE TABLE api_idempotency (
  tenant_id    TEXT NOT NULL,
  idem_key     TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  response     JSONB NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, idem_key)
);
CREATE INDEX api_idempotency_created ON api_idempotency (created_at);

-- ---------------------------------------------------------------- seed data

INSERT INTO cluster_meta (id, cluster_generation) VALUES (true, 0);

INSERT INTO partition_lease (partition_id)
SELECT p FROM generate_series(0, 127) AS p;
