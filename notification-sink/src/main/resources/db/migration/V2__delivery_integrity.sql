-- =====================================================================================
-- V2: integrity and bookkeeping for the delivery endpoint (POST /api/v1/deliveries).
--  * delivery: channel + SHA-256 of the payload (the payload itself is never stored, it may
--    hold phone numbers, e-mails and home locations); all columns mandatory.
--  * attempt_log: run_id, so per-run stats and fence-order audits are possible; all columns
--    mandatory; outcome restricted to the three DeliveryOutcome values.
-- V1 tables have no writer before this migration, so tightening NOT NULL is safe.
-- =====================================================================================

ALTER TABLE sink.delivery
  ADD COLUMN channel        TEXT NOT NULL,
  ADD COLUMN payload_sha256 TEXT NOT NULL,
  ALTER COLUMN run_id        SET NOT NULL,
  ALTER COLUMN fencing_epoch SET NOT NULL,
  ALTER COLUMN fencing_seq   SET NOT NULL,
  ALTER COLUMN delivered_at  SET NOT NULL,
  ADD CONSTRAINT delivery_channel_valid     CHECK (channel IN ('SMS', 'PUSH')),
  ADD CONSTRAINT delivery_payload_hash_hex  CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
  ADD CONSTRAINT delivery_token_nonnegative CHECK (fencing_epoch >= 0 AND fencing_seq >= 0);

ALTER TABLE sink.run_fence
  ADD CONSTRAINT run_fence_token_nonnegative CHECK (max_epoch >= 0 AND max_seq >= 0);

ALTER TABLE sink.attempt_log
  ADD COLUMN run_id BIGINT NOT NULL,
  ALTER COLUMN idem_key      SET NOT NULL,
  ALTER COLUMN fencing_epoch SET NOT NULL,
  ALTER COLUMN fencing_seq   SET NOT NULL,
  ALTER COLUMN outcome       SET NOT NULL,
  ALTER COLUMN at            SET NOT NULL,
  ADD CONSTRAINT attempt_log_outcome_valid
    CHECK (outcome IN ('DELIVERED', 'ALREADY_DELIVERED', 'STALE_TOKEN'));

-- per-run stats: count attempts by outcome for one run
CREATE INDEX attempt_log_run_outcome ON sink.attempt_log (run_id, outcome);
