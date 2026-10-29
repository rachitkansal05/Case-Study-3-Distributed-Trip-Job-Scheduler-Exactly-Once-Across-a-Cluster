-- =====================================================================================
-- V200: Engine team indexes (Flyway range V200-V299).
-- =====================================================================================

-- Materializer: one-off jobs that still need their single run. The API creates a one-off job
-- with next_fire_at = run_at; the materializer inserts the run and clears the cursor, so this
-- partial index only ever holds pending one-offs (job_next_fire covers RECURRING only).
CREATE INDEX job_one_off_pending ON job (partition_id) WHERE enabled AND kind = 'ONE_OFF' AND next_fire_at IS NOT NULL;
