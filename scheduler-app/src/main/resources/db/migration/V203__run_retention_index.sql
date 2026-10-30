-- =====================================================================================
-- V203: run retention (plan section 17).
--
-- RunPurge deletes finished runs (SUCCEEDED, SKIPPED, CANCELLED) older than the retention
-- (7 days by default), oldest first, in small batches. This partial index serves that scan: it
-- holds only finished runs, ordered by finish time, so a batch reads its rows from the front of
-- the index instead of scanning job_run. DEAD_LETTERED runs are never purged, so they are not in it.
-- =====================================================================================
CREATE INDEX run_finished ON job_run (finished_at) WHERE state IN ('SUCCEEDED', 'SKIPPED', 'CANCELLED');
