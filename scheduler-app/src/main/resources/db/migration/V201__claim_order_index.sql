-- =====================================================================================
-- V201: claim-order index (Engine team).
--
-- The claim picks a tenant's due runs ORDER BY priority, due_at, id LIMIT k across the ~43
-- partitions a node polls. run_due leads with partition_id, so for a tenant with a deep backlog
-- (50,000 due runs) Postgres had to read and sort every due row on each claim. With tenant_id
-- leading, the scan walks the tenant's runs in claim order, filters the polled partitions, and
-- stops after k rows. Same partial predicate as run_due, so it only holds claimable runs.
-- =====================================================================================
CREATE INDEX run_claim_order ON job_run (tenant_id, priority, due_at, id)
    WHERE state IN ('SCHEDULED', 'RETRY_WAIT');
