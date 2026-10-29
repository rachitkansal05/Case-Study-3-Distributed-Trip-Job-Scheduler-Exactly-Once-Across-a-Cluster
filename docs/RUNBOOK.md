# On-call runbook: trip job scheduler

This runbook is for the on-call OPERATOR of the scheduler cluster. Every alert in [`ops/prometheus/alerts.yml`](../ops/prometheus/alerts.yml) links to a section here through its `runbook_url`. Design background is in [`PLAN.md`](../PLAN.md) (§07 claim and fencing, §09 retries and dead letters, §10 caps, §11 freeze and takeover, §12 failure modes).

**Where things are (compose demo):**

| What | URL / port |
|---|---|
| Grafana dashboard (uid `scheduler-overview`, admin / admin) | http://localhost:3000/d/scheduler-overview |
| Prometheus (alerts at `/alerts`) | http://localhost:9090 |
| Scheduler nodes `scheduler-1..3` | http://localhost:8081, :8082, :8083 |
| Notification sink | http://localhost:8090 |
| Toxiproxy API (fault injection) | http://localhost:8474 |
| Postgres | localhost:5432, db and user `scheduler` |

There is no Alertmanager in the demo. Firing alerts are listed at http://localhost:9090/alerts.

### Get an OPERATOR token (dev demo only)

`POST /dev/token` exists only when the `dev` Spring profile is active. Body: `tenant` (required, 1-64 chars; for an OPERATOR it only namespaces API idempotency keys), `role` (`EMPLOYEE`, `ADMIN` or `OPERATOR`), optional `subject`. The token lasts 1 hour.

`docker-compose.yml` starts the nodes with `SPRING_PROFILES_ACTIVE: json,dev`, so every demo node serves it. In production `dev` is never active and tokens come from the identity provider.

```bash
TOKEN=$(curl -s -X POST localhost:8081/dev/token \
  -H 'Content-Type: application/json' \
  -d '{"tenant":"ops","role":"OPERATOR"}' | jq -r .token)
AUTH="Authorization: Bearer $TOKEN"
```

The response is `{"token": "...", "tokenType": "Bearer", "expiresAt": "..."}`. The token is valid on every node, because all nodes share the HMAC secret.

### Open a read-only SQL session

```bash
docker compose exec -e PGOPTIONS='-c default_transaction_read_only=on' postgres psql -U scheduler -d scheduler
```

### Never do these

- **Never** `UPDATE job_run` state, slot, lease or token columns by hand, and never `INSERT` or `DELETE` rows of `tenant_slot`. Only the scheduler code writes them: `JdbcClaimProtocol` for claims, renewals, checkpoints and commits, and the API for replays and cap changes, always with the lock order slot row first, then run row. A manual write can break the cap or let two nodes run the same job.
- **Never** lower `fencing_epoch`, `fencing_seq` or `cluster_meta.cluster_generation`, and never edit the sink's `sink.run_fence` or `sink.delivery` tables. That turns off fencing and dedup.
- **Never** edit `partition_lease` or `scheduler_node`. Nodes fix ownership themselves within about 7 s.
- **Never** delete `dead_letter` rows. Replay them, or leave them.
- Killing or restarting a scheduler node **is** safe: its runs are taken over within 11.2 s and the sink drops the duplicates.

---

## First five minutes

1. Open http://localhost:9090/alerts and write down which alerts fire and since when. Several at once usually share one cause: start with `SchedulerTargetDown`, then `SchedulerPartitionsUnowned`, then the rest.
2. Check that every target is up:

   ```promql
   up{job=~"scheduler|notification-sink"}
   ```

3. Open the Grafana dashboard and read fire-lag p99, queue depth, outcomes and partitions per node for the last 30 minutes.
4. Check cluster membership and partition ownership. You want 3 live nodes and `ownedPartitions` = 128:

   ```bash
   curl -s -H "$AUTH" localhost:8081/api/v1/cluster \
     | jq '{observedAt, ownedPartitions, unownedPartitions, liveNodes}'
   ```

5. Check what is waiting and what is dead-lettered:

   ```promql
   max by (state) (scheduler_runs)
   ```

   ```promql
   max(scheduler_queue_depth)
   ```

   ```bash
   curl -s -H "$AUTH" 'localhost:8081/api/v1/dead-letters?size=20' \
     | jq -r '.items[] | [.runId, .tenantId, .errorCode, .attempts, .deadAt, .runState] | @tsv'
   ```

Then go to the alert's section below. If the cause is not clear after 15 minutes, escalate with the dashboard link, the alert names, and the output of the checks above.

---

## SchedulerFireLagHigh

**Severity:** critical, for 5m. SLO: p99 fire lag ≤ 2 s.

```promql
histogram_quantile(0.99, sum by (le) (rate(scheduler_fire_lag_seconds_bucket[5m]))) > 2
```

**What it means.** Runs start more than 2 s after their scheduled time (DB time `started_at - scheduled_at`, recorded at a run's first start only). Pickup alerts arrive late.

**Likely causes**
- Fewer nodes or partitions than normal (see `SchedulerTargetDown`, `SchedulerPartitionsUnowned`).
- One tenant is at its cap with a deep backlog. That is expected for that tenant only; lag stays normal for the others.
- The database is slow (`SchedulerClaimBatchSlow`), or the Hikari pool is exhausted.
- Workers are blocked on a slow sink (handler timeouts).
- A recovery backlog after downtime (misfire policy `FIRE_NOW` fires everything it missed).

**First 3 checks**

1. Which handler is late:

   ```promql
   histogram_quantile(0.99, sum by (le, handler) (rate(scheduler_fire_lag_seconds_bucket[5m])))
   ```

2. Is one tenant held back by its cap:

   ```promql
   topk(5, sum by (tenant) (increase(scheduler_tenant_cap_rejections_total[5m])))
   ```

3. Is the database or the sink slow:

   ```promql
   histogram_quantile(0.99, sum by (le) (rate(scheduler_claim_batch_seconds_bucket[5m])))
   ```

   ```promql
   histogram_quantile(0.99, sum by (le) (rate(sink_delivery_latency_seconds_bucket[5m])))
   ```

   Also run [oldest due-not-started runs](#oldest-due-not-started-runs) to see whether the backlog sits in one tenant or one partition.

**Safe actions**
- Bring missing nodes back (`docker compose up -d scheduler-2`). Partitions rebalance on their own.
- If one tenant's backlog is the cause and the business agrees, raise its cap (`PUT /api/v1/tenants/{t}/cap`, see `SchedulerTenantCapDraining`).
- If the sink is slow, follow `SchedulerErrorRateHigh`.
- **Do not** pause other tenants to "make room". Caps already isolate tenants, so pausing only adds lag.

---

## SchedulerDeadLettersIncreasing

**Severity:** warning, fires at once.

```promql
max(delta(scheduler_runs{state="dead_lettered"}[5m])) > 0
```

**What it means.** More runs are in state `DEAD_LETTERED` than 5 minutes ago. Each one is an occurrence that will not run again until someone replays it.

**Likely causes**
- `INVALID_PAYLOAD`, `INVALID_EFFECT_PAYLOAD`, `INVALID_CHECKPOINT`, `NO_HANDLER`, `JOB_NOT_FOUND`: bad job data. Dead-lettered on the first attempt.
- `SINK_REJECTED_<status>`: the sink answered with a 4xx. Permanent.
- `SINK_5XX`, `SINK_TIMEOUT`, `SINK_UNAVAILABLE`, `TIMEOUT`, `IO_ERROR`: retryable errors that used up all attempts (default 5).
- `FENCING_CONFLICT`: the run lost its lease 20 times. See `SchedulerLeaseLostSpike`.

**First 3 checks**

1. Dead letters by error code and tenant, last 24 h:

   ```sql
   SELECT error_code, tenant_id, count(*) AS runs, max(dead_at) AS newest
     FROM dead_letter
    WHERE dead_at >= clock_timestamp() - interval '24 hours' AND replayed_at IS NULL
    GROUP BY error_code, tenant_id
    ORDER BY runs DESC;
   ```

2. The same through the API (newest first):

   ```bash
   curl -s -H "$AUTH" 'localhost:8081/api/v1/dead-letters?tenant=acme&size=50' \
     | jq -r '.items[] | [.runId, .jobId, .errorCode, .attempts, .deadAt, .runState] | @tsv'
   ```

3. Outcome mix, to tell a permanent-error burst from retries running out:

   ```promql
   sum by (outcome) (rate(scheduler_run_outcome_total[5m]))
   ```

**Safe actions**
- Fix the cause first, then [replay](#replaying-dead-letters). Bad payloads need the job fixed by the tenant ADMIN; a replay without the fix just dead-letters again.
- For sink errors, wait until the sink is healthy (`SchedulerErrorRateHigh`), then replay.
- **Do not** set `job_run.state` back by hand. Replay through the API.

---

## SchedulerQueueDepthGrowing

**Severity:** warning, for 2m.

```promql
max(deriv(scheduler_queue_depth[2m])) > 0 and max(scheduler_queue_depth) > 0
```

**What it means.** The number of runs that are due but not started (state `SCHEDULED` or `RETRY_WAIT`, `due_at` in the past) keeps rising. Work arrives faster than the cluster starts it.

**Likely causes**
- A large burst for one tenant draining at its cap. Expected; check that other tenants are fine.
- A **paused tenant**: pause only sets `tenant.paused`, so its due runs stay `SCHEDULED` and are still counted in queue depth.
- Unowned partitions: nobody polls them.
- Nodes are down, or the database is slow.

**First 3 checks**

1. Backlog per tenant, with the paused flag. Use the [slot occupancy](#slot-occupancy-per-tenant) query and look at `due_waiting` and `paused`.
2. Is the backlog stuck in unowned partitions? Use the [oldest due-not-started runs](#oldest-due-not-started-runs) query and look at `partition_owned`.
3. Is the cluster starting work at all:

   ```promql
   sum(rate(scheduler_run_outcome_total[5m]))
   ```

   ```promql
   topk(5, sum by (tenant) (increase(scheduler_tenant_cap_rejections_total[5m])))
   ```

**Safe actions**
- Paused tenant, intentional: no action. Silence or ignore. Unintentional: resume it with `POST /api/v1/tenants/{t}/resume`. When resumed, the misfire policy decides what happens to the missed runs.
- Single-tenant burst at the cap: no action, or raise the cap if the business agrees.
- Otherwise follow `SchedulerPartitionsUnowned` or `SchedulerClaimBatchSlow`.

---

## SchedulerErrorRateHigh

**Severity:** critical, for 5m.

```promql
sum(rate(scheduler_run_outcome_total{outcome!="success"}[5m])) / sum(rate(scheduler_run_outcome_total[5m])) > 0.05
```

**What it means.** More than 5 % of finished attempts end `retryable`, `permanent` or `timeout`. Retries hide it for a while; dead letters come next.

**Likely causes**
- The sink is slow, down, or returning 5xx (in the demo, check for injected faults).
- A Toxiproxy toxic between a node and the sink (demo chaos left on).
- One tenant's jobs have bad payloads (`permanent`).
- A `maxRunTime` that is too short for the job (`timeout`).

**First 3 checks**

1. Which outcome:

   ```promql
   sum by (outcome) (rate(scheduler_run_outcome_total[5m]))
   ```

2. What the sink sees:

   ```promql
   sum by (outcome) (rate(sink_deliveries_total[5m]))
   ```

   ```bash
   curl -s -H 'X-Admin-Token: dev-only-sink-admin-token' localhost:8090/admin/faults
   curl -s localhost:8474/proxies | jq 'to_entries[] | {proxy: .key, enabled: .value.enabled, toxics: [.value.toxics[].type]}'
   ```

3. Which error codes the retrying runs carry, and for which tenants:

   ```sql
   SELECT tenant_id, split_part(last_error, ':', 1) AS error_code, count(*) AS runs
     FROM job_run
    WHERE state = 'RETRY_WAIT'
    GROUP BY 1, 2
    ORDER BY runs DESC;
   ```

**Safe actions**
- Demo: clear injected sink faults (`curl -X DELETE -H 'X-Admin-Token: dev-only-sink-admin-token' localhost:8090/admin/faults`) and remove leftover toxics (`curl -X DELETE localhost:8474/proxies/sink-scheduler-1/toxics/<toxic-name>`).
- Sink outage: let backoff work (full jitter, cap 5 min). Replay dead letters once the sink is healthy.
- One tenant with bad payloads: tell its ADMIN. Pausing the tenant (`POST /api/v1/tenants/{t}/pause`) is safe if it floods the sink with failures.
- **Do not** restart nodes to "clear" errors. Restarts do not fix the sink, and they add takeovers.

---

## SchedulerStaleCommitsRejected

**Severity:** warning, fires at once. Expected only during chaos tests.

```promql
sum(increase(scheduler_stale_commit_rejected_total[5m])) > 0
```

**What it means.** A node tried to commit a run after its lease had expired, and the fenced `UPDATE` matched 0 rows. Fencing worked: nothing wrong was written. But a node was stalled for more than its 10 s lease.

**Likely causes**
- A long GC pause or a frozen container (`docker pause`, the freeze chaos script).
- A network split or latency between one node and Postgres (Toxiproxy toxic on `pg-scheduler-N`).
- CPU starvation on the host.

**First 3 checks**

1. Which node:

   ```promql
   sum by (node) (increase(scheduler_stale_commit_rejected_total[5m]))
   ```

2. GC pauses on that node:

   ```promql
   max by (node) (jvm_gc_pause_seconds_max)
   ```

   ```promql
   sum by (node) (rate(jvm_gc_pause_seconds_sum[5m]))
   ```

3. Paused containers and DB toxics:

   ```bash
   docker ps --filter status=paused
   curl -s localhost:8474/proxies | jq 'to_entries[] | {proxy: .key, enabled: .value.enabled, toxics: [.value.toxics[].type]}'
   ```

**Safe actions**
- During a chaos test: none. That is the test passing.
- A paused container that is not part of a test: `docker compose unpause scheduler-2`.
- Repeated GC pauses: restart the node (safe), then look at heap use (`jvm_memory_used_bytes`) before raising heap.
- **Do not** raise the run lease to hide the problem. The 11.2 s takeover bound depends on it.

---

## SchedulerLeaseLostSpike

**Severity:** warning, fires at once.

```promql
sum by (reason) (increase(scheduler_lease_lost_total[5m])) > 5
```

**What it means.** Handlers stopped because their run was taken over. The `reason` label says how the node found out:

| reason | Meaning |
|---|---|
| stale_409 | the sink rejected the token: a newer claim exists |
| fenced_write_0_rows | a renewal, checkpoint or commit matched 0 rows |
| self_fence | the node's own monotonic deadline passed before it could renew |

Lease-lost attempts do not count toward `max_attempts`, but after 20 of them a run is dead-lettered with `FENCING_CONFLICT`.

`FENCING_CONFLICT` dead letters are counted by `scheduler_dead_letters_total{error_code="FENCING_CONFLICT"}` and alert as `SchedulerFencingConflictDeadLetters`. Check 3 lists them with SQL.

**Likely causes**
- Same as `SchedulerStaleCommitsRejected`: pauses, DB latency, CPU starvation.
- A node that cannot reach Postgres to renew (`SchedulerLeaseRenewFailures` fires too).
- `FENCING_CONFLICT` dead letters with no pauses in sight: a fencing bug. Escalate.

**First 3 checks**

1. Which node and reason:

   ```promql
   sum by (node, reason) (increase(scheduler_lease_lost_total[5m]))
   ```

2. Renew failures and takeovers in the same window:

   ```promql
   sum by (node) (increase(scheduler_lease_renew_failures_total[5m]))
   ```

   ```promql
   sum(increase(scheduler_orphan_takeovers_total[5m]))
   ```

3. `FENCING_CONFLICT` dead letters, and runs close to the limit:

   ```sql
   SELECT d.run_id, d.tenant_id, d.attempts, d.dead_at, d.replayed_at
     FROM dead_letter d
    WHERE d.error_code = 'FENCING_CONFLICT'
      AND d.dead_at >= clock_timestamp() - interval '24 hours'
    ORDER BY d.dead_at DESC;
   ```

   ```sql
   SELECT id, tenant_id, state, attempt, lease_lost_count, owner_node
     FROM job_run
    WHERE lease_lost_count >= 10 AND state IN ('RUNNING', 'RETRY_WAIT', 'SCHEDULED')
    ORDER BY lease_lost_count DESC
    LIMIT 20;
   ```

**Safe actions**
- Fix the stalled node (unpause, restart, remove the toxic).
- Replay `FENCING_CONFLICT` dead letters only after lease losses have stopped. Replay resets `lease_lost_count`.
- **Do not** replay in a loop while losses continue. The runs dead-letter again.

---

## SchedulerFencingConflictDeadLetters

**Severity:** critical, fires at once.

```promql
sum(increase(scheduler_dead_letters_total{error_code="FENCING_CONFLICT"}[15m])) > 0
```

**What it means.** A run lost its lease 20 times and was dead-lettered with `FENCING_CONFLICT` instead of being retried forever. The effect was not delivered twice (the sink's fencing check held), but the run has stopped and needs an operator.

**Likely causes**
- A node that keeps stalling (GC pauses, CPU starvation, `docker pause`) while holding the run. `SchedulerLeaseLostSpike` usually fired first.
- A handler that runs longer than the lease can be renewed under DB latency.
- None of the above: a fencing bug. Escalate with the run ids.

**First 3 checks**

1. Which error codes are dead-lettering, and how fast:

   ```promql
   sum by (error_code) (increase(scheduler_dead_letters_total[15m]))
   ```

2. Lease losses by node and reason over the same window:

   ```promql
   sum by (node, reason) (increase(scheduler_lease_lost_total[15m]))
   ```

3. The dead-lettered runs (same SQL as check 3 of [SchedulerLeaseLostSpike](#schedulerleaselostspike)).

**Safe actions**
- Fix or drain the stalling node first.
- Then replay the runs ([Replaying dead letters](#replaying-dead-letters)). The sink's idempotency key makes a replay safe even if the earlier attempt reached it.

---

## SchedulerLeaseRenewFailures

**Severity:** warning, fires at once.

```promql
sum by (node) (increase(scheduler_lease_renew_failures_total[2m])) > 0
```

**What it means.** A node's 3 s lease renewal failed or matched 0 rows. Each failure stops that run on the node; another node takes it over after the lease runs out.

**Likely causes**
- The node cannot reach Postgres, or reaches it slowly (Toxiproxy `pg-scheduler-N`, network).
- The Hikari pool on that node is exhausted.
- The node stalled past its lease (GC, pause).

**First 3 checks**

1. Connection pool on that node:

   ```promql
   max by (node) (hikaricp_connections_pending)
   ```

   ```promql
   sum by (node) (increase(hikaricp_connections_timeout_total[5m]))
   ```

2. Time to get a connection:

   ```promql
   sum by (node) (rate(hikaricp_connections_acquire_seconds_sum[5m])) / sum by (node) (rate(hikaricp_connections_acquire_seconds_count[5m]))
   ```

3. The node's DB proxy and its logs:

   ```bash
   curl -s localhost:8474/proxies/pg-scheduler-2 | jq '{enabled, toxics}'
   docker compose logs --since 10m --no-log-prefix scheduler-2 | grep '"level":"WARN"\|"level":"ERROR"' | tail -50
   ```

**Safe actions**
- Re-enable or clean the proxy: `curl -X POST localhost:8474/proxies/pg-scheduler-2 -d '{"enabled":true}'`.
- Restart the node if it stays unhealthy. Its runs move within 11.2 s.
- **Do not** raise lease durations or the self-fence margin in production config during an incident.

---

## SchedulerOrphanTakeoverSlow

**Severity:** critical, fires at once. Bound: 11.2 s (10 s lease + 1 s sweep + 0.2 s claim).

```promql
histogram_quantile(0.99, sum by (le) (rate(scheduler_orphan_takeover_time_seconds_bucket[10m]))) > 11.2
```

**What it means.** Runs whose node died took more than 11.2 s (lease expiry to restart, DB time) to restart. Buckets are coarse (5, 11, 30 s), so a value just over 11.2 means "some takeovers fell in the 11-30 s bucket".

**Likely causes**
- Every live node is busy: no worker free for recovered runs.
- The tenant has no free slot, because a slot is held by a run whose lease is still valid.
- Only one node, or no node, left alive.
- A slow database makes the 1 s orphan sweep fall behind.

**First 3 checks**

1. Takeover rate and live nodes:

   ```promql
   sum(increase(scheduler_orphan_takeovers_total[10m]))
   ```

   ```promql
   up{job="scheduler"}
   ```

2. Expired `RUNNING` runs nobody has picked up yet. Run the [stuck RUNNING runs](#stuck-running-runs) query; `expired_for` above 2 s means the sweep is behind.
3. Database latency:

   ```promql
   histogram_quantile(0.99, sum by (le) (rate(scheduler_claim_batch_seconds_bucket[5m])))
   ```

**Safe actions**
- Bring nodes back. A fresh node sweeps orphans in any partition immediately.
- If the database is slow, follow `SchedulerClaimBatchSlow`.
- **Do not** reap runs by hand (setting `RUNNING` to `RETRY_WAIT`). The sweep does it under the right locks.

---

## SchedulerPartitionsUnowned

**Severity:** critical, for 10s.

```promql
sum(scheduler_partitions_owned) != 128
```

**What it means.** The nodes together do not own exactly 128 partitions. Under 128: due runs in unowned partitions do not start (running jobs are still recovered by the orphan sweep). Over 128 for a few seconds during a handover is normal.

**Likely causes**
- A node died and its partition leases (6 s) have not been picked up yet. That should take about 7 s.
- A node cannot renew partition leases (DB reachability).
- A node is down and not scraped, so its share is missing from the sum while it still holds valid leases.

**First 3 checks**

1. Partitions per node:

   ```promql
   scheduler_partitions_owned
   ```

2. What the database says:

   ```bash
   curl -s -H "$AUTH" localhost:8081/api/v1/cluster \
     | jq '{ownedPartitions, unownedPartitions, liveNodes, unowned: [.partitions[] | select(.leaseValid | not) | .partitionId]}'
   ```

3. Partition owners and heartbeats: run the [partition owners](#partition-owners-and-live-nodes) queries.

**Safe actions**
- Wait 10 s. If it does not settle, restart or start the missing node.
- If the DB shows 128 owned but Prometheus does not, the problem is scraping (`SchedulerTargetDown`), not ownership.
- **Do not** edit `partition_lease`. A wrong `owner_node` or `epoch` makes nodes fight over partitions.

---

## SchedulerClaimBatchSlow

**Severity:** warning, for 5m.

```promql
histogram_quantile(0.99, sum by (le) (rate(scheduler_claim_batch_seconds_bucket[5m]))) > 0.1
```

**What it means.** The per-tenant claim transaction takes over 100 ms at p99. The database is the bottleneck, and fire lag follows.

**Likely causes**
- Postgres under load (benchmark, vacuum, a long-running query someone left open).
- Lock waits on slot or run rows (a long manual transaction holding locks).
- A bloated `job_run` table or a missing index after a migration.
- Pool exhaustion: time waiting for a connection counts too.

**First 3 checks**

1. Per node, to tell a node network problem from a DB problem:

   ```promql
   histogram_quantile(0.99, sum by (le, node) (rate(scheduler_claim_batch_seconds_bucket[5m])))
   ```

2. Pool pressure:

   ```promql
   max by (node) (hikaricp_connections_active)
   ```

   ```promql
   max by (node) (hikaricp_connections_pending)
   ```

3. Long or blocked queries in Postgres:

   ```sql
   SELECT pid, state, wait_event_type, wait_event,
          clock_timestamp() - xact_start AS xact_age, left(query, 80) AS query
     FROM pg_stat_activity
    WHERE datname = 'scheduler' AND state <> 'idle'
    ORDER BY xact_start NULLS LAST
    LIMIT 20;
   ```

**Safe actions**
- End an idle-in-transaction psql session that holds locks (yours or a colleague's) with pg_terminate_backend and its pid from the query above. Only for sessions you know are not scheduler nodes.
- Stop a running benchmark or load generator.
- **Do not** terminate scheduler node connections in bulk. They recover, but you add lease losses on top.

---

## SchedulerTenantCapDraining

**Severity:** warning, for 30m.

```promql
max by (tenant) (scheduler_tenant_cap{kind="effective"}) > max by (tenant) (scheduler_tenant_cap{kind="configured"})
```

**What it means.** A tenant's cap was lowered, but it still has more slot rows (effective cap) than the configured cap after 30 minutes. New runs of that tenant start only when running drops below the new cap. The 30 min bound is a fixed guess for "longest `maxRunTime`"; see the rule comment in `alerts.yml`.

**Likely causes**
- Long jobs (`max_run_time` over 30 min) still running in high slots. Expected; it drains when they finish.
- A slot held by a run that is stuck `RUNNING` with a lease that keeps being renewed.
- The slot compactor is not running (no live node).

**First 3 checks**

1. Gap per tenant:

   ```promql
   max by (tenant) (scheduler_tenant_cap{kind="effective"})
   ```

   ```promql
   max by (tenant) (scheduler_tenant_cap{kind="configured"})
   ```

2. Who holds the high slots (replace `acme`):

   ```sql
   SELECT s.slot_no, r.id AS run_id, r.owner_node, r.lease_until, r.max_run_time,
          clock_timestamp() - r.started_at AS since_first_start
     FROM tenant_slot s
     JOIN tenant t ON t.id = s.tenant_id
     LEFT JOIN job_run r ON r.tenant_id = s.tenant_id AND r.slot_no = s.slot_no AND r.state = 'RUNNING'
    WHERE s.tenant_id = 'acme' AND s.slot_no >= t.configured_cap
    ORDER BY s.slot_no;
   ```

3. Overall occupancy: the [slot occupancy](#slot-occupancy-per-tenant) query.

**Safe actions**
- Long jobs still running: wait. Nothing is wrong; the cap holds.
- To re-apply the cap (for example after it was changed twice), call the cap API again. It is idempotent with the same `Idempotency-Key`:

  ```bash
  curl -s -X PUT -H "$AUTH" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: cap-acme-$(date +%s)" \
    -d '{"cap":16}' localhost:8081/api/v1/tenants/acme/cap
  ```

  The response is `{"tenantId":"acme","configuredCap":16,"effectiveCap":...,"draining":...}`. Allowed cap range: 0 to 10000.
- **Do not** `DELETE FROM tenant_slot` to force the drain. `ON DELETE RESTRICT` blocks slots in use; the free ones the compactor removes on its own.

---

## SchedulerTargetDown

**Severity:** critical, for 30s.

```promql
up{job=~"scheduler|notification-sink"} == 0
```

**What it means.** Prometheus cannot scrape a scheduler node or the sink. If it is a node, its runs fail over within 11.2 s and its partitions within about 7 s. If it is the sink, every delivery fails and retries.

**Likely causes**
- The container crashed, is restarting, or is paused.
- The node refused to start: the **epoch guard** found the current fencing epoch lower than the sink's highest accepted epoch (database rebuilt from a fresh `initdb`).
- Flyway migration failed at startup.
- Out of memory.

**First 3 checks**

1. Container state:

   ```bash
   docker compose ps
   docker ps --filter status=paused
   ```

2. Last log lines of the target:

   ```bash
   docker compose logs --tail 100 --no-log-prefix scheduler-2 | grep '"level":"ERROR"\|cluster_generation\|Flyway'
   ```

3. Health and the epoch guard inputs:

   ```bash
   curl -s localhost:8082/actuator/health
   curl -s localhost:8090/api/v1/fence/highest-epoch
   ```

   ```sql
   SELECT cluster_generation FROM cluster_meta;
   ```

**Safe actions**
- `docker compose up -d scheduler-2` (or `unpause`). Safe at any time.
- Epoch guard refusing to start after a rebuild (PLAN F5): get a second person to confirm the database really is a fresh `initdb`, then **raise** the generation by one. This is the only manual write in this runbook:

  ```sql
  UPDATE cluster_meta SET cluster_generation = cluster_generation + 1;
  ```

  Run it in a normal (not read-only) psql session.
- **Never** lower `cluster_generation`, and never reset the sink's fence tables to "make the guard pass".

---

## Key SQL queries

All read-only. Run them in the read-only session (see the top of this runbook). They use clock_timestamp(), the same DB clock the scheduler uses, never now() (the transaction start time).

### Stuck RUNNING runs

`RUNNING` with an expired lease (the sweep should reap these within about 1 s), or running far longer than `max_run_time` (the hard stop should have ended it). `started_at` is the first start, across retries.

```sql
SELECT id, tenant_id, job_id, owner_node, slot_no, attempt, lease_lost_count,
       fencing_epoch || '.' || fencing_seq AS token,
       lease_until, clock_timestamp() - lease_until AS expired_for,
       clock_timestamp() - started_at AS since_first_start, max_run_time
  FROM job_run
 WHERE state = 'RUNNING'
   AND (lease_until < clock_timestamp()
        OR started_at < clock_timestamp() - 2 * max_run_time)
 ORDER BY lease_until
 LIMIT 50;
```

### Slot occupancy per tenant

Rule: `running` ≤ `slot_rows` always. `slot_rows` > `configured_cap` means a reduction is draining.

```sql
SELECT t.id AS tenant, t.paused, t.configured_cap,
       (SELECT count(*) FROM tenant_slot s WHERE s.tenant_id = t.id) AS slot_rows,
       (SELECT count(*) FROM job_run r
         WHERE r.tenant_id = t.id AND r.state = 'RUNNING') AS running,
       (SELECT count(*) FROM job_run r
         WHERE r.tenant_id = t.id AND r.state = 'RUNNING' AND r.lease_until >= clock_timestamp()) AS running_valid_lease,
       (SELECT count(*) FROM job_run r
         WHERE r.tenant_id = t.id AND r.state IN ('SCHEDULED', 'RETRY_WAIT')
           AND r.due_at <= clock_timestamp()) AS due_waiting
  FROM tenant t
 ORDER BY due_waiting DESC
 LIMIT 50;
```

### Partition owners and live nodes

Per owner: want 128 valid in total, spread roughly evenly over live nodes.

```sql
SELECT coalesce(owner_node, '(none)') AS owner_node,
       count(*) FILTER (WHERE lease_until >= clock_timestamp()) AS valid,
       count(*) FILTER (WHERE lease_until IS NULL OR lease_until < clock_timestamp()) AS expired,
       max(epoch) AS max_epoch
  FROM partition_lease
 GROUP BY 1
 ORDER BY 1;
```

Partitions without a valid lease:

```sql
SELECT partition_id, owner_node, epoch, lease_until,
       clock_timestamp() - lease_until AS expired_for
  FROM partition_lease
 WHERE owner_node IS NULL OR lease_until IS NULL OR lease_until < clock_timestamp()
 ORDER BY partition_id;
```

Nodes (live = heartbeat under 6 s old, DB time):

```sql
SELECT node_id, started_at, heartbeat_at,
       clock_timestamp() - heartbeat_at AS heartbeat_age,
       heartbeat_at >= clock_timestamp() - interval '6 seconds' AS live
  FROM scheduler_node
 ORDER BY node_id;
```

### Dead letters by error code

Last 24 h, not replayed. A replayed run that dies again gets its row reset (`replayed_at` back to NULL), so it shows up here again.

```sql
SELECT error_code, tenant_id, count(*) AS runs,
       min(dead_at) AS oldest, max(dead_at) AS newest, max(attempts) AS max_attempts
  FROM dead_letter
 WHERE dead_at >= clock_timestamp() - interval '24 hours'
   AND replayed_at IS NULL
 GROUP BY error_code, tenant_id
 ORDER BY runs DESC;
```

### Oldest due-not-started runs

What `scheduler_queue_depth` counts, oldest first, with the reasons a run may wait.

```sql
SELECT r.id, r.tenant_id, r.partition_id, r.state, r.attempt, r.priority, r.due_at,
       clock_timestamp() - r.due_at AS waiting,
       t.paused, p.owner_node,
       p.lease_until >= clock_timestamp() AS partition_owned
  FROM job_run r
  JOIN tenant t ON t.id = r.tenant_id
  JOIN partition_lease p ON p.partition_id = r.partition_id
 WHERE r.state IN ('SCHEDULED', 'RETRY_WAIT')
   AND r.due_at <= clock_timestamp()
 ORDER BY r.due_at
 LIMIT 20;
```

Grouped, to see whether one tenant dominates:

```sql
SELECT tenant_id, count(*) AS due_waiting, min(due_at) AS oldest_due,
       clock_timestamp() - min(due_at) AS max_wait
  FROM job_run
 WHERE state IN ('SCHEDULED', 'RETRY_WAIT') AND due_at <= clock_timestamp()
 GROUP BY tenant_id
 ORDER BY due_waiting DESC
 LIMIT 20;
```

---

## Replaying dead letters

**Fix the root cause first.** A replay runs the same job with the same payload.

| Error code | Before replaying |
|---|---|
| `INVALID_PAYLOAD`, `INVALID_EFFECT_PAYLOAD`, `INVALID_CHECKPOINT` | The tenant ADMIN fixes the job. Replaying unchanged data dead-letters again on the first attempt. |
| `NO_HANDLER`, `JOB_NOT_FOUND` | Code or data bug. Escalate; do not replay. |
| `SINK_REJECTED_<status>` | Find out why the sink rejected it. |
| `SINK_5XX`, `SINK_TIMEOUT`, `SINK_UNAVAILABLE`, `TIMEOUT`, `IO_ERROR` | Sink healthy again (`SchedulerErrorRateHigh` resolved). |
| `FENCING_CONFLICT` | Lease losses stopped (`SchedulerLeaseLostSpike` resolved). |

**How replay works.** The same `job_run` row goes back to `SCHEDULED`, due now, with `attempt` and `lease_lost_count` reset. Its `idem_key` and checkpoint are kept, so the sink deduplicates anything already delivered and chunked jobs resume from their last checkpoint. `dead_letter.replayed_at` is set. A disabled job cannot be replayed.

### List

`GET /api/v1/dead-letters` (OPERATOR, or ADMIN for their own tenant). Query: `tenant` (optional; OPERATOR sees all tenants without it), `page` (from 0), `size` (1-200, default 50). Newest first. The response is `{"items":[...], "page", "size", "hasMore"}`; each item has `runId`, `jobId`, `tenantId`, `errorCode`, `attempts`, `deadAt`, `replayedAt`, `runState`. No payloads.

```bash
curl -s -H "$AUTH" 'localhost:8081/api/v1/dead-letters?tenant=acme&size=50' | jq .
```

### Replay one

`POST /api/v1/dead-letters/{runId}/replay` (OPERATOR only). No body. Optional `Idempotency-Key` header (1-128 printable ASCII characters, no spaces): a retry with the same key returns the first result and does not replay twice.

```bash
curl -s -X POST -H "$AUTH" -H 'Idempotency-Key: replay-12345-1' \
  localhost:8081/api/v1/dead-letters/12345/replay | jq .
```

`200` returns `{"runId":12345,"jobId":"...","state":"SCHEDULED","dueAt":"...","idemKey":"...","replayedAt":"..."}`. Errors: `404 DEAD_LETTER_NOT_FOUND`, `409 DEAD_LETTER_ALREADY_REPLAYED` (the run is no longer dead-lettered), `409 JOB_DISABLED`, `409 IDEMPOTENCY_KEY_REUSED`.

API idempotency keys are kept for 24 h. If the same run dies again and needs a second replay within 24 h, use a new key, or you get the cached first answer.

### Bulk replay

Collect the run ids first (replays do not remove rows from the list, so paging while replaying is unreliable), check the count, then replay slowly and log each status.

```bash
TENANT=acme
CODE=SINK_5XX
BATCH=$(date +%Y%m%d%H%M%S)
IDS=/tmp/replay-$TENANT-$BATCH.txt
: > "$IDS"
page=0
while :; do
  body=$(curl -s -H "$AUTH" "localhost:8081/api/v1/dead-letters?tenant=$TENANT&page=$page&size=200")
  echo "$body" | jq -r --arg c "$CODE" \
    '.items[] | select(.errorCode == $c and .runState == "DEAD_LETTERED") | .runId' >> "$IDS"
  [ "$(echo "$body" | jq -r .hasMore)" = "true" ] || break
  page=$((page + 1))
done
wc -l < "$IDS"
```

Look at the count. If it is what you expect, replay:

```bash
while read -r id; do
  status=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "$AUTH" \
    -H "Idempotency-Key: replay-$BATCH-$id" \
    "localhost:8081/api/v1/dead-letters/$id/replay")
  echo "$id $status"
  [ "$status" = "200" ] || [ "$status" = "409" ] || { echo "stopping at $id"; break; }
  sleep 0.2
done < "$IDS" | tee "$IDS.log"
```

The loop stops on anything other than 200 or 409. Rerunning it with the same `BATCH` is safe: done runs return the cached 200 or 409. Replayed runs drain at the tenant's cap; watch `SchedulerDeadLettersIncreasing` for runs that dead-letter again.

---

## Useful PromQL

Fire lag p99 and p50:

```promql
histogram_quantile(0.99, sum by (le) (rate(scheduler_fire_lag_seconds_bucket[5m])))
```

```promql
histogram_quantile(0.5, sum by (le) (rate(scheduler_fire_lag_seconds_bucket[5m])))
```

Runs by state, and the backlog (cluster-wide DB gauges, so `max`, never `sum`):

```promql
max by (state) (scheduler_runs)
```

```promql
max(scheduler_queue_depth)
```

Throughput and error rate:

```promql
sum by (outcome) (rate(scheduler_run_outcome_total[5m]))
```

Tenants held back by their cap:

```promql
topk(10, sum by (tenant) (rate(scheduler_tenant_cap_rejections_total[5m])))
```

Partitions per node:

```promql
sum by (node) (scheduler_partitions_owned)
```

Fencing activity:

```promql
sum by (node, reason) (rate(scheduler_lease_lost_total[5m]))
```

```promql
sum by (node) (rate(scheduler_stale_commit_rejected_total[5m]))
```

Sink side: delivered vs deduplicated vs stale vs faults:

```promql
sum by (outcome) (rate(sink_deliveries_total[5m]))
```

API 5xx per node:

```promql
sum by (node, uri) (rate(http_server_requests_seconds_count{status=~"5.."}[5m]))
```

Node restarts (uptime resets):

```promql
min by (node) (process_uptime_seconds)
```

---

## Logs

Nodes log JSON lines (Spring profile `json`, set in `docker-compose.yml`). Every line carries the MDC fields that are set:

| Field | Set by | Meaning |
|---|---|---|
| `nodeId` | engine loops and workers | `scheduler-1..3` |
| `runId`, `jobId`, `tenantId`, `fencingToken`, `attempt` | the worker running a run | `fencingToken` is `<epoch>.<seq>` |
| `traceId`, `tenantId` | every API request | `traceId` is also in every API error response |

MDC values are strings, so match them quoted. One run across all nodes:

```bash
docker compose logs --no-log-prefix scheduler-1 | grep '"runId":"12345"'
docker compose logs --no-log-prefix scheduler-1 scheduler-2 scheduler-3 | grep '"runId":"12345"'
```

Readable, with `jq`:

```bash
docker compose logs --no-log-prefix scheduler-1 scheduler-2 scheduler-3 \
  | grep '"runId":"12345"' \
  | jq -r '[.["@timestamp"], .level, .nodeId, .fencingToken, .attempt, .message] | @tsv'
```

An API error a client reported (take the `traceId` from the error body):

```bash
docker compose logs --no-log-prefix scheduler-1 scheduler-2 scheduler-3 | grep '"traceId":"4f1c2a9e7b3d"'
```

Everything for one tenant in the last 15 minutes:

```bash
docker compose logs --since 15m --no-log-prefix scheduler-1 scheduler-2 scheduler-3 | grep '"tenantId":"acme"'
```

**No personal data in logs.** Job payloads are never logged, and a masking encoder replaces anything that looks like a phone number, email or coordinates in every string value. Do not paste payloads from the database into tickets or chat either: use run ids, job ids and error codes.
