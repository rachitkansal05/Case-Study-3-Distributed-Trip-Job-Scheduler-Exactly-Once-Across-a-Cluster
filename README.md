# Distributed Trip Job Scheduler

A cluster scheduler for MoveInSync trip jobs: nightly rosters at 22:00 tenant time, "your cab is assigned" alerts 30 minutes before pickup, no-show auto-cancels and daily billing close. It runs on three or more identical nodes. A node can be killed or frozen at any time without a job being lost or its effect happening twice.

> MoveInSync Backend Case Study 2026 · Case Study 3 · Advanced Track
>
> Full design plan: https://claude.ai/artifact/JuMnTBHToDBjXiXkPYSVz3

**Status:** design complete, implementation in progress. Sections marked _TBD_ are filled in as milestones land.

---

## Contents

- [Quick start](#quick-start)
- [What it guarantees](#what-it-guarantees)
- [How it works](#how-it-works)
- [Time zones and daylight saving time](#time-zones-and-daylight-saving-time)
- [Misfires, retries and the dead-letter list](#misfires-retries-and-the-dead-letter-list)
- [Per-tenant fairness](#per-tenant-fairness)
- [API](#api)
- [Security](#security)
- [Monitoring](#monitoring)
- [Caching](#caching)
- [Project layout](#project-layout)
- [Testing](#testing)
- [Benchmark](#benchmark)
- [Failure modes](#failure-modes)
- [Trade-offs](#trade-offs)
- [Assumptions](#assumptions)
- [Design review fixes](#design-review-fixes)

---

## Quick start

Requirements: Docker Desktop (about 6 GB RAM for the benchmark), Java 21 and Maven only if you build outside Docker.

```bash
docker compose up --build
```

This starts:

| Service | Port | Purpose |
|---|---|---|
| `postgres` | 5432 | Job store and the cluster's only clock |
| `scheduler-1`, `scheduler-2`, `scheduler-3` | 8081–8083 | Identical scheduler nodes |
| `notification-sink` | 8090 | Mock SMS/push provider that enforces idempotency keys and fencing tokens |
| `toxiproxy` | 8474 | Network fault injection for tests |
| `prometheus` | 9090 | Metrics |
| `grafana` | 3000 | Pre-built scheduler dashboard (admin / admin) |

Get a demo token and create a job:

```bash
TOKEN=$(curl -s -X POST localhost:8081/dev/token \
  -H 'Content-Type: application/json' \
  -d '{"tenant":"acme","role":"ADMIN"}' | jq -r .token)

curl -X POST localhost:8081/api/v1/jobs \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{
        "kind": "RECURRING",
        "handler": "ROSTER_GEN",
        "cron": "0 22 * * *",
        "payload": {"site": "BLR-01"},
        "priority": 5,
        "maxRunTime": "PT5M",
        "misfire": "COALESCE"
      }'
```

A Postman collection is in [`docs/postman/`](docs/postman/).

The demo runs a single PostgreSQL and does not claim to survive a database failover. The production failover setup is in [`deploy/patroni/`](deploy/patroni/) (see [Failover requirements](#failover-requirements-production)).

---

## What it guarantees

**Exactly-once execution is impossible.** A node can send an alert and then die before it records the send. To the rest of the cluster that looks the same as a node that died before sending. A node in a 30-second GC pause also looks the same as a dead one. A scheduler has two choices: retry and maybe repeat the work, or never retry and maybe lose it. Losing jobs is not allowed, so this scheduler retries.

**What we guarantee instead:**

- Every scheduled occurrence is **executed at least once**.
- Its **side effect happens exactly once**. Every run has a deterministic idempotency key (`{jobId}:{scheduledAtUtc}`), and every item inside a run has its own key (`{runKey}:{employeeId}`). The receiving side stores keys under a unique constraint.
- A node whose lease has expired **cannot write anything**: no renewal, no checkpoint, no commit, even if nobody has taken the run over yet. Every state change carries a fencing token `(epoch, seq)`. The database and the notification sink both reject writes with an outdated token.
- **Across a PostgreSQL failover**, no acknowledged write is lost and fencing tokens never go backwards, as long as the [failover requirements](#failover-requirements-production) hold. If they can't be met, writes stop rather than continue unprotected.

This relies on downstream systems honouring idempotency keys, or on a dedupe table we control in front of them. No scheduler can make a non-idempotent third-party API exactly-once.

**Which clock decides when a job is due:** the PostgreSQL primary's clock. Nodes never compare their own wall clock with due times or lease expiry; those checks are SQL predicates using `clock_timestamp()`, the current time. (`now()` is the time the transaction started, so it can call an expired lease valid.) Each node uses its monotonic clock only to stop itself early when it has not renewed a lease in time.

---

## How it works

```
         Admin / Operator (REST + JWT)          Prometheus / Grafana
                     │                                 ┆ scrape
     ┌───────────────┼─────────────────────────────────┼──────┐
     │  Node 1         Node 2         Node 3   (identical, no leader)
     │  membership · partition leases · poller · dispatcher · reaper
     └───────┬───────────────┬───────────────┬────────────────────┘
             │ claim SKIP LOCKED, fenced commits      │ effect + idempotency key + token
             ▼                                        ▼
     PostgreSQL 16  (job, job_run, partition_lease,   Notification sink
     scheduler_node, tenant, tenant_slot,             (UNIQUE key, max token per run)
     dead_letter; clock)
```

Each node runs five loops:

| Loop | Interval | Job |
|---|---|---|
| Membership | 2 s | Heartbeat into `scheduler_node`; a node is live if its heartbeat is under 6 s old (DB time) |
| Partitions | 1 s | Compute owned partitions by rendezvous hashing over live nodes; acquire, renew (6 s lease, renewed every 2 s) or release |
| Poller | 200 ms | Claim due runs from owned partitions, per tenant, one tenant slot per run |
| Dispatcher | continuous | Run claimed work, taking from tenant queues in turn (deficit round robin); recovered runs first |
| Orphan sweep | 1 s | Take over expired `RUNNING` runs in **any** partition and restart them in the same slot |
| Slot compactor | on release + 1 s | Finish lowered caps (see [Changing a cap](#changing-a-cap)) |

### No single hot lock

Jobs are split into **128 fixed partitions** (`murmur3(jobId) % 128`), each with its own lease row. Nodes contend per partition, not on one global lock row. When a node joins or leaves, only about `128 / N` partitions move.

Partition ownership only spreads polling load; correctness does not depend on it. If two nodes poll the same partition during a handover, `FOR UPDATE SKIP LOCKED` and the run-level fencing token still allow only one claim per run.

When a partition's lease expires, its previous owner is treated as dead for that partition at once, so another node picks it up within about 7 s, before the 10 s run lease expires. Running jobs don't wait for that at all: the **orphan sweep** takes over expired runs in any partition. Recovered runs go to the front of the dispatch queue and each node keeps one worker free for them.

**Takeover bound: ≤ 11.2 s** after a crash (10 s run lease after its last renewal + up to 1 s sweep + about 0.2 s claim), given at least one live node.

### Claim, fence, commit

All writes to run state and slots go through one class, `ClaimProtocol`. Polling, the orphan sweep, manual triggers and replays all use it.

1. **Claim**, in one transaction, always locking the slot row first and the run row second:
   - Lock a free-looking slot of the tenant (`FOR UPDATE SKIP LOCKED`).
   - If the slot still has a holder whose lease looks expired, **reap it with a conditional update**: `UPDATE job_run SET state = 'RETRY_WAIT', slot_no = NULL WHERE id = ? AND state = 'RUNNING' AND lease_until < clock_timestamp()`. One row updated means the slot is free. Zero rows means the holder renewed in time, so the slot is skipped. Because the reap and a renewal update the same row, Postgres runs them one at a time and exactly one succeeds.
   - Start the due run: `state = RUNNING`, `slot_no`, token `(epoch, nextval('fence_seq'))`, `lease_until = clock_timestamp() + 10 s`.
   - The token is used only after this transaction commits.
2. **Execute.** The handler gets the run's idempotency key, fencing token and last checkpoint. Every outbound call sends both. A renewer extends the lease every 3 s with `WHERE id = ? AND (fencing_epoch, fencing_seq) = (?, ?) AND state = 'RUNNING' AND lease_until >= clock_timestamp()`, so **an expired lease can never be renewed**.
3. **Checkpoint.** Chunked handlers such as the 1,200 pickup alerts send in chunks of 50 and save progress after each chunk, using the same fenced predicate.
4. **Commit:** the same predicate, setting `state = 'SUCCEEDED'` and `slot_no = NULL` in one statement. Zero rows updated means the lease was lost; the commit is rejected and counted. Commits never lock slot rows, so claims and commits can't deadlock.

### Fencing tokens

- A token is a pair `(fencing_epoch, fencing_seq)`, compared epoch first. There's no bit-packing of the sequence, so there's no range limit.
- `fencing_epoch = cluster_generation × 2^32 + timeline`. PostgreSQL's timeline ID (unsigned 32-bit) rises on every promotion, so tokens from a new primary beat every token from the old one, and clients still talking to the old primary get rejected. If the timeline can't be read, the claim fails and nothing starts.
- `cluster_generation` is raised by operators when rebuilding from a fresh `initdb`, which restarts at timeline 1. Nodes refuse to start if the current epoch is lower than the highest epoch the sink has seen.
- A `409` for an outdated token means **lease lost**: the handler stops and the attempt doesn't count toward `maxAttempts`. After 20 lease losses the run is dead-lettered as `FENCING_CONFLICT` and an alert fires.

Run states:

```
SCHEDULED → RUNNING → SUCCEEDED
              │  ├──► RETRY_WAIT → RUNNING   (failure or expired lease, new token)
              │  └──► DEAD_LETTERED → SCHEDULED (operator replay, same key)
SCHEDULED → SKIPPED  (misfire SKIP / COALESCE)
SCHEDULED ↔ PAUSED   (tenant pause / resume)
```

### Freeze test, step by step

1. Node A claims run 77 in slot 4 with token `7.41`, sends alerts 1–600 and saves checkpoint 601.
2. Node A is frozen (`docker pause`) for 30 s. Its lease expires after 10 s.
3. Node B's orphan sweep locks slot 4, reaps run 77 and restarts it in the same transaction, in the same slot, with token `7.42`. It sends alerts 601–1,200 from the checkpoint and commits.
4. Node A wakes up. Its monotonic deadline has passed, so it stops. Any send still in flight with token `7.41` gets a `409` from the sink, and any renewal or commit with token `7.41` updates zero rows.
5. Result: 1,200 distinct deliveries, zero duplicates, and the tenant's running count never went above its cap.

---

## Time zones and daylight saving time

Cron expressions match **local wall time in the tenant's zone**. The resolver then maps each local time to exactly one instant.

| Case | Example (America/New_York) | Rule | Fires at |
|---|---|---|---|
| Clocks jump 02:00 → 03:00 | 8 Mar 2026, job at 02:30 | Fires once at **03:00 local**, when the skipped hour ends, as classic cron does. Java's default would be 03:30. | 07:00Z |
| 01:00–01:59 happens twice | 1 Nov 2026, job at 01:30 | Fires **once, at the first 01:30** (EDT, −04:00) | 05:30Z |
| Sub-hourly job in the repeated hour | `*/15 * * * *` | Each distinct local time fires once: 4 runs in the repeated hour, not 8 | — |
| Zone without DST | Asia/Kolkata, 22:00 | Direct mapping | 16:30Z |

Runs are created up to 15 minutes ahead. A unique `(job_id, scheduled_at)` constraint makes creating them safe to repeat.

---

## Misfires, retries and the dead-letter list

A run has **misfired** when it is claimed more than `misfire_threshold` (default 60 s) after its scheduled time, for example after cluster downtime. Each job picks a policy:

| Policy | After 3 missed hourly runs | Typical use |
|---|---|---|
| `FIRE_NOW` | All 3 run now, in order | Billing close: every period must close |
| `SKIP` | All 3 skipped; the next run is normal | Pickup alerts: a late alert is useless |
| `COALESCE` | The newest runs once with `coalescedCount = 3` | Roster generation: only the latest matters |

**Retries** use exponential backoff with full jitter: `delay = random(0, min(5 min, 2 s × 2^attempt))`. Timeouts and 5xx responses are retried. Bad payloads and 4xx responses go to the dead-letter list immediately. After `maxAttempts` (default 5) the run is dead-lettered. An operator can inspect it (error code only, never payload data) and replay it. A replay keeps the **same idempotency key**, so work that was already delivered is not sent again.

---

## Per-tenant fairness

Each tenant has a cluster-wide concurrency cap (default 32), enforced by **slots**: the tenant has exactly `cap` rows in `tenant_slot`, and a run can only be `RUNNING` while holding one. The dispatcher takes from tenant queues in turn, so one tenant's backlog never blocks another tenant.

Scenario: 3 nodes × 64 workers = 192 slots, each alert takes about 40 ms. Both tenants' alerts are due at 08:00:00.

| | Without caps (one FIFO queue) | With caps (32 per tenant) |
|---|---|---|
| Tenant A, 50,000 alerts | drains in ≈ 10.4 s | drains in ≈ 62.5 s (800/s) |
| Tenant B, 20 alerts | waits up to 10.4 s ❌ | starts within one poll, < 0.3 s ✅ |
| Free slots for other tenants | 0 | 160 |

A hard cap leaves capacity idle during a single-tenant burst; we chose isolation over peak throughput. Caps are configurable per tenant.

### Why the cap always holds

The database enforces it, with no tolerance during partition moves or crashes:

- A running run must hold a slot: `CHECK (state <> 'RUNNING' OR slot_no IS NOT NULL)`.
- The slot must be a real slot row of the same tenant: composite foreign key `(tenant_id, slot_no) → tenant_slot`, `ON DELETE RESTRICT`.
- No slot is used twice: `UNIQUE (tenant_id, slot_no) WHERE state = 'RUNNING'`.
- `job_run.slot_no` is the only record of who holds a slot, and it's cleared whenever a run leaves `RUNNING`.

So running runs per tenant ≤ its slot rows. A slot passes to a new run only after the old run is reaped by a conditional update (see [Claim, fence, commit](#claim-fence-commit)). Precisely: at most `cap` runs per tenant hold a valid lease at any moment. A frozen process may run for a moment when it wakes; fencing rejects anything it does.

### Changing a cap

Each tenant has a `configured_cap` (what the admin set) and an **effective cap**, its number of slot rows. The rule "running ≤ effective cap" has no exceptions.

- **Raising** a cap adds slot rows at once; they're usable on the next poll.
- **Lowering** a cap is deferred. Example: 28 running, cap lowered from 32 to 16:
  1. Free slots numbered 16 or higher are deleted at once. The database refuses to delete a slot in use.
  2. Whenever a slot below 16 becomes free, the compactor moves one running run from a high slot into it, then deletes the empty high slot.
  3. Compaction fills every free low slot first, so **no new run starts until fewer than 16 are running**.

  The effective cap only goes down and reaches 16 within the longest remaining `maxRunTime` of the runs already active. Running jobs aren't stopped; `force=true` revokes their leases instead, which fencing makes safe. The tenant API shows `draining: true` until then.

---

## API

All endpoints are under `/api/v1` and need a bearer token.

| Method and path | Purpose | Roles |
|---|---|---|
| `POST /jobs` | Create a one-off or recurring job | ADMIN |
| `GET /jobs/{id}` | Job definition (payload hidden from EMPLOYEE) | ADMIN, EMPLOYEE |
| `GET /jobs/{id}/next-fire-times?count=10` | Next fire times with local time and offset | ADMIN, EMPLOYEE |
| `POST /jobs/{id}/trigger` | Run now; requires an `Idempotency-Key` header | ADMIN, OPERATOR |
| `DELETE /jobs/{id}` | Disable a job and cancel its unstarted runs | ADMIN |
| `GET /jobs/{id}/runs` | Run history: state, attempt, lag, error code | ADMIN, OPERATOR |
| `POST /tenants/{t}/pause` · `/resume` | Pause or resume all of a tenant's jobs | ADMIN (own tenant), OPERATOR |
| `PUT /tenants/{t}/cap` | Change the concurrency cap; returns configured cap, effective cap and `draining` | ADMIN (own tenant), OPERATOR |
| `GET /dead-letters?tenant=` | List dead-lettered runs | OPERATOR, ADMIN (own tenant) |
| `POST /dead-letters/{runId}/replay` | Replay with the same idempotency key | OPERATOR |
| `GET /cluster` | Live nodes and partitions per node | OPERATOR |

`POST /dev/token` mints demo tokens and exists only in the `dev` profile.

All write endpoints (`POST /jobs`, pause, resume, cap, trigger, replay) accept an `Idempotency-Key` header, kept for 24 hours. A client that retries after an unclear failure, for example a commit lost in a database failover, gets the original result and no second action. Reusing a key with a different request body returns `409`.

Errors use RFC 7807 problem details with a stable `code`, a safe message and a `traceId`:

```json
{
  "type": "about:blank",
  "title": "Job not found",
  "status": 404,
  "code": "JOB_NOT_FOUND",
  "traceId": "4f1c2a9e7b3d"
}
```

Responses never contain stack traces, SQL or payload values. Validation errors name the field, never its value.

---

## Security

- **Roles.** EMPLOYEE reads schedules for their own tenant. ADMIN manages one tenant's jobs. OPERATOR is the platform on-call role: it works across tenants, but only on operational data (states, counts, error codes), never payloads.
- **Tenant isolation.** The tenant comes only from the token, never from the request body. Every repository query is scoped by tenant. A request for another tenant's job returns `404`, not `403`, so it does not reveal that the job exists.
- **No personal data in logs.** Payloads, which can contain phone numbers, emails and home locations, are never logged. A masking log converter also catches phone, email and coordinate patterns.

---

## Monitoring

Metrics are at `/actuator/prometheus` on each node, and the Grafana dashboard is provisioned automatically.

| Metric | What it tells on-call | Alert when |
|---|---|---|
| `scheduler_fire_lag_seconds` (histogram) | Actual start minus scheduled time | p99 > 2 s for 5 min |
| `scheduler_runs{state}` | Running, retrying and dead-lettered counts | Any new dead letters |
| `scheduler_queue_depth` | Runs due but not started | Rising for 2 min |
| `scheduler_run_outcome_total{outcome}` | Success, retryable, permanent, timeout | Error rate > 5 % |
| `scheduler_stale_commit_rejected_total` | Fencing rejections (GC pauses, network splits) | Any increase outside tests |
| `scheduler_tenant_cap_rejections_total` | Claims held back by tenant caps | Informational |
| `scheduler_partitions_owned{node}` | Partition spread | Total ≠ 128 for 10 s |
| `scheduler_tenant_cap{tenant,kind}` | Configured vs effective cap | Effective > configured for longer than the tenant's longest max run time |
| `scheduler_lease_lost_total{reason}` | Stale-token 409s, fenced writes that matched nothing, self-fences | Any `FENCING_CONFLICT` dead letter |
| `scheduler_orphan_takeovers_total` | Runs taken over after a crash or freeze, with takeover time | Takeover p99 > 11.2 s |
| `scheduler_lease_renew_failures_total` | Nodes losing leases | Rate > 0 |
| `scheduler_claim_batch_seconds` | Database claim latency | p99 > 100 ms |

Logs are JSON lines carrying `nodeId`, `runId`, `jobId`, `tenantId`, `fencingToken` and `attempt`.

---

## Caching

| Cache | Expiry | How stale it can be |
|---|---|---|
| Job definitions | Keyed by job version, max 50,000 entries | Never stale: a version never changes, and an edit creates a new one |
| Tenant settings (time zone, display settings) | 5 s | Up to 5 s. **Caps and pause are never cached**: the cap is the slot rows themselves and the claim reads pause directly, so both take effect within one poll (200 ms) |
| Next fire times | 30 s | Up to 30 s after a tenant changes time zone |
| Live node list | Refreshed every 2 s | Partition moves can lag one refresh; correctness is unaffected |

---

## Project layout

```
trip-job-scheduler/
├─ docker-compose.yml
├─ scheduler-core/      Plain Java, no Spring, no database: cron and DST, misfire and backoff
│                       policies, partition assignment, fairness dispatcher, lease clock
├─ scheduler-app/       Spring Boot: controllers, services, JDBC repositories, engine loops,
│                       job handlers, security, metrics, error handling
├─ notification-sink/   Mock provider that enforces idempotency keys and fencing tokens
├─ loadgen/             100,000-job load generator and lag reporter
├─ chaos/               kill-during-alerts.sh, freeze-during-alerts.sh
├─ deploy/patroni/      Production failover config (F1–F5)
└─ docs/                DESIGN.md, BENCHMARK.md, diagrams, Postman collection
```

The core algorithms sit behind interfaces (`RunStore`, `PartitionLeaseStore`, `Clock`), so they are tested with in-memory fakes and run in production against PostgreSQL.

---

## Testing

```bash
mvn verify                       # unit + integration tests (Testcontainers starts PostgreSQL)
./chaos/kill-during-alerts.sh    # kill -9 a node while it sends 1,200 alerts
./chaos/freeze-during-alerts.sh  # freeze a node for 30 s, check the stale write is rejected
```

Every hard rule from the case study has an automated test:

| Rule | Test | Kind |
|---|---|---|
| One-off and recurring jobs, max run time | `JobDefinitionTest` | Unit |
| Time zones and DST | `WallClockResolverTest`, `NextFireTimesTest` | Unit |
| Exactly-once effect | `ExactlyOnceEffectIT`: 3 nodes, 10,000 jobs, random failures, one delivery per key | Concurrency |
| Leases and fencing | `FencingIT`, `ClaimRaceIT` (32 threads racing for the same runs) | Integration, concurrency |
| Renewal can't race slot reuse | `SlotStealRaceIT`: two connections forced into the exact interleaving, both orders, exactly one wins; `LeaseResurrectionIT`: an expired lease can't be renewed | Concurrency |
| Tokens survive failover | `FencingTokenOrderTest`, `StaleTokenClassificationTest`, `FailoverFencingIT` (promote the synchronous standby; nothing acknowledged lost, new tokens accepted, old-primary clients rejected), `StrictSyncIT`, `ClusterGenerationGuardIT` | Unit, failure injection |
| Partitions move on join and leave | `PartitionAssignerTest`, `RebalanceIT` (kill right after a partition-lease renewal; all partitions owned within 7 s) | Unit, integration |
| Misfire policies | `MisfirePolicyTest` | Unit |
| Per-tenant fairness | `TenantFairnessIT`: 50,000 + 20 alerts at once | Concurrency |
| Cap is never exceeded | `TenantCapIT` (cap 5, random kills, freezes and partition moves, sampled every 10 ms, no tolerance), `SlotConstraintTest`, `CapReductionIT`, `CapIncreaseIT`, `LockOrderIT` (no deadlocks) | Concurrency, integration |
| Retries and dead-letter list | `BackoffPolicyTest`, `RetryToDeadLetterIT` | Unit, integration |
| Pause, resume, next fire times, manual trigger, retry-safe writes | `OpsApiIT`, `ApiIdempotencyIT` | API |
| Metrics | `MetricsIT` | Integration |
| kill -9 during 1,200 alerts | `NodeCrashIT` (worst-case timing, takeover ≤ 11.2 s) + `chaos/kill-during-alerts.sh` | Failure injection |
| 30 s freeze, stale write rejected | `GcPauseIT` + `chaos/freeze-during-alerts.sh` | Failure injection |
| Tenant isolation, roles, no PII leaks | `TenantIsolationIT`, `RoleMatrixIT`, `PiiMaskingTest`, `ErrorResponseTest` | API, unit |

---

## Benchmark

```bash
docker compose up -d
./loadgen/run.sh spread     # 100,000 jobs, 50 tenants, due across 10 minutes
./loadgen/run.sh burst      # tenant A 50,000 + tenant B 20, same instant
./loadgen/run.sh chaos      # spread, with a kill at minute 4 and a freeze at minute 7
```

**Target:** 100,000 jobs due in one 10-minute window on 3 nodes, p99 fire lag under 2 s.

### Results (_TBD_: filled in after the benchmark runs)

| Scenario | Throughput | p50 lag | p99 lag | Max lag | Duplicates |
|---|---|---|---|---|---|
| Spread (100k / 10 min) | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| Burst, tenant B | — | _TBD_ | _TBD_ | _TBD_ | _TBD_ |
| Spread + chaos | _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |

**Machine:** _TBD_ (CPU, cores, RAM, disk, OS, Docker resources, JVM flags, PostgreSQL settings).

### Expected capacity

| Quantity | Estimate | Reasoning |
|---|---|---|
| Average arrival rate | 167 runs/s | 100,000 / 600 s |
| Cluster capacity | ≈ 4,800 runs/s | 192 workers / 40 ms per run, about 29× headroom |
| Database writes | ≈ 1,100 rows/s | 5–6 writes per run (slot lock at claim; slot freed in the commit) plus about 64 partition renewals/s |
| Typical lag | ≈ 100 ms + queueing | Half the 200 ms poll interval plus a 10–20 ms claim |
| Takeover after kill -9 | ≤ 11.2 s | 10 s lease + 1 s orphan sweep + 0.2 s claim, whatever the partition timing |
| Synchronous replication (production) | + ≈ 1 ms per commit | One LAN round trip to the synchronous standby |

### Complexity

- **Claim:** O(T · log R + b) per poll: T tenants with free slots, R due runs in owned partitions (partial index), b claimed rows. Node memory is O(workers + batch), not O(jobs).
- **Partition assignment:** O(P · N) hashes per membership change (128 × 3 = 384).
- **Run creation:** O(occurrences in the next 15 minutes).

### Where it stops scaling

The single PostgreSQL primary is the limit. Above roughly 5,000–10,000 run state changes per second, claim latency rises and lag follows. 128 partitions also caps useful nodes at about 40. The next steps would be sharding PostgreSQL by partition range, then moving dispatch to a partitioned log such as Kafka. The benchmark report will state the measured rate at which p99 passes 2 s.

---

## Failure modes

| What fails | What users see | How it recovers |
|---|---|---|
| Node killed mid-run | Alerts pause ≤ 11.2 s, then continue; nobody gets two | Lease expires; the orphan sweep on any node restarts the run in the same slot with a higher token from the checkpoint; the sink drops repeats |
| Node frozen (GC pause) | Same as a kill | Node stops itself on wake-up; sink and database reject its old token, and its expired lease can't be renewed |
| Node lost, partitions orphaned | New due runs in those partitions wait up to about 7 s | Partition leases (6 s) expire, the expired owner is treated as dead at once, and live nodes take them over |
| Renewal and slot reuse at the same moment | Nothing | Both update the same run row; exactly one succeeds |
| Node joins | Nothing | About 128/N partitions are released and picked up |
| PostgreSQL unreachable | No jobs fire; API returns `503 SCHEDULER_STORE_UNAVAILABLE` | Nodes stop starting work; misfire policies handle the backlog once the database returns |
| PostgreSQL primary fails (production) | Writes pause about 10–30 s; nothing acknowledged is lost | Patroni promotes only the synchronous standby; the new timeline raises the token epoch |
| No synchronous standby available | Writes block; no jobs fire | Strict synchronous mode; resumes when a standby returns |
| Commit outcome unknown after failover | API client gets an error | Client retries with the same `Idempotency-Key` and gets one result |
| Cluster rebuilt from a fresh `initdb` | Nodes refuse to start | Operator raises `cluster_generation` |
| Admin lowers a cap below the running count | New starts for that tenant wait | Deferred reduction with compaction; running stays ≤ effective cap |
| Notification sink slow | Alerts delayed, retry count rises | Timeouts retry with backoff, then dead-letter; caps contain the impact to one tenant |
| Late or out-of-order messages | Nothing | Every write is conditional on token and state |
| Double-clicked manual trigger | One run | `Idempotency-Key` header maps to a unique run key |
| Clock skew on a node | Nothing | Due times and leases use the database clock |
| Bad payload | Run appears in the dead-letter list with `INVALID_PAYLOAD` | Dead-lettered at once; operator fixes the job and replays |

### Failover requirements (production)

We rely on Patroni with etcd for database failover and treat these as preconditions:

| # | Requirement | How it is met |
|---|---|---|
| F1 | Every acknowledged commit is on at least one standby | `synchronous_commit = on` |
| F2 | The primary never falls back to asynchronous replication | Patroni `synchronous_mode: true` + `synchronous_mode_strict: true`; with no synchronous standby, writes block |
| F3 | Only a replica holding every acknowledged commit can be promoted | 1 primary + 2 standbys with `FIRST 1`; Patroni promotes only the standby it has recorded as synchronous. In quorum mode (`ANY k of n`), promotion must reach n − k + 1 standbys and pick the highest flushed WAL position, or refuse. |
| F4 | The old primary can't accept writes after a promotion | Patroni leader lease + watchdog; clients connect with `target_session_attrs=read-write` |
| F5 | Fencing epochs strictly increase | New timeline on every promotion + `cluster_generation` on rebuilds + startup check |

---

## Trade-offs

- **Consistency over availability.** If the database is unreachable, nothing fires. Firing from local state could fire twice.
- **Latency vs database load.** A 200 ms poll keeps lag around 100 ms at about 15 claim queries per second across the cluster. LISTEN/NOTIFY could cut lag further later.
- **PostgreSQL vs etcd, ZooKeeper or Kafka.** One dependency, transactional state and a single trusted clock, with a hard write ceiling (see [Where it stops scaling](#where-it-stops-scaling)).
- **Hard caps vs throughput.** Caps leave capacity idle during single-tenant bursts, in exchange for isolation between tenants.
- **Lease length.** A 10 s lease gives fast takeover. A 20 s GC pause causes an extra attempt, which fencing makes safe.
- **Exact caps vs write cost.** Tenant slots enforce the cap exactly, at the cost of one slot-row lock per run start. Per-node shares would be cheaper but could exceed the cap while partitions move.
- **Strict lease expiry.** A write after the lease expired is rejected even if nobody has taken over, which costs an occasional rerun (deduplicated) and makes slot reuse race-free.
- **Durability vs availability on failover.** Strict synchronous replication blocks writes when no standby is available, and a promotion is refused rather than lose acknowledged jobs. Each commit pays about one LAN round trip.
- **Deferred cap reduction.** Lowering a cap doesn't stop running jobs; new starts wait until the running count drops below the new cap.

**Left out on purpose:** multi-region scheduling, exactly-once effects against APIs that ignore idempotency keys, job dependencies and DAGs, fixed-rate schedules, sub-second precision, an admin UI, real SMS and push providers, a full OAuth2 identity provider, and our own failover manager (Patroni is relied on).

---

## Assumptions

- A mock notification sink stands in for real SMS and push providers.
- Jobs need precision in seconds, not milliseconds.
- Tenant time zones are valid IANA zone IDs.
- The PostgreSQL primary's clock never steps backwards (NTP in slew mode), and database nodes stay within about 2 s of each other.
- In production, a single failover manager (Patroni + etcd) meets F1–F5 and prevents split brain. The single-node demo doesn't claim to survive a database failover.
- A frozen process can run for a moment after it wakes. The guarantees cover valid leases and effects, not what a paused process physically does.

---

## Design review fixes

A review of the first design found four problems, all fixed above:

| # | Issue | Fix |
|---|---|---|
| 1 | Takeover could take about 21 s, because the reaper only scanned owned partitions | Orphan sweep in any partition, faster partition leases, recovered runs first |
| 2 | Per-node cap shares could exceed the tenant cap while partitions moved | Tenant slot table enforced by foreign key, CHECK and unique index; deferred cap reduction |
| 3 | A slot could be reused while its holder renewed the lease | Expired leases can't be renewed; conditional reap before slot reuse; `clock_timestamp()`; fixed lock order |
| 4 | Tokens could go backwards after an async failover | `(epoch, seq)` tokens with a timeline-based epoch; synchronous replication with promotion rules F1–F5 |

---

## Documents

- [`docs/DESIGN.md`](docs/DESIGN.md): architecture diagram, sequence diagram for the freeze case, failure-mode table
- [`docs/BENCHMARK.md`](docs/BENCHMARK.md): full benchmark output
- Design plan: https://claude.ai/artifact/JuMnTBHToDBjXiXkPYSVz3
- Demo video: _TBD_
