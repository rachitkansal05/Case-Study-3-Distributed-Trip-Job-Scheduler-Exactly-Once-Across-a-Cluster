# Distributed Trip Job Scheduler

A cluster scheduler for MoveInSync trip jobs: nightly rosters at 22:00 tenant time, "your cab is assigned" alerts 30 minutes before pickup, no-show auto-cancels and daily billing close. It runs on three or more identical nodes. A node can be killed or frozen at any time without a job being lost or its effect happening twice.

> MoveInSync Backend Case Study 2026 · Case Study 3 · Advanced Track
>
> Design note: [`docs/DESIGN.md`](docs/DESIGN.md) · full plan: [`PLAN.md`](PLAN.md) (mirrors https://claude.ai/artifact/JuMnTBHToDBjXiXkPYSVz3) · benchmark: [`docs/BENCHMARK.md`](docs/BENCHMARK.md) · demo script: [`docs/DEMO.md`](docs/DEMO.md)

**Status:** implemented. 855 automated tests (853 pass, 2 Docker-gated tests skip without Docker). Every number in this file was measured on the machine in [Benchmark](#benchmark) unless it says *estimate*. What could not be run locally is listed under [Known limitations](#known-limitations-and-what-was-not-verified-locally).

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
- [Testing](#testing)
- [Benchmark](#benchmark)
- [Capacity, bounds and where it stops scaling](#capacity-bounds-and-where-it-stops-scaling)
- [Cost estimation](#cost-estimation)
- [Failure modes](#failure-modes)
- [Trade-offs](#trade-offs)
- [Assumptions](#assumptions)
- [Known limitations and what was not verified locally](#known-limitations-and-what-was-not-verified-locally)
- [Repository layout](#repository-layout)
- [Design review fixes](#design-review-fixes)
- [Documents](#documents)

---

## Quick start

Requirements: Docker with Compose v2. To build and test without Docker: JDK 21 or newer (the wrapper downloads Maven).

```bash
docker compose up --build
```

This builds the two Spring Boot images (`scheduler-app/Dockerfile`, `notification-sink/Dockerfile`) and starts:

| Service | Port | Purpose |
|---|---|---|
| `postgres` | 5432 | PostgreSQL 16: job store and the cluster's only clock. Runs with `idle_in_transaction_session_timeout=5s`, so a `docker pause`d node cannot hold row locks for long |
| `scheduler-1`, `scheduler-2`, `scheduler-3` | 8081–8083 | Identical scheduler nodes, profiles `json,dev` |
| `notification-sink` | 8090 | Mock SMS/push provider that enforces idempotency keys and fencing tokens |
| `toxiproxy` | 8474 | Fault injection API; each node reaches Postgres and the sink through its own proxy |
| `prometheus` | 9090 | Metrics and the 13 alert rules (firing alerts at http://localhost:9090/alerts; no Alertmanager) |
| `grafana` | 3000 | Dashboard at http://localhost:3000/d/scheduler-overview: anonymous read-only, or admin / admin |

Every service has a health check; the nodes start after Postgres and the sink are healthy. `docker compose ps` shows when all are `healthy`.

> **The `dev` profile is for the demo only.** It opens `POST /dev/token`, which mints a token for any tenant and role without authentication. Never enable it in production, and set `SCHEDULER_JWT_HMAC_SECRET` and `SINK_ADMIN_TOKEN` to real secrets outside a laptop.

### First calls

Tenants are created by an OPERATOR; jobs by an ADMIN of that tenant (the tenant always comes from the token).

```bash
# tokens (valid 1 hour, accepted by every node)
OP=$(curl -s -X POST localhost:8081/dev/token -H 'Content-Type: application/json' \
  -d '{"tenant":"platform","role":"OPERATOR"}' | jq -r .token)

curl -s -X POST localhost:8081/api/v1/tenants -H "Authorization: Bearer $OP" \
  -H 'Content-Type: application/json' -d '{"id":"acme","timeZone":"America/New_York","cap":32}'

ADMIN=$(curl -s -X POST localhost:8081/dev/token -H 'Content-Type: application/json' \
  -d '{"tenant":"acme","role":"ADMIN"}' | jq -r .token)

# a recurring roster job at 22:00 New York time
curl -s -X POST localhost:8081/api/v1/jobs -H "Authorization: Bearer $ADMIN" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: roster-blr-01' \
  -d '{"kind":"RECURRING","handler":"ROSTER_GEN","cron":"0 22 * * *",
       "payload":{"shiftDate":"2026-11-01","siteId":"BLR-01"},
       "maxRunTime":"PT5M","misfire":"COALESCE"}'

# a one-off pickup alert for three employees, 30 s from now (GNU date; macOS: date -u -v+30S +%FT%TZ)
RUN_AT=$(date -u -d '+30 seconds' +%FT%TZ)
JOB=$(curl -s -X POST localhost:8081/api/v1/jobs -H "Authorization: Bearer $ADMIN" \
  -H 'Content-Type: application/json' \
  -d '{"kind":"ONE_OFF","handler":"PICKUP_ALERT","runAt":"'"$RUN_AT"'","priority":1,
       "maxRunTime":"PT5M","misfire":"SKIP",
       "payload":{"employeeIds":["emp-0001","emp-0002","emp-0003"],
                  "messageTemplate":"Your cab arrives at {pickupTime}","pickupTime":"08:05"}}' | jq -r .id)

# after it ran: its run history, then the sink's exactly-once counters for that run
RUN=$(curl -s localhost:8081/api/v1/jobs/$JOB/runs -H "Authorization: Bearer $ADMIN" | jq -r '.items[0].runId')
curl -s "localhost:8090/api/v1/deliveries/stats?runId=$RUN"   # deliveredDistinct 3
curl -s localhost:8090/api/v1/deliveries/stats                 # maxDeliveriesPerKey 1

# run it again now: the Idempotency-Key header is required, a repeat returns the same run
curl -s -X POST localhost:8081/api/v1/jobs/$JOB/trigger -H "Authorization: Bearer $ADMIN" \
  -H 'Idempotency-Key: manual-1'
```

Cron expressions have five fields (`minute hour day-of-month month day-of-week`) and are evaluated in the tenant's time zone. Handler payloads: `PICKUP_ALERT` `{"employeeIds":[…],"messageTemplate":"…{employeeId}…{pickupTime}…","pickupTime":"08:05"}`, `ROSTER_GEN` `{"shiftDate":"2026-11-01","siteId":"BLR-01"}`, `NO_SHOW_CANCEL` `{"tripId":"T-1001","employeeId":"e42"}`, `BILLING_CLOSE` `{"period":"2026-10","accountId":"ACME"}`. The API checks only that the payload is a JSON object (≤ 64 KiB); a payload the handler cannot read is dead-lettered on its first attempt as `INVALID_PAYLOAD`.

A Postman collection with every call above and more is in [`docs/postman/`](docs/postman/). The on-call guide is [`docs/RUNBOOK.md`](docs/RUNBOOK.md).

The demo runs a single PostgreSQL and does not claim to survive a database failover. The production failover setup is in [`deploy/patroni/`](deploy/patroni/) (see [Failover requirements](#failover-requirements-production)).

---

## What it guarantees

**Exactly-once execution is impossible.** A node can send an alert and then die before it records the send. To the rest of the cluster that looks the same as a node that died before sending. A node in a 30-second GC pause also looks the same as a dead one. A scheduler has two choices: retry and maybe repeat the work, or never retry and maybe lose it. Losing jobs is not allowed, so this scheduler retries.

**What we guarantee instead:**

- Every scheduled occurrence is **executed at least once**.
- Its **side effect happens exactly once**. Every run has a deterministic idempotency key (`{jobId}:{scheduledAtUtc}`, or `{jobId}:manual:{Idempotency-Key}` for a manual run), and every item inside a run has its own key (`{runKey}:{employeeId}`). The receiving side stores keys under a unique constraint.
- A node whose lease has expired **cannot write anything**: no renewal, no checkpoint, no commit, even if nobody has taken the run over yet. Every state change carries a fencing token `(epoch, seq)`. The database and the notification sink both reject writes with an outdated token.
- **Across a PostgreSQL failover**, no acknowledged write is lost and fencing tokens never go backwards, as long as the [failover requirements](#failover-requirements-production) hold. If they can't be met, writes stop rather than continue unprotected.

This relies on downstream systems honouring idempotency keys, or on a dedupe table we control in front of them. No scheduler can make a non-idempotent third-party API exactly-once.

**Which clock decides when a job is due:** the PostgreSQL primary's clock. Due-time and lease checks are SQL predicates using `clock_timestamp()`, the current time. (`now()` is the time the transaction started, so it can call an expired lease valid.) Each node uses its monotonic clock only to stop itself early when it has not renewed a lease in time.

The argument in five bullets, with the sequence diagram, is in [`docs/DESIGN.md`](docs/DESIGN.md#3-why-the-effect-happens-exactly-once).

---

## How it works

The architecture diagram (Mermaid) is in [`docs/DESIGN.md` §1](docs/DESIGN.md#1-architecture). In short:

```
         Admin / Operator / Employee (REST + JWT)       Prometheus → Grafana
                     │                                 ┆ scrape (nodes and sink)
     ┌───────────────┼─────────────────────────────────┼──────┐
     │  Node 1         Node 2         Node 3   (identical, no leader)
     │  membership · partition leases · poller · dispatcher · orphan sweep · materialiser
     └───────┬───────────────┬───────────────┬────────────────────┘
             │ claim SKIP LOCKED, fenced writes        │ effect + idempotency key + token
             ▼                                        ▼
     PostgreSQL 16  (job, job_run, partition_lease,   Notification sink
     scheduler_node, tenant, tenant_slot,             (UNIQUE key, highest token per run)
     dead_letter, api_idempotency; the clock)
```

Each node runs these loops:

| Loop | Interval | Job |
|---|---|---|
| Membership | 2 s | Heartbeat into `scheduler_node`; a node is live if its heartbeat is under 6 s old (DB time) |
| Partitions | 1 s | Compute owned partitions by rendezvous hashing over live nodes; acquire, renew (6 s lease, renewed every 2 s) or release |
| Materialiser | 5 s | Create `SCHEDULED` runs up to 15 minutes ahead (`UNIQUE (job_id, scheduled_at)` makes it safe to repeat) |
| Poller | 200 ms, plus on every finished run | Claim due runs from owned partitions, per tenant, one tenant slot per run. When a run finishes, the worker thread claims for its tenant at once (at most 4 at a time per node), so a freed slot is refilled within one round trip |
| Dispatcher | continuous | 64 virtual-thread workers per node, taking from tenant queues in turn (deficit round robin); recovered runs first, with 1 worker reserved for them |
| Lease renewer | 3 s | Extend each running run's 10 s lease with a fenced update |
| Orphan sweep | 500 ms | Take over expired `RUNNING` runs in **any** partition and restart them in the same slot |
| Slot compactor | 1 s | Finish lowered caps (see [Changing a cap](#changing-a-cap)) |

### No single hot lock

Jobs are split into **128 fixed partitions** (`murmur3(jobId) % 128`), each with its own lease row. Nodes contend per partition, not on one global lock row. When a node joins or leaves, only the partitions that hash to it move (about `128 / N`). With the compose node ids the split is 39 / 40 / 49; rendezvous hashing gives multinomial counts, so balance is a statistical property (max/min about 1.3: mean 1.31, median 1.27 over 10,000 random 3-node sets), not an exact one.

Partition ownership only spreads polling load; correctness does not depend on it. If two nodes poll the same partition during a handover, `FOR UPDATE SKIP LOCKED` and the run-level fencing token still allow only one claim per run.

When a partition's lease expires, its previous owner is treated as dead for that partition at once, so another node picks it up within about 7 s (`RebalanceIT`). Running jobs don't wait for that: the **orphan sweep** takes over expired runs in any partition.

**Takeover bound: ≤ 10.7 s** after a crash (10 s run lease after its last renewal + up to 0.5 s sweep + about 0.2 s claim), given at least one live node. Measured from the kill to the new claim: 9.99–10.15 s in `NodeCrashIT`, 7.97–8.22 s in `EndToEndKillIT`. The tests assert ≤ 11.2 s, the bound of the original 1 s sweep, which also stays the alert threshold.

### Claim, fence, commit

All engine writes to run state and slots go through one class, `JdbcClaimProtocol`. Full polls, slot-freed claims and the orphan sweep all use it. (The API writes runs in four documented ways that never touch a running run or an occupied slot: manual-trigger inserts, cancelling unstarted runs of a disabled job, dead-letter replay, and cap changes.)

1. **Claim**, in one transaction, always locking the slot row first and the run row second:
   - Lock a free-looking slot of the tenant below its configured cap (`FOR UPDATE SKIP LOCKED`).
   - If the slot still has a holder whose lease looks expired, **reap it with a conditional update**: `UPDATE job_run SET state = 'RETRY_WAIT', slot_no = NULL WHERE id = ? AND state = 'RUNNING' AND lease_until < clock_timestamp()`. One row updated means the slot is free. Zero rows means the holder renewed in time, so the slot is skipped. Because the reap and a renewal update the same row, Postgres runs them one at a time and exactly one succeeds.
   - Start the most urgent due run (`ORDER BY priority, due_at`): `state = RUNNING`, `slot_no`, token `(epoch, nextval('fence_seq'))`, `lease_until = clock_timestamp() + 10 s`.
   - The token is used only after this transaction commits.
2. **Execute.** The handler gets the run's idempotency key, fencing token and last checkpoint. Every outbound call sends `Idempotency-Key` and `Fencing-Token: <epoch>.<seq>`. The renewer extends the lease every 3 s with `WHERE id = ? AND (fencing_epoch, fencing_seq) = (?, ?) AND state = 'RUNNING' AND lease_until >= clock_timestamp()`, so **an expired lease can never be renewed**.
3. **Checkpoint.** Chunked handlers such as the 1,200 pickup alerts send in chunks of 50 and save `{"nextItem": n}` after each chunk, using the same fenced predicate.
4. **Commit:** the same predicate, setting `state = 'SUCCEEDED'` and `slot_no = NULL` in one statement. Zero rows updated means the lease was lost; the commit is rejected and counted (`scheduler_stale_commit_rejected_total`). Commits never lock slot rows, so claims and commits can't deadlock (`LockOrderIT`).

### Fencing tokens

- A token is a pair `(fencing_epoch, fencing_seq)`, compared epoch first. There's no bit-packing of the sequence, so there's no range limit.
- `fencing_epoch = cluster_generation × 2^32 + timeline`. PostgreSQL's timeline ID rises on every promotion, so tokens from a new primary beat every token from the old one. If the timeline can't be read, the claim fails and nothing starts.
- `cluster_generation` is raised by operators when rebuilding from a fresh `initdb`, which restarts at timeline 1. Nodes refuse to start if the current epoch is lower than the sink's highest accepted epoch (`EpochGuard`).
- A `409` for an outdated token means **lease lost**: the handler stops and the attempt doesn't count toward `maxAttempts`. After 20 lease losses the run is dead-lettered as `FENCING_CONFLICT`, which raises `SchedulerFencingConflictDeadLetters`.

Run states (`RunStateMachine`):

```
SCHEDULED → RUNNING → SUCCEEDED
              │  ├──► RETRY_WAIT → RUNNING        (failure or expired lease, new token)
              │  └──► DEAD_LETTERED → SCHEDULED   (operator replay, same row and key)
SCHEDULED → SKIPPED    (misfire SKIP / COALESCE)
SCHEDULED, RETRY_WAIT → CANCELLED   (job disabled)
```

Pausing a tenant does not change run states: its runs stay `SCHEDULED` and the claim skips the tenant (it reads `tenant.paused` directly, never from a cache). After a resume the misfire policy decides what happens to runs that became late.

### Freeze, step by step

1. Node A claims run 77 in slot 4 with token `7.41`, sends alerts 1–600 and saves checkpoint 601.
2. Node A is frozen (`docker pause`) for 30 s. Its lease expires 10 s after its last renewal.
3. Within 500 ms, node B's orphan sweep locks slot 4, reaps run 77 and restarts it in the same transaction, in the same slot, with token `7.42`. It sends alerts 601–1,200 from the checkpoint and commits.
4. Node A wakes up. Its monotonic deadline has passed, so it stops. A send still in flight with token `7.41` gets a `409` from the sink, and any renewal or commit with token `7.41` updates zero rows. If A was frozen inside a transaction, Postgres ended its session after 5 s idle, so its row locks did not block B.
5. Result: 1,200 distinct deliveries, zero duplicates, and the tenant's running count never went above its cap (`EndToEndFreezeIT`).

---

## Time zones and daylight saving time

Cron expressions match **local wall time in the tenant's zone** (five fields, Vixie cron day matching, parsed by our own `CronSchedule`). `WallClockResolver` then maps each local time to exactly one instant.

| Case | Example (America/New_York) | Rule | Fires at |
|---|---|---|---|
| Clocks jump 02:00 → 03:00 | 8 Mar 2026, job at 02:30 | Fires once at **03:00 local**, when the skipped hour ends, as classic cron does. Java's default would be 03:30. | 07:00Z |
| 01:00–01:59 happens twice | 1 Nov 2026, job at 01:30 | Fires **once, at the first 01:30** (EDT, −04:00) | 05:30Z |
| Sub-hourly job in the repeated hour | `*/15 * * * *` | Each distinct local time fires once: 4 runs in the repeated hour, not 8 | — |
| Zone without DST | Asia/Kolkata, 22:00 | Direct mapping | 16:30Z |

`GET /api/v1/jobs/{id}/next-fire-times?count=N` shows the instants with local time and offset, so the DST behaviour is visible through the API. The next US transitions after today are 1 Nov 2026 (fall back) and 14 Mar 2027 (spring forward).

---

## Misfires, retries and the dead-letter list

A run has **misfired** when it is claimed more than `misfireThreshold` (default 60 s, at most 7 days) after its scheduled time, for example after cluster downtime or a tenant pause. Each job picks a policy (default `FIRE_NOW`):

| Policy | After 3 missed hourly runs | Typical use |
|---|---|---|
| `FIRE_NOW` | All 3 run now, in order | Billing close: every period must close |
| `SKIP` | All 3 skipped; the next run is normal | Pickup alerts: a late alert is useless |
| `COALESCE` | The newest runs once with `coalescedCount = 3`; the others are skipped | Roster generation: only the latest matters |

**Retries** use exponential backoff with full jitter: `delay = random(0, min(5 min, 2 s × 2^attempt))`. Sink 5xx (`SINK_5XX`), timeouts (`SINK_TIMEOUT`) and the max-run-time stop (`TIMEOUT`) are retried. Payloads the handler cannot read (`INVALID_PAYLOAD`) and other sink 4xx (`SINK_REJECTED_<status>`) are dead-lettered at once. After `maxAttempts` (default 5, up to 100) the run is dead-lettered. An operator lists dead letters (error code and counts only, never payload data) and replays them: the **same run row and idempotency key** go back to `SCHEDULED`, so items delivered before the run died are not sent again. Replaying twice returns `409 DEAD_LETTER_ALREADY_REPLAYED`.

**Replays and manual triggers ignore the misfire policy.** Both are explicit requests to run, so the run is flagged `misfire_exempt`: the claim runs it however late it is (a replay keeps the original `scheduled_at`, which the handler and the idempotency key depend on), never skips it and never coalesces it with the job's scheduled occurrences, which still follow the policy. A replayed run also does not report fire lag again; only a run's first start does (`ReplayMisfireIT`).

**Max run time is a hard stop.** At `maxRunTime` the handler is interrupted and further effects are refused; the attempt fails with `TIMEOUT` and goes through the retries above. A handler that returns anyway is still a `TIMEOUT`, never a success. One that ignores the interrupt is abandoned one renew interval (3 s) later: the engine records the `TIMEOUT` for it with the run's fenced commit, which frees its slot, and stops renewing its lease (`MaxRunTimeIT`).

---

## Per-tenant fairness

Each tenant has a cluster-wide concurrency cap (default 32), enforced by **slots**: the tenant has exactly `cap` rows in `tenant_slot`, and a run can only be `RUNNING` while holding one. The dispatcher takes from tenant queues in turn, so one tenant's backlog never blocks another tenant.

Scenario: 3 nodes × 64 workers, both tenants' alerts due at the same instant, each alert a 20–60 ms sink call (`docs/BENCHMARK.md` §3.2, `TenantFairnessIT`).

| | Without caps (one FIFO queue), *estimate* | With caps (32 per tenant), planned | With caps, **measured** |
|---|---|---|---|
| Tenant A, 50,000 alerts | drains in ≈ 10.4 s | 800 runs/s → 62.5 s | **698 runs/s → 71.6 s** |
| Tenant B, 20 alerts | waits up to 10.4 s ❌ | starts within one poll, < 0.3 s | **max lag 185 ms** ✅ |

Tenant A is slower than planned because each run holds its slot for about 46 ms, not 40 ms: the sink call plus the checkpoint, the fenced commit and the claim that refills the slot (32 / 0.046 s ≈ 700/s). `DrainRateIT` measures a single tenant at 614–645 runs/s and asserts ≥ 600. Before slot-freed claims, a capped tenant drained at about 144 runs/s.

A hard cap leaves capacity idle during a single-tenant burst; we chose isolation over peak throughput. Caps are configurable per tenant.

### Why the cap always holds

The database enforces it, with no tolerance during partition moves or crashes:

- A running run must hold a slot: `CHECK (state <> 'RUNNING' OR (slot_no IS NOT NULL AND fencing_seq IS NOT NULL))`.
- The slot must be a real slot row of the same tenant: composite foreign key `(tenant_id, slot_no) → tenant_slot`, `ON DELETE RESTRICT`.
- No slot is used twice: `UNIQUE (tenant_id, slot_no) WHERE state = 'RUNNING'`.
- `job_run.slot_no` is the only record of who holds a slot, and it's cleared whenever a run leaves `RUNNING`.

So running runs per tenant ≤ its slot rows. A slot passes to a new run only after the old run is reaped by a conditional update. Precisely: at most `cap` runs per tenant hold a valid lease at any moment. A frozen process may run for a moment when it wakes; fencing rejects anything it does. `TenantCapIT` samples this every 10 ms through random kills, freezes and partition moves, with no tolerance; `SchemaIT` shows the database itself rejects every violation.

### Changing a cap

Each tenant has a `configured_cap` (what the admin set) and an **effective cap**, its number of slot rows. The rule "running ≤ effective cap" has no exceptions.

- **Raising** a cap adds slot rows at once; they're usable on the next claim.
- **Lowering** a cap is deferred. Example: 28 running, cap lowered from 32 to 16:
  1. Free slots numbered 16 or higher are deleted at once. The database refuses to delete a slot in use.
  2. Whenever a slot below 16 becomes free, the compactor moves one running run from a high slot into it, then deletes the empty high slot.
  3. Claims use only slots below the configured cap, so **no new run starts until fewer than 16 are running** (`CapReductionIT`).

  The effective cap only goes down and reaches 16 within the longest remaining `maxRunTime` of the runs already active. Running jobs aren't stopped, and there is no option to revoke their leases early. `PUT /tenants/{t}/cap` answers with `configuredCap`, `effectiveCap` and `draining`; afterwards the `scheduler_tenant_cap{kind}` gauges show both numbers (there is no `GET /tenants/{t}`).

---

## API

All endpoints are under `/api/v1` and need a bearer token. Roles are checked per path; a wrong role is `403 FORBIDDEN`, a missing or bad token `401 UNAUTHORIZED`.

| Method and path | Purpose | Roles | `Idempotency-Key` |
|---|---|---|---|
| `POST /tenants` | Create a tenant: `{"id","timeZone","cap"}` (cap optional, default 32) | OPERATOR | ignored (a repeat gets `409 TENANT_EXISTS`) |
| `POST /jobs` | Create a one-off or recurring job (201) | ADMIN | optional |
| `GET /jobs/{id}` | Job definition; payload only for ADMIN, `payloadRedacted: true` otherwise | ADMIN, EMPLOYEE, OPERATOR | — |
| `GET /jobs/{id}/next-fire-times?count=10` | Next 1–100 fire times with local time and offset | ADMIN, EMPLOYEE | — |
| `POST /jobs/{id}/trigger` | Run now (202); a repeat returns the same run with `duplicate: true` | ADMIN, OPERATOR | **required** |
| `DELETE /jobs/{id}` | Disable a job and cancel its unstarted runs | ADMIN | — |
| `GET /jobs/{id}/runs?page=0&size=50` | Run history: state, attempt, lag, error code | ADMIN, OPERATOR | — |
| `POST /tenants/{t}/pause` · `/resume` | Pause or resume all of a tenant's jobs | ADMIN (own tenant), OPERATOR | optional |
| `PUT /tenants/{t}/cap` | Change the cap (`{"cap": 0..10000}`); returns configured, effective and `draining` | ADMIN (own tenant), OPERATOR | optional |
| `GET /dead-letters?tenant=&page=&size=` | List dead-lettered runs (OPERATOR: all tenants or one; ADMIN: own) | OPERATOR, ADMIN | — |
| `POST /dead-letters/{runId}/replay` | Replay with the same idempotency key | OPERATOR | optional |
| `GET /cluster` | Live nodes, partitions per node, every partition's owner and lease epoch | OPERATOR | — |

`POST /dev/token` (outside `/api/v1`) mints demo tokens and exists only in the `dev` profile. There is no list-jobs endpoint. Lists are paged with `page` / `size` (≤ 200) and return `{"items", "page", "size", "hasMore"}`.

**Retry-safe writes.** With an `Idempotency-Key` (1–128 printable ASCII characters), the action and its stored response commit in one transaction: a retry gets the original status, `Location` and body with `Idempotent-Replayed: true`, and no second action. Reusing a key with a different request is `409 IDEMPOTENCY_KEY_REUSED`. Keys are namespaced per tenant and honoured for **24 hours**. `IdempotencyPurge` deletes older records every 10 minutes in batches of 1,000 (`scheduler.api.idempotency-purge.interval`, `.batch-size`, `.max-batches`); it uses `FOR UPDATE SKIP LOCKED`, so it is safe on all nodes at once. A manual run is also deduplicated by its run key `{jobId}:manual:{key}`, which never expires.

Errors are RFC 7807 problem details (`application/problem+json`) with a stable `code`, a safe `detail` and a `traceId` (also in the `X-Trace-Id` header):

```json
{
  "type": "about:blank",
  "title": "Not Found",
  "status": 404,
  "detail": "Job not found",
  "instance": "urn:trace:4f1c2a9e7b3d4c5a8e2f1b0c9d8e7f6a",
  "code": "JOB_NOT_FOUND",
  "traceId": "4f1c2a9e7b3d4c5a8e2f1b0c9d8e7f6a"
}
```

Responses never contain stack traces, SQL or payload values. Validation errors (`400 VALIDATION_FAILED`, `INVALID_CRON`) list `errors: [{field, reason}]` and never echo the value. The notification sink's own API is in [`docs/sink-api.md`](docs/sink-api.md).

---

## Security

- **Roles.** EMPLOYEE reads schedules (job, next fire times) of their own tenant. ADMIN manages one tenant's jobs, pause, resume and cap. OPERATOR is the platform on-call role: it creates tenants and works across tenants, but only on operational data (states, counts, error codes), never payloads. The full matrix is tested endpoint by endpoint (`RoleMatrixIT`).
- **Tenant isolation.** The tenant comes only from the token's claim, never from the request body or path. Every repository query takes a `TenantScope` and adds a tenant predicate, so another tenant's row is simply not found: a request for another tenant's job, tenant or dead letter returns `404`, not `403`, and changes nothing (`TenantIsolationIT`).
- **Tokens.** HS256 JWTs with `roles` and tenant claims, validated by Spring Security's resource server. The secret is `SCHEDULER_JWT_HMAC_SECRET` (at least 32 characters); the compose default is an obviously fake dev key. `/dev/token` exists only in the `dev` profile (`DevTokenIT`, `DevTokenDisabledIT`); in production tokens would come from an identity provider.
- **No personal data in logs or errors.** Payloads, which can hold phone numbers, emails and home locations, are never logged or returned to non-admins. JSON logs mask every string value through `PiiValueMasker`, plain-text logs through `PiiMaskingConverter`, as a last line of defence (`PiiMaskingTest`, `LoggingIT`). The sink stores only the SHA-256 of a payload and logs only a key hash (`PayloadPrivacyIT`).
- The sink's fault-injection API (`/admin/faults`) needs `X-Admin-Token` (`SINK_ADMIN_TOKEN`).

---

## Monitoring

Metrics are at `/actuator/prometheus` on every node and on the sink; Prometheus scrapes all four every 5 s. Grafana provisions the "Trip Job Scheduler" dashboard (fire lag, runs and outcomes, claims and partitions, leases and takeovers, dead letters by error code, tenant caps, the sink, JVM/Hikari/caches).

| Metric | What it tells on-call | Alert (rule name) |
|---|---|---|
| `scheduler_fire_lag_seconds` (histogram, SLO buckets 0.1–30 s, by `handler`, `tenant_tier`) | Actual start minus scheduled time, DB clock | p99 > 2 s for 5 min (`SchedulerFireLagHigh`) |
| `scheduler_runs{state}` | Running, retrying and dead-lettered counts (DB counts, every 5 s) | Any new dead letters (`SchedulerDeadLettersIncreasing`) |
| `scheduler_queue_depth` | Runs due but not started | Rising for 2 min (`SchedulerQueueDepthGrowing`) |
| `scheduler_run_outcome_total{outcome}` | success, retryable, permanent, timeout | Error rate > 5 % for 5 min (`SchedulerErrorRateHigh`) |
| `scheduler_stale_commit_rejected_total` | Fenced commits that matched nothing (GC pauses, network splits) | Any increase (`SchedulerStaleCommitsRejected`) |
| `scheduler_lease_lost_total{reason}` | `stale_409`, `fenced_write_0_rows`, `self_fence` | > 5 for one reason in 5 min (`SchedulerLeaseLostSpike`) |
| `scheduler_dead_letters_total{error_code}` | Dead letters by error code | Any `FENCING_CONFLICT` in 15 min (`SchedulerFencingConflictDeadLetters`) |
| `scheduler_lease_renew_failures_total` | A node losing its leases | Any in 2 min (`SchedulerLeaseRenewFailures`) |
| `scheduler_orphan_takeovers_total`, `scheduler_orphan_takeover_time_seconds` | Takeovers, and time from the dead owner's last lease renewal to the restart (normally 10–10.7 s; buckets include 10.7 s and 11.2 s) | p99 > 11.2 s (`SchedulerOrphanTakeoverSlow`) |
| `scheduler_partitions_owned{node}` | Partition spread | Total ≠ 128 for 10 s (`SchedulerPartitionsUnowned`) |
| `scheduler_claim_batch_seconds` | Claim transaction latency | p99 > 100 ms for 5 min (`SchedulerClaimBatchSlow`) |
| `scheduler_tenant_cap{tenant,kind}` | Configured vs effective cap | Effective > configured for 30 min (`SchedulerTenantCapDraining`) |
| `scheduler_tenant_cap_rejections_total{tenant}` | Claims held back by tenant caps | Informational |
| `up{job=~"scheduler\|notification-sink"}` | A node or the sink not scraped | 30 s (`SchedulerTargetDown`) |
| `sink_deliveries_total{outcome}`, `sink_delivery_latency_seconds` | What the provider saw: delivered, already delivered, stale token, injected fault | — |

That is **13 alert rules** in [`ops/prometheus/alerts.yml`](ops/prometheus/alerts.yml), each with a `runbook_url` to its section of [`docs/RUNBOOK.md`](docs/RUNBOOK.md). There is no Alertmanager in the demo; firing alerts are listed at http://localhost:9090/alerts. `AlertRulesTest`, `GrafanaDashboardTest` and `RunbookTest` check the files against [`ops/metric-names.txt`](ops/metric-names.txt), and `MetricsIT` and `OpsMetricNamesIT` prove every listed name is really exported.

`scheduler_orphan_takeover_time_seconds` is measured on the DB clock from the dead owner's last successful lease renewal (the expired `lease_until` minus the 10 s run lease: the last moment that node was proven alive) to the takeover claim. That is exactly the interval the 10.7 s bound limits, so the 11.2 s alert compares like with like; it is at most one renewal interval (3 s) longer than the time from the crash itself. `NodeCrashIT` checks that the recorded value matches its own measurement.

**Logs** are JSON lines (profile `json`, used by compose). Request lines carry `traceId` and `tenantId`; lines written while a run executes carry `nodeId`, `runId`, `jobId`, `tenantId`, `fencingToken`, `attempt` and `idemKeyHash` (a hash, never the raw key). `LoggingIT` checks the fields and that no phone, email or coordinate from a payload reaches the output.

---

## Caching

| Cache | Key and bound | How stale it can be |
|---|---|---|
| Job definitions (engine, `RunExecutor`) | `(jobId, version)`, 50,000 entries | Never: a run records the version it was materialised from, and a version never changes |
| `tenant-settings` (time zone) | tenant id, 10,000 entries, expire 5 s after write | Up to 5 s |
| `next-fire-times` | `(jobId, version, count)`, 50,000 entries, expire 30 s after write | Up to about 35 s after a tenant's time zone changes (5 s tenant-settings + 30 s), though there is no API to change a zone yet |

**Caps and pause are never cached**: the cap is the slot rows themselves and the claim reads `tenant.paused` directly, so both take effect on the next claim (`CachingIT`). The two Spring caches export `cache_gets_total`, `cache_puts_total`, `cache_evictions_total` and `cache_size`.

**Why Caffeine and not Redis.** Nothing whose staleness could break a guarantee is cached: leases, tokens, slots, pause and due times are always read from Postgres inside the claim. What is cached is small (a time zone, a version-keyed definition, a computed list of instants), bounded in staleness by a short TTL or by an immutable version, and needs no invalidation across nodes. A shared Redis would add a network hop to every lookup and one more stateful service to run, secure and fail over, to save a few cheap primary-key reads. The cost of the in-process choice is that each node warms its own cache (up to 3× the misses) and a zone change takes up to 30 s to show in `next-fire-times`.

---

## Testing

```bash
./mvnw -B verify          # Windows: mvnw.cmd -B verify. About 6 minutes, no Docker needed
```

Unit tests (`*Test`) run under Surefire, integration tests (`*IT`) under Failsafe. Integration tests run on **embedded PostgreSQL 16 (zonky)**, not Testcontainers, so Docker is not needed. Only the two failover tests use Testcontainers (`FailoverFencingIT`, `StrictSyncIT`); they are `@Testcontainers(disabledWithoutDocker = true)` and skip without Docker (CI on `ubuntu-latest` has Docker). Do not run the build from an Administrator shell on Windows: Postgres refuses to start as an administrator.

Results of the last full run on the build machine:

| Module | Unit | Integration | Result |
|---|---|---|---|
| `scheduler-core` | 322 | — | all pass |
| `scheduler-app` | 182 | 257 | 2 skipped (Docker-gated) |
| `notification-sink` | 47 | 35 | all pass |
| `loadgen` | 9 | 3 | all pass |
| **Total** | | | **855 tests, 853 passed, 2 skipped, 0 failures** |

Run one test, for example: `./mvnw -B -pl scheduler-app -am verify -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false -Dit.test=NodeCrashIT -Dfailsafe.failIfNoSpecifiedTests=false`.

Every hard rule from the case study has an automated test (full matrix in [PLAN §16](PLAN.md#16-test-matrix-every-hard-rule-is-automated)):

| Rule | Key tests |
|---|---|
| One-off and recurring jobs | `JobDefinitionTest`, `JobValidationIT` |
| Time zones and DST | `WallClockResolverTest`, `NextFireTimesTest`, `CronScheduleTest`, `MaterializerIT` |
| Exactly-once effect | `ExactlyOnceEffectIT` (3 nodes, 10,000 jobs, failures after the send: 10,000 distinct keys, none twice), `ConcurrentDeliveryIT` (sink) |
| Leases and fencing | `FencingIT`, `ClaimRaceIT` (32 threads), `SlotStealRaceIT` (both interleavings forced), `LeaseResurrectionIT`, `FencingTokenTest`, `StaleTokenClassificationTest` |
| Tokens survive failover | `EpochGuardIT`, `ClusterGenerationGuardIT` (loadgen, real sink), `FailoverFencingIT` and `StrictSyncIT` (Docker-gated) |
| Partitions move on join and leave | `PartitionAssignerTest`, `PartitionPlannerTest`, `RebalanceIT` (all 128 owned within 7 s of a worst-case kill) |
| Misfire policies | `MisfirePolicyTest`, `MisfireIT` |
| Fairness and caps | `TenantFairnessIT` (50,000 + 20; B p99 < 1 s), `DrainRateIT` (≥ 600 runs/s), `TenantCapIT` (no tolerance), `LockOrderIT`, `SchemaIT`, `CapReductionIT`, `CapIncreaseIT`, `CapApiIT` |
| Retries and dead-letter list | `BackoffPolicyTest`, `RetryDeciderTest`, `RetryToDeadLetterIT` |
| Ops API, retry-safe writes, caching | `OpsApiIT`, `ApiIdempotencyIT`, `IdempotencyPurgeIT`, `CachingIT` |
| Metrics, alerts, dashboard, logs | `MetricsIT`, `EngineMetricsTest`, `AlertRulesTest`, `GrafanaDashboardTest`, `RunbookTest`, `ComposeConfigTest`, `LoggingIT`, `OpsMetricNamesIT` |
| kill -9 during 1,200 alerts | `NodeCrashIT` (in-JVM kill right after a lease renewal), `EndToEndKillIT` (real sink over HTTP), `chaos/kill-during-alerts.sh` |
| 30 s freeze, stale write rejected | `GcPauseIT`, `EndToEndFreezeIT` (real sink: in-flight send gets 409, commit rejected), `chaos/freeze-during-alerts.sh` |
| Tenant isolation, roles, no PII leaks | `TenantIsolationIT`, `RoleMatrixIT`, `DevTokenIT`, `PiiMaskingTest`, `ErrorResponseTest`, `PayloadPrivacyIT` |

The chaos scripts run against the compose stack (`chaos/kill-during-alerts.sh`, `chaos/freeze-during-alerts.sh`; see [`chaos/README.md`](chaos/README.md)).

---

## Benchmark

The benchmark is `InProcessBench` (loadgen, test scope): three real scheduler nodes in one JVM with production settings (64 workers, 200 ms poll, 10 s run lease, 500 ms sweep), each with its own pool, calling the real notification sink over HTTP in a second JVM, on PostgreSQL with `fsync`, `synchronous_commit` and `full_page_writes` on. The sink adds 20–60 ms to every delivery to simulate provider I/O.

```bash
# one scenario per run: spread | burst | chaos | ramp | all
./mvnw -B -pl loadgen -am verify -Pbench -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test=InProcessBench -Dfailsafe.failIfNoSpecifiedTests=false -Dbench.scenario=spread
```

Knobs, a 5-minute smoke run and the compose-mode load generator (`java -jar loadgen/target/loadgen-0.1.0-SNAPSHOT.jar --scenario spread …`) are in [`docs/BENCHMARK.md` §2](docs/BENCHMARK.md#2-how-to-run-it). The run fails on any duplicate or unfinished run.

**Target:** 100,000 jobs due in one 10-minute window on 3 nodes, p99 fire lag under 2 s. **Met: p99 237 ms.**

| Scenario | Runs | Throughput | p50 lag | p99 lag | Max lag | Duplicates |
|---|---|---|---|---|---|---|
| Spread: 100,000 jobs, 50 tenants, 10 min | 100,000 | 166.6 runs/s (the arrival rate) | 108 ms | **237 ms** | 849 ms | **0** |
| Burst: tenant B (20 alerts) during A's 50,000 | 20 | — | 142 ms | 185 ms | **185 ms** | **0** |
| Burst: tenant A, 50,000 at once, cap 32 | 50,000 | 698 runs/s, drained in 71.6 s | 36.2 s | 70.8 s | 71.6 s | **0** |
| Spread + chaos: kill at 4 min, 30 s freeze at 7 min | 100,000 | same arrivals as spread | 109 ms | **291 ms** | 7.2 s | **0** |

**Machine:** Intel Core i7-10750H (6 cores / 12 threads, laptop), 16 GiB DDR4-2933, 1 TB NVMe SSD (Intel 660p, QLC), Windows 11 Home Single Language 10.0.26200, Java HotSpot 22.0.2 (code built with `--release 21`), benchmark JVM `-Xmx4g -Xms1g -XX:+UseG1GC` shared by the 3 nodes, sink JVM `-Xmx768m`. PostgreSQL 16.15 (zonky embedded binaries), `shared_buffers=512MB`, `max_connections=400`, `fsync=on`, `synchronous_commit=on`. No Docker: everything ran in-process or as a local process.

**The chaos run, honestly.** Across the whole 10 minutes p99 stayed at 291 ms, and nothing was lost or duplicated (100,000 distinct deliveries; 17 orphan takeovers; 5 re-sends absorbed as `ALREADY_DELIVERED`; 17 stale sends from the thawed node rejected with 409). But the 2 s target is **missed for runs that fall due in the dead or frozen node's partitions in the seconds after the event**:

| Window (runs scheduled in the 60 s after the event, in the victim's partitions) | Runs | p99 | Max |
|---|---|---|---|
| After the kill | 3,410 | 3.7 s | 4.4 s |
| After the 30 s freeze | 5,345 | 5.3 s | 7.2 s |
| Unaffected partitions, whole run | 30,377 | 240 ms | 1.3 s |

The cause is partition handover, not takeover: new runs in a dead or frozen node's partitions wait until its 6 s partition lease expires and a live node acquires the partition, as PLAN §12 predicts ("wait up to about 7 s"). Running runs are taken over by the orphan sweep regardless of partitions; their lag was already recorded at their first start. A shorter partition lease would shrink this window at the cost of more lease traffic. Full tables, the raw report names and an earlier blind-timed chaos run are in [`docs/BENCHMARK.md` §3.3](docs/BENCHMARK.md#33-scenario-3-spread--chaos-p99-291-ms-overall-0-duplicates-p99-in-the-affected-window-37-s-kill-and-53-s-freeze).

---

## Capacity, bounds and where it stops scaling

| Quantity | Value | Basis |
|---|---|---|
| Average arrival rate for the target | 167 runs/s | 100,000 / 600 s; measured 166.6 runs/s with p99 237 ms |
| Measured knee (p99 > 2 s) | **between 900 and 1,000 runs/s** | Ramp on this laptop, 3 nodes + sink + Postgres sharing 6 cores; about 5.5× the target rate |
| State transitions at the knee | about 2,700–3,000 /s | 3 per run: claim, checkpoint, commit |
| Typical lag | about 105 ms p50 | Half the 200 ms poll plus the claim |
| Single-tenant drain at cap 32 | ≥ 600 runs/s (614–645 measured) | `DrainRateIT`; 698 runs/s in the burst benchmark |
| Takeover after kill -9 | **≤ 10.7 s** bound; 9.99–10.15 s measured | 10 s lease + 0.5 s sweep + 0.2 s claim, whatever the partition timing (`NodeCrashIT`); 11.2 s is the test assertion and alert threshold |
| New runs in a dead node's partitions | up to about 7 s | 6 s partition lease + 1 s loop (`RebalanceIT` bound 7 s); benchmark worst 4.4 s (kill) and 7.2 s (freeze) |
| Partition balance | max/min about 1.3 (mean 1.31, p90 1.58 over random 3-node sets), not exact | Rendezvous hashing; compose ids give 39 / 40 / 49 |
| Synchronous replication (production) | + ≈ 1 ms per commit, *estimate* | One LAN round trip to the synchronous standby; not measured |

**Where it stops scaling.** On this laptop p99 crosses 2 s between 900 and 1,000 runs/s (900 passed warm with p99 246 ms and failed cold at 4.6 s, so it is right at the edge). Above the knee nothing fails or duplicates; runs queue and lag grows to 9–16 s. The earlier estimate of 5,000–10,000 transitions/s for Postgres alone is withdrawn: the measured knee is lower, and it is not a database-only limit, since the scheduler JVM, the sink JVM and Postgres share 6 cores and CPU reached 91–100 % in the failing steps. **The bottleneck was not isolated.** The leading hypothesis is the poller: each node claims tenant by tenant, one transaction after another, on a single thread; with 50 tenants that is up to 50 claim transactions per node per 200 ms tick, and once a tick takes longer than 200 ms lag grows without bound. The next step is to profile, or to run Postgres on separate hardware. If the poller is the limit, claim several tenants per transaction or in parallel; if Postgres is, shard by partition range (partitions already give the routing key) and later move dispatch to a partitioned log. 128 partitions also caps useful nodes at about 40.

**Complexity.** Claim: O(T · log R + b) per poll (T tenants with due runs, R due runs in owned partitions via a partial index, b claimed rows). Node memory: O(workers + batch), not O(jobs). Partition assignment: O(P · N) hashes per membership change (128 × 3 = 384). Materialisation: O(occurrences in the next 15 minutes).

---

## Cost estimation

A resource estimate for the target load (100,000 alerts in a 10-minute peak), derived from the measurements above. Prices are left out on purpose: multiply by your provider's rates.

| Component | Sizing | Reasoning |
|---|---|---|
| Scheduler nodes | 3 × (2 vCPU, 2–4 GB RAM) | During the spread run the *whole* laptop (3 nodes, sink and Postgres) averaged 15 % of 12 hardware threads, about 2 threads, and peaked at 52 %. Three nodes are the minimum for the failure model, not for throughput |
| PostgreSQL | 1 primary + 2 standbys (F1–F3), each about 4 vCPU, 8–16 GB RAM, SSD | One synchronous standby is required for durability on failover; the third member lets Patroni promote and keep a synchronous standby. The knee above (≈ 3,000 transitions/s on a shared laptop) leaves about 5× headroom at the target |
| Patroni coordination | 3 small etcd members | Leader lease and the synchronous-standby record (F3, F4) |
| Monitoring | Prometheus + Grafana, 1 small instance | 4 scrape targets every 5 s |
| Storage growth | about one `job_run` row per run kept, plus one `sink.delivery` row per item | `RunPurge` deletes finished runs 7 days after they finish (about 7 × the daily run count kept); dead-lettered runs stay until replayed. The sink's deliveries are not purged (see limitations) |

The dominant cost is the replicated Postgres, which is the price of the exactly-once and failover guarantees. Running fewer than 3 scheduler nodes saves little and loses the ability to absorb a crash and a freeze at the same time.

---

## Failure modes

The full table with detection, behaviour and the test that proves each row is in [`docs/DESIGN.md` §4](docs/DESIGN.md#4-failure-modes).

| What fails | What users see | How it recovers |
|---|---|---|
| Node killed mid-run | Alerts pause ≤ 10.7 s, then continue; nobody gets two | Lease expires; the orphan sweep on any node restarts the run in the same slot with a higher token from the checkpoint; the sink drops repeats |
| Node frozen (GC pause, `docker pause`) | Same as a kill | Node stops itself on wake-up; sink and database reject its old token; its expired lease can't be renewed. A session frozen inside a transaction is ended after 5 s |
| Node lost, partitions orphaned | New due runs in those partitions wait up to about 7 s (p99 > 2 s in that window) | Partition leases (6 s) expire, the expired owner is treated as dead at once, live nodes take them over |
| Renewal and slot reuse at the same moment | Nothing | Both update the same run row; exactly one succeeds |
| Node joins | Nothing | Its share of partitions is released and picked up |
| PostgreSQL unreachable | No jobs fire; API returns `503 SCHEDULER_STORE_UNAVAILABLE` | Nodes stop starting work; misfire policies handle the backlog once the database returns |
| PostgreSQL primary fails (production) | Writes pause about 10–30 s; nothing acknowledged is lost | Patroni promotes only the synchronous standby; the new timeline raises the token epoch |
| No synchronous standby available | Writes block; no jobs fire | Strict synchronous mode; resumes when a standby returns |
| Commit outcome unknown after failover | API client gets an error | Client retries with the same `Idempotency-Key` and gets one result |
| Cluster rebuilt from a fresh `initdb` | Nodes refuse to start | Operator raises `cluster_generation` |
| Admin lowers a cap below the running count | New starts for that tenant wait | Deferred reduction with compaction; running stays ≤ effective cap |
| Notification sink down or slow | Alerts delayed, retry count rises | Retries with backoff, then dead-letter; operator replays; caps contain the impact to one tenant |
| Late or out-of-order messages | Nothing | Every write is conditional on token and state |
| Double-clicked manual trigger | One run | `Idempotency-Key` header maps to a unique run key |
| Clock skew on a node | Nothing | Due times and leases use the database clock |
| Bad payload | Run in the dead-letter list with `INVALID_PAYLOAD` | Dead-lettered at once |

### Failover requirements (production)

We rely on Patroni with etcd for database failover and treat these as preconditions ([`deploy/patroni/README.md`](deploy/patroni/README.md) maps each one to a setting):

| # | Requirement | How it is met |
|---|---|---|
| F1 | Every acknowledged commit is on at least one standby | `synchronous_commit = on` |
| F2 | The primary never falls back to asynchronous replication | Patroni `synchronous_mode: true` + `synchronous_mode_strict: true`; with no synchronous standby, writes block |
| F3 | Only a replica holding every acknowledged commit can be promoted | 1 primary + 2 standbys with `FIRST 1`; Patroni promotes only the standby it has recorded as synchronous. In quorum mode (`ANY k of n`), promotion must reach n − k + 1 standbys and pick the highest flushed WAL position, or refuse. |
| F4 | The old primary can't accept writes after a promotion | Patroni leader lease + watchdog; the JDBC URL uses `targetServerType=primary` (pgjdbc) and `target_session_attrs=read-write` (libpq tools) |
| F5 | Fencing epochs strictly increase | New timeline on every promotion + `cluster_generation` on rebuilds + startup check against the sink |

---

## Trade-offs

- **Consistency over availability.** If the database is unreachable, nothing fires. Firing from local state could fire twice.
- **Latency vs database load.** A 200 ms poll keeps lag around 100 ms. Each poll runs one claim transaction per tenant with due runs, and each finished run triggers one more, so claim load grows with tenants and throughput. LISTEN/NOTIFY could cut lag further later.
- **PostgreSQL vs etcd, ZooKeeper or Kafka.** One dependency, transactional state and a single trusted clock, with a write ceiling (see [Where it stops scaling](#capacity-bounds-and-where-it-stops-scaling)).
- **Hard caps vs throughput.** Caps leave capacity idle during single-tenant bursts, in exchange for isolation between tenants.
- **Lease length.** A 10 s lease gives a 10.7 s takeover. A shorter one would take over faster but renew more often and cause more false takeovers during GC pauses; a 20 s pause costs an extra attempt, which fencing makes safe.
- **Partition lease length.** The 6 s partition lease is what new runs in a dead node's partitions wait for (the benchmark's post-chaos p99 window). Shorter means more lease writes.
- **Exact caps vs write cost.** Tenant slots enforce the cap exactly, at the cost of one slot-row lock per run start. Per-node shares would be cheaper but could exceed the cap while partitions move.
- **Strict lease expiry.** A write after the lease expired is rejected even if nobody has taken over, which costs an occasional rerun (deduplicated) and makes slot reuse race-free.
- **Durability vs availability on failover.** Strict synchronous replication blocks writes when no standby is available, and a promotion is refused rather than lose acknowledged jobs. Each commit pays about one LAN round trip.
- **Deferred cap reduction.** Lowering a cap doesn't stop running jobs; new starts wait until the running count drops below the new cap.
- **In-process cache (Caffeine) vs Redis.** See [Caching](#caching): nothing correctness-critical is cached, so a shared cache would add a dependency without protecting anything.

**Left out on purpose:** multi-region scheduling, exactly-once effects against APIs that ignore idempotency keys, job dependencies and DAGs, fixed-rate schedules, sub-second precision, an admin UI, real SMS and push providers, a full OAuth2 identity provider, and our own failover manager (Patroni is relied on).

---

## Assumptions

- A mock notification sink stands in for real SMS and push providers.
- Jobs need precision in seconds, not milliseconds.
- Tenant time zones are valid IANA region IDs (fixed offsets are rejected).
- The PostgreSQL primary's clock never steps backwards (NTP in slew mode), and database nodes stay within about 2 s of each other.
- In production, a single failover manager (Patroni + etcd) meets F1–F5 and prevents split brain. The single-node demo doesn't claim to survive a database failover.
- A frozen process can run for a moment after it wakes. The guarantees cover valid leases and effects, not what a paused process physically does.

---

## Known limitations and what was not verified locally

The build machine has no Docker. Everything below was written and checked as far as possible without it:

- **`docker compose up --build` has not been run.** The compose file is checked by `ComposeConfigTest` (services, profiles, toxiproxy wiring, Prometheus and Grafana mounts) and the Dockerfiles follow the module layout, but the stack has never been started. Expect small fixes on the first real run.
- **Docker-gated tests skip locally.** `FailoverFencingIT` and `StrictSyncIT` (Testcontainers) run only where Docker exists, such as CI.
- **Chaos scripts** (`chaos/*.sh`) have only been checked with `bash -n`.
- **Patroni** (`deploy/patroni/`): YAML parsed and scripts syntax-checked; the images were never built and the cluster never started.
- **The compose-mode load generator** (`LoadGenMain`) was never run against a live stack; the benchmark ran in-process.
- **Benchmark scope.** All numbers come from one laptop with every process on it; the scaling bottleneck was not isolated. The 2 s p99 target is missed for a few seconds in a dead or frozen node's partitions (above).
- **Max run time.** A handler that ignores the interrupt at `maxRunTime` is abandoned, not killed (Java cannot stop a thread): the hard stop releases its run, slot and lease, but the thread keeps one worker of the node until it returns.
- **Retention.** Finished runs are purged after 7 days (`RunPurge`, `scheduler.run-purge.*`) and `api_idempotency` after 24 h, but dead-lettered runs are kept until someone replays them, and the sink's `sink.delivery` (the dedup record) has no retention job.
- No list-jobs endpoint, no `GET /tenants/{t}`, no early lease revocation on cap reduction, no Postgres row-level security (tenant isolation is enforced in every repository query).

---

## Repository layout

```
├─ docker-compose.yml   postgres, 3 scheduler nodes, notification-sink, toxiproxy, prometheus, grafana
├─ scheduler-core/      Plain Java, no Spring, no database: domain types (FencingToken, RunState …), cron and
│                       DST, misfire and backoff policies, partition assignment, fairness, lease clock, ports
├─ scheduler-app/       Spring Boot: api, service, repository (JdbcClaimProtocol …), engine (SchedulerNode,
│                       Poller, Dispatcher, OrphanSweep …), handler, sink client, security, observability, error
├─ notification-sink/   Mock provider that enforces idempotency keys and fencing tokens (own `sink` schema)
├─ loadgen/             LoadGenMain (compose mode); test scope: InProcessBench and the end-to-end ITs
├─ chaos/               kill-during-alerts.sh, freeze-during-alerts.sh, shared lib.sh
├─ deploy/patroni/      Production failover config (F1–F5)
├─ ops/                 Prometheus scrape config and 13 alert rules, Grafana dashboard, Toxiproxy proxies
├─ docs/                DESIGN.md, BENCHMARK.md, RUNBOOK.md, DEMO.md, CONTRACTS.md, sink-api.md, postman/
├─ PLAN.md              The design plan
└─ .github/workflows/   CI: ./mvnw -B verify on JDK 21
```

The core algorithms sit behind ports (`ClaimProtocol`, `PartitionLeaseStore`, `MembershipStore`, `EffectSink`, `SchedulerClock`), so they are tested with in-memory fakes and run in production against PostgreSQL.

---

## Design review fixes

A review of the first design found four problems, all fixed above:

| # | Issue | Fix |
|---|---|---|
| 1 | Takeover could take about 21 s, because the reaper only scanned owned partitions | Orphan sweep in any partition (now every 500 ms), faster partition leases, recovered runs first |
| 2 | Per-node cap shares could exceed the tenant cap while partitions moved | Tenant slot table enforced by foreign key, CHECK and unique index; deferred cap reduction |
| 3 | A slot could be reused while its holder renewed the lease | Expired leases can't be renewed; conditional reap before slot reuse; `clock_timestamp()`; fixed lock order |
| 4 | Tokens could go backwards after an async failover | `(epoch, seq)` tokens with a timeline-based epoch; synchronous replication with promotion rules F1–F5 |

---

## Documents

- [`docs/DESIGN.md`](docs/DESIGN.md): architecture diagram, claim → execute → fenced commit → takeover sequence, the exactly-once argument, failure-mode table with tests
- [`docs/BENCHMARK.md`](docs/BENCHMARK.md): full benchmark method, machine, results and caveats
- [`docs/DEMO.md`](docs/DEMO.md): demo video script with exact commands
- [`docs/RUNBOOK.md`](docs/RUNBOOK.md): on-call runbook, one section per alert
- [`docs/sink-api.md`](docs/sink-api.md): notification sink HTTP API
- [`docs/CONTRACTS.md`](docs/CONTRACTS.md): team ownership, shared contracts, configuration keys
- [`docs/postman/`](docs/postman/): Postman collection and environment
- [`PLAN.md`](PLAN.md): the design plan (also published at https://claude.ai/artifact/JuMnTBHToDBjXiXkPYSVz3)
- [`chaos/README.md`](chaos/README.md), [`deploy/patroni/README.md`](deploy/patroni/README.md)
- Demo video: to be recorded from [`docs/DEMO.md`](docs/DEMO.md)
