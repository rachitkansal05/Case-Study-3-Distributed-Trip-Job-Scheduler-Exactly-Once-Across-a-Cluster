# Distributed Trip Job Scheduler — Implementation Plan

> MoveInSync · Case Study 3 · Implementation plan

A Java 21 / Spring Boot scheduler that runs rosters, pickup alerts, no-show cancels and billing close across three or more nodes. Each side effect happens once even when a node is killed or frozen. Postgres is the only coordinator, and no single row is a hot lock.

| 100,000 | 3 nodes | < 2 s | 128 | 10 s |
|---|---|---|---|---|
| jobs due in a 10-min window | minimum cluster | p99 fire lag target | fixed job partitions | run lease, renewed every 3 s |

## Contents

1. [Hard rules](#01-hard-rules-pulled-from-the-brief)
2. [Guarantees](#02-what-we-guarantee-and-why-exactly-once-execution-is-impossible)
3. [Stack](#03-technology-choices)
4. [Architecture](#04-architecture)
5. [Data model](#05-data-model)
6. [Partitions & leases](#06-partitioning-and-membership-no-single-hot-lock)
7. [Claim, fence, commit](#07-claim-fence-execute-commit)
8. [Cron & DST](#08-recurring-jobs-time-zones-and-dst)
9. [Misfire & retries](#09-misfires-retries-and-the-dead-letter-list)
10. [Fairness numbers](#10-per-tenant-fairness-with-numbers)
11. [Freeze sequence](#11-hardest-flow-freeze-takeover-stale-write-rejected)
12. [Failure modes](#12-failure-mode-table)
13. [API & auth](#13-api-and-access-control)
14. [Metrics, cache, errors](#14-metrics-logging-caching-and-errors)
15. [Code layout](#15-code-layout-and-oop-design)
16. [Test matrix](#16-test-matrix-every-hard-rule-is-automated)
17. [Benchmark](#17-benchmark-plan-and-capacity-math)
18. [Trade-offs](#18-trade-offs-and-deliberate-omissions)
19. [Roadmap](#19-delivery-roadmap)
20. [Demo script](#20-demo-video-script-about-6-minutes)

---

## 01. Hard rules pulled from the brief

Every rule gets an ID. Each ID appears again in the [test matrix](#16-test-matrix-every-hard-rule-is-automated), so every hard rule has at least one automated test.

| ID | Rule | Where it is solved |
|---|---|---|
| **R1** | One-off jobs (run at an instant) and recurring jobs (cron), each with tenant, payload, priority, max run time | `job` table, `JobDefinition` hierarchy |
| **R2** | Recurring jobs run in tenant time zone, including DST gap (02:30 on spring-forward) and overlap (01:30 twice) | `WallClockResolver` (§08) |
| **R3** | At-least-once execution with an idempotency key per run, so the side effect happens exactly once | Run key + item key, idempotent sink (§07) |
| **R4** | Time-limited lease plus fencing token; a node waking after lease expiry cannot commit | Conditional commit on token, sink-side token check, self-fencing (§07) |
| **R5** | No single hot lock; partitions move when nodes join or leave | 128 partition leases + rendezvous hashing (§06) |
| **R6** | Misfire policy per job: fire now, skip, coalesce | `MisfirePolicy` sealed interface (§09) |
| **R7** | Per-tenant concurrency cap; one tenant's 50,000 alerts must not delay another's 20 | Per-tenant claim + deficit round robin (§10) |
| **R8** | Exponential backoff with jitter, then dead-letter list an operator can inspect and replay | `BackoffPolicy`, `DEAD_LETTERED` state, DLQ API |
| **R9** | Pause/resume a tenant, list next fire times, manual trigger | Ops API (§13) |
| **R10** | Fire-lag histogram; running, retrying and dead-lettered counts | Micrometer + Prometheus (§14) |
| **R11** | kill -9 while sending 1,200 alerts: takeover within lease time, every employee gets one alert | Chunked handler + checkpoint + chaos test |
| **R12** | SIGSTOP for 30 s: lease expires, another node finishes, the stale write is rejected | Fencing + `docker pause` chaos test (§11) |
| **R13** | 100k jobs in 10 min on 3 nodes with p99 fire lag < 2 s | Load generator + HdrHistogram report (§17) |
| **R14** | RBAC (employee / admin / operator) with tenant isolation; no PII in logs or errors | JWT + repository tenant scoping + log masking |
| **R15** | One-command run, README with benchmark and machine, design note, demo video, meaningful commits | `docker compose up`, `docs/`, roadmap (§19) |

---

## 02. What we guarantee, and why exactly-once execution is impossible

A node can do the work (send the SMS) and then die before it records that the work happened. To everyone else that looks the same as a node that died before sending. A node in a 30-second GC pause also looks the same as a dead node. No protocol can tell these cases apart in an asynchronous network with crash failures (the FLP result and the Two Generals problem). The scheduler has two choices: retry and maybe repeat the work (at-least-once), or never retry and maybe lose it (at-most-once). Losing jobs is not allowed, so we retry.

> **The guarantee:** each scheduled occurrence is *executed* at least once and *takes effect* exactly once. Every effect carries a deterministic idempotency key, and the receiving side stores it with a unique constraint. Every write that changes state carries a fencing token, and both the scheduler DB and the sink reject writes with an outdated token.
>
> **The assumption:** each downstream system either honours idempotency keys (our notification sink does) or is wrapped by a dedupe table we control. A non-idempotent third-party API cannot be made exactly-once by any scheduler, and the README says so.

### Which clock decides when a job is due

- **The Postgres primary's clock (`now()`) is the only clock that decides due times and lease expiry.** Nodes never compare their own wall clock with `due_at` or `lease_until`. Every due check and lease check is a SQL predicate evaluated in the database.
- A node whose clock is skewed by 5 minutes therefore fires on time. Skew only changes log timestamps, and logs also record the DB timestamp.
- Each node uses its own **monotonic clock** (`System.nanoTime`) for one thing: self-fencing. If it has not renewed a lease within `lease − 2 s` of local elapsed time, it stops starting new side effects. CLOCK_MONOTONIC keeps advancing while a process is SIGSTOPped, so a frozen node sees on wake-up that its deadline has passed.
- Fire lag is measured as `started_at(DB) − scheduled_at`, both on the DB clock.

---

## 03. Technology choices

| Concern | Choice | Why |
|---|---|---|
| Language / runtime | Java 21, Spring Boot 3.3 | Preferred by the brief; records and sealed interfaces fit the domain; virtual threads for blocking I/O in handlers |
| Coordinator + store | PostgreSQL 16 | `FOR UPDATE SKIP LOCKED`, sequences for fencing tokens, a transactional state machine, and a single trusted clock. A ZooKeeper or Kafka cluster would add more to run without fixing anything Postgres cannot handle at this scale. |
| Data access | Spring `JdbcClient` + Flyway | Claim and fence SQL must be exact; JPA hides the conditional updates we depend on |
| Cron | Spring `CronExpression` on `LocalDateTime` + own DST resolver | Cron is matched on wall time, and our own code turns wall time into an instant, so DST policy is explicit and tested |
| Cache | Caffeine | In-process, size and TTL eviction, metrics built in |
| Security | Spring Security resource server, HS256 JWT | Roles and tenant claim in the token; dev token endpoint for the demo |
| Metrics / logs | Micrometer → Prometheus → Grafana; logstash JSON encoder | Histogram with SLO buckets; MDC fields for run, job, tenant, node, token |
| Tests | JUnit 5, AssertJ, Testcontainers, Awaitility, Toxiproxy | Real Postgres in tests; network faults injected on demand |
| Load / chaos | Java load generator + HdrHistogram; shell chaos scripts (`docker kill -s KILL`, `docker pause`) | Reproducible R11–R13 runs that write numbers into the README |

---

## 04. Architecture

Every node runs the same jar, so there is no leader. Each node runs five loops. The **membership** loop heartbeats. The **partition** loop acquires and renews partition leases. The **poller** claims due runs from owned partitions. The **dispatcher** runs them under per-tenant caps. The **reaper** reclaims runs whose lease expired. A separate notification sink stands in for the real SMS/push provider and enforces idempotency keys and fencing tokens, which is what the tests check.

```mermaid
flowchart LR
  subgraph Clients
    A1[Admin / Operator<br/>REST + JWT]
    PR[Prometheus / Grafana]
  end
  subgraph Cluster["Scheduler cluster (N >= 3, identical nodes)"]
    direction TB
    N1["Node 1<br/>membership · partition leases<br/>poller · dispatcher · reaper"]
    N2["Node 2"]
    N3["Node 3"]
  end
  DB[("PostgreSQL 16<br/>job · job_run · partition_lease<br/>node · tenant · dead_letter<br/>clock = now()")]
  SINK["Notification sink<br/>(mock provider)<br/>UNIQUE idem_key<br/>max fencing token per run"]
  A1 -->|create / pause / trigger / replay| N1
  A1 --> N2
  N1 <-->|claim SKIP LOCKED<br/>fenced commits| DB
  N2 <--> DB
  N3 <--> DB
  N1 -->|effect + idem key + token| SINK
  N2 --> SINK
  N3 --> SINK
  PR -.->|scrape /actuator/prometheus| N1
  PR -.-> N2
  PR -.-> N3
```

### Run state machine

```mermaid
stateDiagram-v2
  [*] --> SCHEDULED: materialised (job_id, scheduled_at) unique
  SCHEDULED --> RUNNING: claim (token = nextval)
  SCHEDULED --> SKIPPED: misfire SKIP / COALESCE absorbed
  SCHEDULED --> PAUSED: tenant paused
  PAUSED --> SCHEDULED: resume (misfire policy applied)
  RUNNING --> SUCCEEDED: commit WHERE token matches
  RUNNING --> RETRY_WAIT: failure, attempts left
  RUNNING --> RETRY_WAIT: lease expired (reaper)
  RETRY_WAIT --> RUNNING: due again, new token
  RUNNING --> DEAD_LETTERED: attempts exhausted
  DEAD_LETTERED --> SCHEDULED: operator replay (same idem key)
  SUCCEEDED --> [*]
```

---

## 05. Data model

```sql
CREATE TABLE tenant (
  id            TEXT PRIMARY KEY,
  time_zone     TEXT NOT NULL,              -- e.g. 'Asia/Kolkata', 'America/New_York'
  paused        BOOLEAN NOT NULL DEFAULT false,
  max_concurrency INT NOT NULL DEFAULT 32,  -- cluster-wide cap (R7)
  version       BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE job (
  id            UUID PRIMARY KEY,
  tenant_id     TEXT NOT NULL REFERENCES tenant(id),
  kind          TEXT NOT NULL,              -- ONE_OFF | RECURRING
  handler       TEXT NOT NULL,              -- PICKUP_ALERT | ROSTER_GEN | NO_SHOW_CANCEL | BILLING_CLOSE
  cron          TEXT,                       -- recurring only, wall-clock in tenant zone
  run_at        TIMESTAMPTZ,                -- one-off only
  payload       JSONB NOT NULL,             -- may hold PII; never logged
  priority      SMALLINT NOT NULL DEFAULT 5,
  max_run_time  INTERVAL NOT NULL,
  misfire       TEXT NOT NULL,              -- FIRE_NOW | SKIP | COALESCE
  misfire_threshold INTERVAL NOT NULL DEFAULT '60 seconds',
  max_attempts  SMALLINT NOT NULL DEFAULT 5,
  partition_id  SMALLINT NOT NULL,          -- murmur3(id) % 128
  next_fire_at  TIMESTAMPTZ,                -- materialiser cursor
  version       BIGINT NOT NULL DEFAULT 0,  -- definition version (cache key)
  enabled       BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE job_run (
  id            BIGSERIAL PRIMARY KEY,
  job_id        UUID NOT NULL REFERENCES job(id),
  tenant_id     TEXT NOT NULL,
  partition_id  SMALLINT NOT NULL,
  scheduled_at  TIMESTAMPTZ NOT NULL,       -- the occurrence instant
  due_at        TIMESTAMPTZ NOT NULL,       -- scheduled_at, or retry time
  idem_key      TEXT NOT NULL,              -- jobId:scheduledAtUTC | jobId:manual:requestId
  state         TEXT NOT NULL,
  attempt       SMALLINT NOT NULL DEFAULT 0,
  fencing_token BIGINT,                     -- from fence_seq on every claim
  owner_node    TEXT,
  lease_until   TIMESTAMPTZ,
  started_at    TIMESTAMPTZ,
  finished_at   TIMESTAMPTZ,
  checkpoint    JSONB,                      -- e.g. {"nextItem": 650} for chunked jobs
  coalesced_count INT NOT NULL DEFAULT 1,
  last_error    TEXT,                       -- error code + sanitised message only
  UNIQUE (job_id, scheduled_at),            -- materialisation is idempotent
  UNIQUE (idem_key)
);
CREATE SEQUENCE fence_seq CACHE 1;          -- strictly increasing tokens, no row lock

CREATE INDEX run_due   ON job_run (partition_id, tenant_id, due_at) WHERE state IN ('SCHEDULED','RETRY_WAIT');
CREATE INDEX run_lease ON job_run (partition_id, lease_until)       WHERE state = 'RUNNING';

CREATE TABLE partition_lease (
  partition_id  SMALLINT PRIMARY KEY,       -- 128 rows, one per partition
  owner_node    TEXT,
  epoch         BIGINT NOT NULL DEFAULT 0,
  lease_until   TIMESTAMPTZ
);

CREATE TABLE scheduler_node (
  node_id       TEXT PRIMARY KEY,
  heartbeat_at  TIMESTAMPTZ NOT NULL,       -- DB clock
  started_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE dead_letter (
  run_id        BIGINT PRIMARY KEY REFERENCES job_run(id),
  tenant_id     TEXT NOT NULL,
  error_code    TEXT NOT NULL,
  attempts      SMALLINT NOT NULL,
  dead_at       TIMESTAMPTZ NOT NULL,
  replayed_at   TIMESTAMPTZ
);

-- Notification sink (separate schema, owned by the sink service)
CREATE TABLE sink.delivery   (idem_key TEXT PRIMARY KEY, run_id BIGINT, fencing_token BIGINT, delivered_at TIMESTAMPTZ);
CREATE TABLE sink.run_fence  (run_id BIGINT PRIMARY KEY, max_token BIGINT NOT NULL);
CREATE TABLE sink.attempt_log(id BIGSERIAL, idem_key TEXT, fencing_token BIGINT, outcome TEXT, at TIMESTAMPTZ);
```

The sink keeps an `attempt_log` so the tests can show both sides: duplicates *were attempted* (at-least-once) and only one *was delivered* per key (exactly-once effect).

---

## 06. Partitioning and membership (no single hot lock)

1. **Fixed partitions.** `partition_id = murmur3(job_id) mod 128`, set at creation and never changed. Runs inherit it. 128 allows growth to about 40 nodes while keeping each partition small.
2. **Heartbeat.** Each node upserts `scheduler_node.heartbeat_at = now()` every 2 s. A node is *live* if its heartbeat is less than 10 s old by DB time. Only one row per node is written, so nodes do not contend.
3. **Desired assignment.** Each node reads the live set and computes rendezvous (highest-random-weight) hashing: `owner(p) = argmax_n hash(n, p)`. Every node computes the same answer without talking to the others. When a node joins or leaves, only about `128/N` partitions move.
4. **Acquire / renew** (one row per partition, so contention is per row and spread across 128 rows):

   ```sql
   UPDATE partition_lease
      SET owner_node = :me, epoch = epoch + 1, lease_until = now() + interval '15 s'
    WHERE partition_id = :p
      AND (owner_node = :me OR lease_until < now());
   ```

   Renew every 5 s. Release partitions that are no longer desired (set `lease_until = now()`) so the new owner picks them up immediately. On a crash, they move after at most 15 s.
5. **Correctness never depends on partition ownership.** Partition leases only spread polling load. If two nodes briefly poll the same partition during a handover, `SKIP LOCKED` and the run-level fencing token still allow only one claim per run. Ownership gaps can only add lag, never duplicates.

> **Why this beats one lock row:** Quartz-style clustering serialises every trigger acquisition through one `QRTZ_LOCKS` row, so throughput is capped by lock hand-off latency (about 1–3 ms), roughly 300–1,000 acquisitions/s shared by all nodes. Here, claims from different nodes touch different partitions and different run rows. The fencing sequence is non-transactional and takes no row lock.

---

## 07. Claim, fence, execute, commit

### Claim (every 200 ms, per node)

The claim is *per tenant*, bounded by the free slots this node has for that tenant. A burst from one tenant therefore cannot fill the batch (R7), and nothing is claimed that cannot start right away, so leases are never held by jobs waiting in a queue.

```sql
WITH t AS (SELECT id FROM tenant WHERE NOT paused AND id = ANY(:tenantsWithFreeSlots))
UPDATE job_run r
   SET state = 'RUNNING', fencing_token = nextval('fence_seq'), owner_node = :me,
       lease_until = now() + interval '10 s', started_at = coalesce(r.started_at, now()),
       attempt = r.attempt + 1
  FROM (SELECT c.id FROM t, LATERAL (
          SELECT id FROM job_run
           WHERE partition_id = ANY(:owned) AND tenant_id = t.id
             AND state IN ('SCHEDULED','RETRY_WAIT') AND due_at <= now()
           ORDER BY priority_rank, due_at
           LIMIT :freeSlotsFor(t.id)            -- bounded per tenant
           FOR UPDATE SKIP LOCKED) c) picked
 WHERE r.id = picked.id
RETURNING r.id, r.job_id, r.fencing_token, r.idem_key, r.checkpoint, r.scheduled_at, now() AS db_now;
```

### Execute

- The handler receives a `RunContext(runId, idemKey, fencingToken, checkpoint, deadline)`. Every outbound effect sends `Idempotency-Key` and `Fencing-Token` headers.
- A lease renewer extends `lease_until` every 3 s with `WHERE id = :run AND fencing_token = :token`. If the update touches 0 rows, the node has lost ownership: it interrupts the handler and stops.
- **Chunked handlers** (1,200 pickup alerts) send in chunks of 50 and checkpoint after each chunk, using the same fenced predicate. Item key: `{idemKey}:{employeeId}`.
- A hard stop at `max_run_time` marks the attempt failed (timeout), and normal retry applies.

### Commit (fenced)

```sql
UPDATE job_run SET state = 'SUCCEEDED', finished_at = now()
 WHERE id = :run AND fencing_token = :token AND state = 'RUNNING';
-- 0 rows  =>  StaleFencingTokenException, metric scheduler_stale_commit_rejected_total++
```

### Three layers stop a stale node

| Layer | How it works |
|---|---|
| **1 · Self-fencing (local)** | Monotonic deadline `claimTime + lease − 2 s`. On wake-up after SIGSTOP the check fails before the next send. This is cheap, but it is not enough alone: the pause can land between the check and the send. |
| **2 · Sink fencing (effect)** | The sink keeps `max_token` per run and rejects any request with a lower token (409). The unique `idem_key` absorbs legitimate retries. This layer makes the side effect safe. |
| **3 · DB fencing (state)** | Every checkpoint, renew and commit is conditional on `fencing_token`. A stale node cannot mark a run done, move a checkpoint backward, or overwrite the new owner's result. |
| **Reaper** | Every 1 s per owned partition: `RUNNING AND lease_until < now()` → `RETRY_WAIT, due_at = now()`. The next claim issues a higher token and resumes from the checkpoint. Takeover time ≤ 10 s lease + 1 s reaper + 0.2 s poll. |

---

## 08. Recurring jobs, time zones and DST

Cron expressions are evaluated against **local wall time** in the tenant zone. The `WallClockResolver` then maps each local time to exactly one instant. A materialiser fills `job_run` up to 15 minutes ahead (idempotent thanks to `UNIQUE(job_id, scheduled_at)`) and advances `job.next_fire_at`.

| Case | Example (America/New_York) | Our rule | Fires at (UTC) |
|---|---|---|---|
| **Gap** (spring forward) | Sun 8 Mar 2026: 02:00 → 03:00. Job `30 2 * * *` | The skipped wall time fires once, **at the instant the gap ends (03:00 local)**, like Vixie cron. This is deliberately *not* Java's default shift to 03:30, because a nightly job should not move later than needed. | 07:00Z |
| **Overlap** (fall back) | Sun 1 Nov 2026: 01:00–01:59 occurs twice. Job `30 1 * * *` | Fires **once, at the first occurrence** (EDT, −04:00). The idempotency key is built from the resolved instant, and only one instant is ever produced per local occurrence. | 05:30Z |
| Overlap, sub-hourly job | `*/15 * * * *` | Wall-clock matching would skip the second 01:xx hour. We document this: such jobs fire for each distinct local time once, so there are 4 runs, not 8, in the repeated hour. Teams that need elapsed-time spacing use a fixed-rate job (left out, see §18). | — |
| No-DST zone | Asia/Kolkata, 22:00 roster | Straight mapping, 16:30Z every day. | 16:30Z |
| Tenant changes zone | Admin edits `time_zone` | Bump the job version; delete unstarted future runs and re-materialise them. Runs already started are not touched. | — |

Unit tests pin these exact dates and also run against Europe/London (last Sundays of March and October) to show the rule does not depend on a particular zone.

---

## 09. Misfires, retries and the dead-letter list

A run has *misfired* when it is claimed with `now() − scheduled_at > misfire_threshold` (default 60 s), usually after cluster downtime or a long pause of a tenant. The policy is a sealed interface with three implementations, applied when the poller claims:

| Policy | Behaviour after 3 h downtime, hourly job (3 missed) | Good for |
|---|---|---|
| `FIRE_NOW` | All 3 missed runs fire immediately, in order, each with its own key | Billing close per day: each period must be closed |
| `SKIP` | Missed runs → `SKIPPED`; the next future occurrence runs normally | "Cab assigned" alerts: an alert after pickup time is useless |
| `COALESCE` | Newest missed run fires once with `coalesced_count = 3`; the other two → `SKIPPED` | Roster regeneration: only the latest state matters |

### Retries

- Backoff: `delay = random(0, min(cap, base · 2^attempt))` ("full jitter"), base 2 s, cap 5 min. Retry sets `state = RETRY_WAIT, due_at = now() + delay`, so it waits in the same index and needs no extra queue.
- Errors are classified by type: `RetryableJobException` (timeouts, 5xx) is retried; `PermanentJobException` (bad payload, 4xx) is dead-lettered immediately.
- After `max_attempts`, the run becomes `DEAD_LETTERED` and a row is written to `dead_letter`. Operators can list, filter and inspect error codes (never payload PII), and replay. Replay keeps the **same idempotency key**, so any items that were delivered before the run died are not sent again.

---

## 10. Per-tenant fairness, with numbers

Two mechanisms work together. **Claim-time caps**: a node only claims up to its free slots per tenant. Its share of a tenant's cap is `ceil(cap × owned_partitions / 128)`, recomputed whenever partitions move. **Dispatch-time deficit round robin**: claimed runs go into per-tenant queues, and the worker pool takes from tenants in turn, weighted by priority, so a small tenant is never stuck behind a deep queue.

| Scenario at 08:00:00 | Without caps (global FIFO) | With caps (cap 32 / tenant) |
|---|---|---|
| Cluster capacity | 3 nodes × 64 workers = 192 slots; alert send ≈ 40 ms → 4,800 runs/s | (same) |
| Tenant A: 50,000 alerts | drains in 50,000 / 4,800 ≈ 10.4 s | 32 / 0.04 s = 800/s → 62.5 s |
| Tenant B: 20 alerts | queued behind A: up to 10.4 s lag ❌ **fails** | starts on the next 200 ms poll: lag < 0.3 s ✅ **passes** |
| Idle capacity left for others | 0 slots | 160 slots |

> **Trade-off made visible:** a hard cap leaves capacity idle while tenant A waits. Alerts go out 30 minutes before pickup, so a 62 s drain is fine. The cap is configurable per tenant (A could get 96, which drains in about 21 s). A cap is not work-conserving; we pick isolation over peak throughput because the brief asks for it. The README includes the measured version of this table.

**Rebalance edge case:** while partitions move, two nodes may briefly each count shares for the same partitions. The cluster-wide cap can be exceeded by at most one node's share (about 11 slots) for up to one lease period (15 s). This is documented and covered by a test.

---

## 11. Hardest flow: freeze, takeover, stale write rejected

```mermaid
sequenceDiagram
  autonumber
  participant A as Node A
  participant DB as Postgres (clock)
  participant B as Node B
  participant S as Notification sink
  A->>DB: claim run 77 (token 41, lease_until = t+10s)
  A->>S: send items 1..600 (key 77:emp, token 41)
  S-->>A: 201 delivered
  A->>DB: checkpoint next=601 WHERE token=41 (1 row)
  Note over A: SIGSTOP (docker pause) for 30 s
  Note over DB: t+10s: lease expires
  B->>DB: reaper: RUNNING and lease_until < now()  →  RETRY_WAIT
  B->>DB: claim run 77 (token 42, checkpoint 601)
  B->>S: send items 601..1200 (token 42)
  S-->>B: 201 delivered (run_fence.max_token = 42)
  B->>DB: commit SUCCEEDED WHERE token=42 (1 row)
  Note over A: resumes after 30 s
  A->>A: self-fence: monotonic deadline passed → stop
  A->>S: (if a send was already in flight) item 601, token 41
  S-->>A: 409 stale token, not delivered
  A->>DB: commit WHERE token=41
  DB-->>A: 0 rows → StaleFencingTokenException
  Note over S: 1,200 distinct deliveries, 0 duplicates
```

The **kill -9** case is the same flow without steps 15–18. Items sent after the last checkpoint and before the crash (up to 49) are resent by B and absorbed by the sink's unique `idem_key`.

---

## 12. Failure-mode table

| What fails | What the user sees | How the system recovers |
|---|---|---|
| Node killed (kill -9) mid-run | Alerts for that run pause ≤ 11 s, then continue; nobody gets two | Run lease expires → reaper → new claim with higher token → resume from checkpoint; sink dedups the replayed chunk |
| Node frozen (GC pause, SIGSTOP) | Same as kill; on resume nothing changes | Self-fence + sink token check + fenced DB commit (§11) |
| Node crash, partitions orphaned | Lag bump ≤ 15 s for jobs in those partitions | Partition leases expire; rendezvous hashing gives them to live nodes |
| Node joins | Nothing visible | About 128/N partitions handed over through release and acquire; no gaps, because the old owner releases only after the new owner is live |
| Postgres unreachable | No jobs fire; API returns 503 `SCHEDULER_STORE_UNAVAILABLE` | Nodes self-fence (cannot renew), stop starting effects, and retry with backoff. When the DB returns, the misfire policy handles the backlog. We choose consistency over availability. |
| Postgres failover to replica | Up to about 30 s pause; clock may step slightly | Leases use a 2 s safety margin; idempotency keys cover any repeated effect |
| Sink slow / timing out | Alerts delayed; retry counter rises | Handler timeout → retryable → jittered backoff → DLQ after 5 attempts; per-tenant cap limits the damage to that tenant |
| Sink returns a duplicate (already delivered) | Nothing | Handler treats 200 `ALREADY_DELIVERED` as success |
| Out-of-order / late messages (stale renew, old checkpoint) | Nothing | All writes are conditional on token and state; the sink rejects lower tokens |
| Duplicate manual trigger (double click) | One run created; the second call returns the same run id | Client `Idempotency-Key` header → `idem_key = jobId:manual:{key}` unique |
| Clock skew on a node | Nothing (only node log timestamps are off) | Due and lease checks run on the DB clock |
| Poison payload | Run in DLQ with error code `INVALID_PAYLOAD` | Permanent failure → immediate dead-letter; operator fixes the job and replays |
| Tenant floods 50k jobs | That tenant drains at its cap; others unaffected | Claim-time cap + DRR (§10) |

---

## 13. API and access control

| Method · Path | Purpose | Roles |
|---|---|---|
| `POST /api/v1/jobs` | Create a one-off or recurring job | ADMIN |
| `GET /api/v1/jobs/{id}` | Job definition (payload redacted for EMPLOYEE) | ADMIN, EMPLOYEE (own tenant) |
| `GET /api/v1/jobs/{id}/next-fire-times?count=10` | Next N instants, with local time and offset | ADMIN, EMPLOYEE |
| `POST /api/v1/jobs/{id}/trigger` | Manual run now (needs `Idempotency-Key` header) | ADMIN, OPERATOR |
| `DELETE /api/v1/jobs/{id}` | Disable job; cancel unstarted runs | ADMIN |
| `GET /api/v1/jobs/{id}/runs` | Run history: state, attempt, lag, error code | ADMIN, OPERATOR |
| `POST /api/v1/tenants/{t}/pause` · `/resume` | Pause or resume all of a tenant's jobs | ADMIN (own), OPERATOR (any) |
| `GET /api/v1/dead-letters?tenant=` | Inspect the DLQ | OPERATOR, ADMIN (own) |
| `POST /api/v1/dead-letters/{runId}/replay` | Replay with the same idempotency key | OPERATOR |
| `GET /api/v1/cluster` | Live nodes, partitions per node, lease epochs | OPERATOR |
| `POST /dev/token` | Mint a demo JWT (dev profile only) | — |

- **EMPLOYEE** can read schedules for their tenant. **ADMIN** manages one tenant. **OPERATOR** is the platform on-call role: it works across tenants, but only on operational data (state, counts, error codes), never payloads.
- **Tenant isolation:** `tenant_id` comes only from the JWT claim, never from request bodies. Every repository method takes a `TenantScope` parameter and adds `WHERE tenant_id = ?`. Cross-tenant reads return **404, not 403**, so the response does not reveal that a job exists. Postgres row-level security is an optional second guard.

---

## 14. Metrics, logging, caching and errors

### Metrics for the 3 AM on-call engineer

| Metric | Type | Alert when |
|---|---|---|
| `scheduler_fire_lag_seconds{tenant_tier,handler}` | Histogram, SLO buckets 0.1, 0.25, 0.5, 1, 2, 5, 10, 30 | p99 > 2 s for 5 min |
| `scheduler_runs{state="running\|retrying\|dead_lettered"}` | Gauge (DB count every 5 s) | dead_lettered increase > 0 |
| `scheduler_queue_depth` | Gauge: due and not started | Growing for 2 min |
| `scheduler_run_outcome_total{outcome}` | Counter: success, retryable, permanent, timeout | Error rate > 5% |
| `scheduler_stale_commit_rejected_total` | Counter | Any increase outside chaos tests (shows GC pauses or network splits) |
| `scheduler_tenant_cap_rejections_total{tenant}` | Counter: claim skipped because cap is full | Informational |
| `scheduler_partitions_owned{node}` | Gauge | Sum ≠ 128 for > 30 s |
| `scheduler_lease_renew_failures_total` | Counter | Rate > 0 |
| `scheduler_claim_batch_seconds` | Timer | p99 > 100 ms (DB is the bottleneck) |

A Grafana dashboard is provisioned by compose. Logs are JSON lines with MDC fields `nodeId, runId, jobId, tenantId, fencingToken, attempt, idemKeyHash`. Payloads are never logged. A `PiiMaskingConverter` masks phone, email and lat/long patterns as a last line of defence, and a unit test feeds it real-looking samples.

### Caching (and exactly how stale it can get)

| Cache | Key / eviction | Staleness bound |
|---|---|---|
| Job definitions (handler, cron, max run time, policy) | `(jobId, version)`, max 50k entries, LRU-ish (Caffeine W-TinyLFU) | **Never stale.** The run records the version it was materialised from, and a version is immutable; an edit creates a new version. |
| Tenant config (cap, zone) | `tenantId`, expire-after-write 5 s | A cap change takes effect within 5 s. **Pause is not cached:** the claim SQL joins `tenant.paused`, so pause is effective on the next poll (≤ 200 ms). |
| Next-fire-times API | `(jobId, version, count)`, 30 s TTL | Pure function of cron, zone and version, so it can only be stale for the 30 s after a zone change |
| Live node set | Refreshed every 2 s | Partition assignment can lag by one refresh; correctness is unaffected (§06) |

### Error handling

One `@RestControllerAdvice` maps a typed exception hierarchy (`SchedulerException` → `NotFound`, `Validation`, `Conflict`, `StaleFencingToken`, `StoreUnavailable`) to RFC 7807 `ProblemDetail` with a stable `code`, a safe message and a `traceId`. Responses never include stack traces, SQL or payload values. Validation errors name the field, never its value.

---

## 15. Code layout and OOP design

```
trip-job-scheduler/
├─ docker-compose.yml            postgres, 3× scheduler, sink, toxiproxy, prometheus, grafana
├─ scheduler-core/               PLAIN JAVA, no Spring, no DB — unit-testable algorithms
│  ├─ time/      CronSchedule, WallClockResolver (DST gap/overlap), NextFireCalculator
│  ├─ policy/    MisfirePolicy (sealed): FireNow | Skip | Coalesce
│  │             BackoffPolicy: ExponentialFullJitter
│  ├─ cluster/   PartitionAssigner (rendezvous), PartitionHasher
│  ├─ fairness/  TenantSlotLedger, DeficitRoundRobinDispatcher
│  ├─ lease/     LeaseClock (monotonic self-fence), FencingToken (value object)
│  └─ ports/     RunStore, PartitionLeaseStore, MembershipStore, Clock  (interfaces)
├─ scheduler-app/                Spring Boot
│  ├─ api/        JobController, TenantController, DeadLetterController, ClusterController
│  ├─ service/    JobService, RunLifecycleService, OpsService
│  ├─ repository/ JdbcRunStore, JdbcPartitionLeaseStore, JdbcJobRepository …  (implement ports)
│  ├─ engine/     MembershipLoop, PartitionLoop, Poller, Reaper, Materializer, LeaseRenewer
│  ├─ handler/    JobHandler (abstract) ← ChunkedJobHandler ← PickupAlertHandler
│  │                                   ← RosterGenerationHandler, NoShowCancelHandler, BillingCloseHandler
│  ├─ security/   JwtConfig, TenantScope, Roles
│  ├─ observability/ SchedulerMetrics, PiiMaskingConverter
│  └─ error/      SchedulerException hierarchy, ProblemDetailAdvice
├─ notification-sink/            mock provider: idempotent + fencing-aware
├─ loadgen/                      100k-job generator + lag reporter (HdrHistogram)
├─ chaos/                        kill-during-alerts.sh, freeze-during-alerts.sh
└─ docs/                         DESIGN.md (2 pages), BENCHMARK.md, diagrams/
```

- **Encapsulation:** `JobRun` exposes transitions (`claim`, `checkpoint`, `succeed`, `fail`) and no state setters, so illegal transitions do not compile.
- **Inheritance:** `JobHandler` provides the template method (validate → execute → classify errors); `ChunkedJobHandler` adds checkpointing; concrete handlers implement only `items()` and `deliver(item)`.
- **Polymorphism:** `MisfirePolicy`, `BackoffPolicy`, `JobDefinition` (`OneOffJob` / `RecurringJob`) and `Clock` (system vs test-controlled) are swapped through interfaces.
- **Ports and adapters:** the core runs against in-memory fakes in unit tests and against JDBC in integration tests. That meets the brief's requirement that core logic be testable without a web server or DB.

---

## 16. Test matrix (every hard rule is automated)

| Rule | Test | Kind |
|---|---|---|
| **R1** | `JobDefinitionTest`: one-off vs cron validation, max run time enforced (handler sleeping past limit → TIMEOUT) | Unit |
| **R2** | `WallClockResolverTest`: NY 2026-03-08 02:30 → 07:00Z; NY 2026-11-01 01:30 → one run at 05:30Z; London; Kolkata; `NextFireTimesTest` across both transitions | Unit |
| **R3** | `ExactlyOnceEffectIT`: 3 in-process nodes + Testcontainers Postgres, 10k jobs, forced random handler failures; assert sink deliveries = 10k distinct keys, attempt_log > 10k | **Concurrency** |
| **R4** | `FencingIT`: claim (token t), expire lease via test clock, second node claims (t+1); stale commit → 0 rows + exception; stale sink call → 409 | Integration |
| **R4** | `ClaimRaceIT`: 32 threads claim the same 1,000 due runs; each run claimed exactly once per token | **Concurrency** |
| **R5** | `PartitionAssignerTest`: determinism, balance (max/min ≤ 1.3 at N=3), minimal movement on join/leave. `RebalanceIT`: stop one of 3 nodes → all 128 partitions owned within 20 s | Unit + Integration |
| **R6** | `MisfirePolicyTest`: 3 missed hourly runs → FIRE_NOW 3, SKIP 0, COALESCE 1 with count 3 | Unit |
| **R7** | `TenantFairnessIT`: tenant A 50,000 + tenant B 20 at the same instant; assert B p99 lag < 1 s and A's concurrency never above cap (+ rebalance tolerance) | **Concurrency** |
| **R8** | `BackoffPolicyTest` (bounds, jitter distribution); `RetryToDeadLetterIT`: always-failing handler → 5 attempts → DLQ → replay keeps idem key | Unit + Integration |
| **R9** | `OpsApiIT`: pause stops new claims within one poll, resume applies misfire policy, next fire times, manual trigger dedup by header | API (MockMvc) |
| **R10** | `MetricsIT`: lag histogram records, state gauges match DB counts | Integration |
| **R11** | `chaos/kill-during-alerts.sh` + `NodeCrashIT` (in-JVM: halt the worker executor and drop its connection through Toxiproxy); assert 1,200 deliveries, 0 duplicates, takeover ≤ 11 s | **Failure injection** |
| **R12** | `chaos/freeze-during-alerts.sh` (`docker pause` 30 s) + `GcPauseIT` (block the handler thread past its lease); assert stale-commit counter = 1, sink 409s > 0, 1,200 deliveries | **Failure injection** |
| **R13** | `loadgen` scenarios in §17, run against compose; numbers go into BENCHMARK.md | Benchmark |
| **R14** | `TenantIsolationIT` (A's token on B's job → 404), `RoleMatrixIT`, `PiiMaskingTest`, `ErrorResponseTest` (no payload or stack trace in body) | API + Unit |

---

## 17. Benchmark plan and capacity math

### Scenarios

1. **Spread:** 100,000 one-off jobs across 50 tenants, due uniformly over 10 minutes (about 167/s), handler 20–60 ms simulated I/O. Report throughput, p50 / p95 / p99 / max fire lag. Target p99 < 2 s.
2. **Burst + fairness:** 50,000 for tenant A and 20 for tenant B, all due at T. Report B's lag separately.
3. **Spread + chaos:** scenario 1 with one node killed at minute 4 and one frozen at minute 7. Report p99 for the whole run and for runs in affected partitions, plus a duplicate count (must be 0).
4. **Micro-benchmarks** (JMH, core only): `WallClockResolver.next`, `PartitionAssigner.assign(128, N)`, DRR dispatch op.

### Expected numbers (to be replaced with measured ones)

| Quantity | Estimate | Reasoning |
|---|---|---|
| Average arrival rate | 167 runs/s | 100,000 / 600 s |
| Cluster execution capacity | ≈ 4,800 runs/s | 192 workers / 40 ms; about 29× headroom |
| DB writes per run | 3–4 | materialise (bulk), claim, 1–3 renews if long, commit |
| DB write load | ≈ 700 rows/s avg | A laptop Postgres handles 10k+ simple updates/s |
| Lag budget | ≈ 0.1 s + queueing | average poll wait 100 ms (200 ms interval) + claim 10–20 ms + dispatch |
| Takeover after kill | ≤ 11.2 s | 10 s lease + 1 s reaper + 0.2 s poll |

### Complexity

- **Claim:** O(T · log R + b) per poll, where T is tenants with free slots, R is due runs in owned partitions (partial index), and b is the batch size. Memory per node is O(workers + claimed batch), not O(jobs).
- **Assignment:** O(P · N) hashes per membership change (128 × 3 = 384, microseconds).
- **Materialisation:** O(occurrences in a 15-min horizon); one cron `next()` is O(1) amortised.
- **Storage:** O(jobs + runs kept). Completed runs are moved out after 7 days by a partition-by-day retention job.

### Where it stops scaling

The single Postgres primary is the ceiling. Above about 5–10k state transitions/s sustained (WAL and fsync bound on commodity disks), claim latency grows and lag follows. The second limit is 128 partitions, which caps useful nodes at about 40. Next steps, in order: shard Postgres by partition range (partitions already give the routing key), then move run dispatch to a partitioned log (Kafka) and keep Postgres for definitions and state. The README states the exact transitions/s where the benchmark p99 crosses 2 s.

The README reports CPU model, cores, RAM, disk type, OS, Docker resources, JVM flags and Postgres config next to the results.

---

## 18. Trade-offs and deliberate omissions

| Trade-off | Decision |
|---|---|
| **Consistency over availability** | When the DB is unreachable, no job fires. Firing from local state could double-fire and would need a second source of truth. Downtime is recovered later through the misfire policy. |
| **Latency vs DB load** | A 200 ms poll gives about 100 ms average lag and about 15 claim queries/s cluster-wide. LISTEN/NOTIFY for near-due runs would cut lag further but adds complexity; it is listed as a future improvement. |
| **Postgres vs etcd/ZooKeeper/Kafka** | One dependency, transactional state and a single trusted clock, at the cost of a hard write ceiling (§17). This is the right choice at this scale and is stated as a limit. |
| **Hard caps vs throughput** | Per-tenant caps leave capacity idle during a single-tenant burst. We chose isolation; caps are configurable per tenant. |
| **Lease length** | A shorter lease means faster takeover but more renew writes and more false takeovers during long GC pauses. 10 s lease / 3 s renew handles a 20 s GC pause safely through fencing, at the cost of an extra attempt. |
| **Approximate cluster cap** | Splitting each tenant's cap into per-node shares avoids a hot per-tenant counter row. It can briefly exceed the cap during rebalance (bounded, §10). |

### Left out on purpose

- Multi-region active-active scheduling and cross-region fencing.
- Exactly-once effects against third-party APIs that ignore idempotency keys.
- Job dependencies or DAG workflows; fixed-rate (elapsed-time) schedules.
- Sub-second scheduling precision (the target is seconds).
- An admin UI (Grafana and the REST API cover operations); real SMS/push providers.
- Full OAuth2 identity provider (a dev token minting endpoint stands in).

---

## 19. Delivery roadmap

Each milestone lands as several small commits with clear messages, so the history tells the story the brief asks for.

| Day | Milestone | Work |
|---|---|---|
| Day 1 | **Skeleton and one-command run** | Maven multi-module, `docker-compose.yml` (Postgres, 3 nodes, sink, Prometheus, Grafana), Flyway V1 schema, health endpoints, CI (GitHub Actions: build + tests). |
| Day 2 | **Core time logic** | `CronSchedule`, `WallClockResolver`, `NextFireCalculator` with the DST test suite (R2). Misfire and backoff policies with unit tests (R6, R8). |
| Day 3 | **Membership and partitions** | Heartbeats, rendezvous assignment, partition lease loop, `RebalanceIT` (R5). |
| Day 4 | **Claim, fence, commit** | Poller with per-tenant claim SQL, lease renewer, reaper, fenced commit. `FencingIT`, `ClaimRaceIT` (R4). |
| Day 5 | **Handlers and the sink** | `JobHandler` hierarchy, chunked pickup-alert handler with checkpoints, notification sink with idempotency and token checks. `ExactlyOnceEffectIT` (R3). |
| Day 6 | **Fairness, retries, DLQ** | Slot ledger, DRR dispatcher, retry/backoff wiring, dead-letter table and replay. `TenantFairnessIT`, `RetryToDeadLetterIT` (R7, R8). |
| Day 7 | **API, security, errors** | Controllers, JWT roles, `TenantScope`, ProblemDetail advice, PII masking. Isolation and role tests (R9, R14). |
| Day 8 | **Observability and caching** | Micrometer metrics, JSON logs with MDC, Grafana dashboard JSON, Caffeine caches (R10). |
| Day 9 | **Chaos and benchmark** | `chaos/*.sh`, `NodeCrashIT`, `GcPauseIT`, load generator, run the three scenarios and record results (R11–R13). |
| Day 10 | **Docs and demo** | README (run, assumptions, benchmark table with machine spec, trade-offs), `docs/DESIGN.md` (diagrams, sequence, failure table), Postman collection, demo video (R15). |

---

## 20. Demo video script (about 6 minutes)

1. `docker compose up`: show 3 nodes, the Grafana dashboard, and `GET /api/v1/cluster` with about 43 partitions per node.
2. Mint ADMIN tokens for two tenants. Create a recurring 22:00 roster job in `America/New_York` and show `next-fire-times` across 1 Nov 2026 (one 01:30 run) and 8 Mar 2027.
3. Show tenant isolation: tenant B's token on tenant A's job returns 404.
4. Start a 1,200-alert pickup job, run `chaos/kill-during-alerts.sh` live, and watch node 2 take over in Grafana. Query the sink: 1,200 delivered, 0 duplicates, some attempts deduped.
5. Repeat with `docker pause` for 30 s. Show the `stale_commit_rejected_total` counter increase and the 409 in the sink log.
6. Pause and resume a tenant; trigger a job manually twice with the same `Idempotency-Key`, which creates one run; replay a dead-lettered run.
7. Show the benchmark output: 100k jobs, p50 / p99 lag, and the fairness table for tenants A and B.

> **Assumptions to confirm before building:** a new repository is created for this project (the current workspace is an unrelated memory allocator). A mock notification sink is an acceptable stand-in for real SMS/push. Java 21 and Docker Desktop with about 6 GB RAM are available for the benchmark.
