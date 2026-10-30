# Distributed Trip Job Scheduler

**Exactly-once effects across a cluster.** A scheduler for MoveInSync trip jobs:
- nightly rosters at 22:00 tenant time;
- "your cab is assigned" alerts 30 minutes before pickup;
- no-show auto-cancels;
- daily billing close.

It runs on three or more identical nodes with no leader. A node can be killed or frozen at any moment, and no job is lost and no job's effect happens twice.

> MoveInSync Backend Case Study 2026 · SDE Java Backend · **Case Study 3 · Advanced Track**
>
> Java 21 · Spring Boot 3.3 · PostgreSQL 16 · Docker Compose · Prometheus + Grafana · 868 automated tests

| Target from the case study | Result |
|---|---|
| 100,000 jobs due in one 10-minute window, 3 nodes, p99 fire lag < 2 s | **p99 237 ms**, 0 duplicates |
| `kill -9` while sending 1,200 alerts | Taken over in **7.6–8.0 s**; every employee alerted **once** |
| 30 s freeze (SIGSTOP) | Run finished elsewhere; the frozen node's stale write **rejected by the fencing token** |
| 50,000 alerts from one tenant vs 20 from another | The 20 start within **185 ms** |

---

## Demo video

<!-- PLACEHOLDER:DEMO_VIDEO -->
> 🎥 **Demo video:** _link to be added_
>
> The video shows the main flows, a node killed and a node frozen while sending 1,200 alerts (handled live), and sample API calls with curl and Postman.

---

## Table of contents

**In this README**

1. [Architecture](#1-architecture): the design note, with the architecture diagrams, the sequence diagram of the hardest flow and the [failure-mode table](#failure-modes)
2. [How to run it](#2-how-to-run-it)
3. [Benchmark results](#3-benchmark-results)
4. [Assumptions](#4-assumptions)
5. [Trade-offs](#5-trade-offs)
6. [Repository layout](#6-repository-layout)

**Design documentation** (all in [`docs/design/`](docs/design/))

📘 **Overall detailed architecture: [`docs/design/architecture.md`](docs/design/architecture.md)**

| Feature | Design doc |
|---|---|
| Job model, cron, time zones and DST | [job-model-and-time-zones.md](docs/design/job-model-and-time-zones.md) |
| Exactly-once effect, leases and fencing tokens | [exactly-once-leases-and-fencing.md](docs/design/exactly-once-leases-and-fencing.md) |
| Partitioning and membership (no single hot lock) | [partitioning-and-membership.md](docs/design/partitioning-and-membership.md) |
| Misfire policies, retries and the dead-letter list | [misfires-retries-and-dead-letters.md](docs/design/misfires-retries-and-dead-letters.md) |
| Per-tenant fairness and concurrency caps | [tenant-fairness-and-caps.md](docs/design/tenant-fairness-and-caps.md) |
| Operations: pause/resume, next fire times, manual trigger, API, config | [operations-api.md](docs/design/operations-api.md) |
| Failure handling: kill, freeze, failure-mode table, DB failover, chaos tests | [failure-handling.md](docs/design/failure-handling.md) |
| Performance: benchmark, complexity, where it stops scaling, cost | [performance-and-benchmark.md](docs/design/performance-and-benchmark.md) |
| Authentication, roles and tenant isolation | [security-and-tenant-isolation.md](docs/design/security-and-tenant-isolation.md) |
| Metrics, alerts and structured logging | [monitoring-and-logging.md](docs/design/monitoring-and-logging.md) |
| Caching and staleness bounds | [caching.md](docs/design/caching.md) |
| Error and exception handling, no PII | [error-handling.md](docs/design/error-handling.md) |
| Code structure, layering and OOP | [code-structure.md](docs/design/code-structure.md) |
| Testing: every hard rule automated | [testing.md](docs/design/testing.md) |
| Trade-offs and deliberate omissions | [trade-offs.md](docs/design/trade-offs.md) |

**Other references:**
- [`docs/RUNBOOK.md`](docs/RUNBOOK.md): on-call runbook, one section per alert;
- [`docs/sink-api.md`](docs/sink-api.md): notification sink API;
- [`docs/postman/`](docs/postman/): Postman collection;
- [`chaos/README.md`](chaos/README.md): chaos scripts;
- [`deploy/patroni/README.md`](deploy/patroni/README.md): production DB failover.

---

## 1. Architecture

### System overview

![Architecture overview: clients, the 3-node scheduler cluster, PostgreSQL as coordinator and clock, the notification sink, Prometheus and Grafana](docs/images/architecture-overview.png)

| Component | Role |
|---|---|
| **Scheduler cluster** | Three identical nodes running the same jar. There is no leader; any node takes API calls and runs jobs |
| **PostgreSQL** | The only coordinator **and the only clock**. Holds jobs and runs, node heartbeats, 128 partition leases, tenant slots (caps), run leases with fencing tokens, and dead letters. Every due-time and lease check uses its `clock_timestamp()` |
| **Notification sink** | Mock SMS/push provider. Stores each idempotency key once (repeats get `200 ALREADY_DELIVERED`) and rejects any fencing token lower than the highest it has seen for the run (`409 STALE_TOKEN`) |
| **Clients** | Admin and employee use the REST API with a JWT; the operator replays dead letters and watches the cluster |
| **Prometheus + Grafana** | Scrape every node and the sink; 13 alert rules, each with a runbook section |

The numbered arrows in the diagram:
1. **Create.** Any node accepts API calls.
2. **Claim.** A node claims due runs from the partitions it owns, one tenant slot per run.
3. **Run.** It renews the lease, saves checkpoints and makes a fenced commit.
4. **Send.** Every alert carries an idempotency key and the fencing token.
5. **Dead-letter.** A run that fails 5 times becomes a dead letter.
6. **Replay.** An operator replays it with the same key, so it runs once.

Heartbeats every 2 s keep membership current. A node silent for 6 s is dead, and its partitions move.

### Inside a node

![Inside a node: API, membership, partition owner and materializer, poller, orphan sweep and workers, writing to node heartbeats, runs and dead letters in PostgreSQL and sending to the sink](docs/images/architecture-node.png)

| Component | Interval | What it does | Design doc |
|---|---|---|---|
| **API** | — | REST endpoints, JWT roles, tenant isolation, retry-safe writes | [operations](docs/design/operations-api.md), [security](docs/design/security-and-tenant-isolation.md) |
| **Membership** | 2 s | Heartbeat into `scheduler_node`; a node is live if its heartbeat is under 6 s old (DB time) | [partitioning](docs/design/partitioning-and-membership.md) |
| **Partition owner** | 1 s | Owns a share of 128 partitions by rendezvous hashing; 6 s partition leases | [partitioning](docs/design/partitioning-and-membership.md) |
| **Materializer** | 5 s | Creates `SCHEDULED` runs up to 15 min ahead, in the tenant's time zone, DST-correct | [job model](docs/design/job-model-and-time-zones.md) |
| **Poller** | 200 ms + on every freed slot | Claims due runs, one tenant slot per run; applies misfire policies | [fairness](docs/design/tenant-fairness-and-caps.md), [misfires](docs/design/misfires-retries-and-dead-letters.md) |
| **Workers** | continuous | 64 per node, fair across tenants (deficit round robin); run handlers, renew the 10 s lease every 3 s, retry with backoff | [exactly-once](docs/design/exactly-once-leases-and-fencing.md) |
| **Orphan sweep** | 500 ms | Takes over runs whose lease expired, in **any** partition, from their checkpoint, with a higher token | [failure handling](docs/design/failure-handling.md) |

### Hardest flow: node frozen mid-run, taken over, stale write rejected

![Sequence: node A claims run 77 with token 7.41 and is frozen; its lease expires; node B's orphan sweep restarts the run with token 7.42 and finishes it; node A wakes up and its stale send and commit are rejected](docs/images/sequence-freeze-takeover.png)

1. **Normal run** (steps 1–5).
   - Node A claims run 77 in slot 4 with token `7.41` and a 10 s lease.
   - It sends alerts 1–600 with the idempotency key and token, then saves a checkpoint at 601.
   - Every renew and checkpoint is conditional on the token and a live lease.
2. **Freeze and takeover** (steps 6–11).
   - Node A is frozen for 30 s, and its lease expires on the database clock.
   - Within 500 ms, node B's orphan sweep reaps the run and restarts it in the same slot with token `7.42`.
   - B sends alerts 601–1,200 from the checkpoint and commits. Repeats get `200 ALREADY_DELIVERED`.
3. **Wake-up** (steps 12–16). Three layers stop node A:
   - its own deadline has passed, so it stops itself;
   - a send already in flight gets `409 STALE_TOKEN` from the sink;
   - its renew or commit matches 0 rows in the database.

Result: 1,200 employees alerted exactly once. A `kill -9` is the same flow without the wake-up, and the takeover bound is **≤ 10.7 s** after the last renewal.

**Why exactly-once execution is impossible:** a node that sent an alert and died before recording it looks the same as one that died before sending. So execution is **at least once**, and the **effect** is exactly once, through idempotency keys and fencing tokens ([details](docs/design/exactly-once-leases-and-fencing.md)).

### Failure modes

| What fails | What the user sees | How the system recovers |
|---|---|---|
| Node killed (`kill -9`) mid-run | Alerts pause ≤ 10.7 s, then continue; nobody gets two | The run lease expires; another node's orphan sweep restarts the run in the same slot with a higher token, from its checkpoint; the sink drops repeated items |
| Node frozen (GC pause, SIGSTOP) | Same as a kill | Another node takes over as above; on wake-up the frozen node stops itself, its in-flight send gets `409`, and its renew and commit match 0 rows |
| Node lost, its partitions orphaned | New runs in those partitions start up to about 7 s late | Partition leases (6 s) expire and the surviving nodes take the partitions over |
| Node joins | Nothing | Rendezvous hashing moves only its share of partitions to it |
| Renewal and slot reuse at the same moment | Nothing | Both are conditional updates of one row; exactly one succeeds |
| Notification sink down, slow or timing out | Alerts delayed; retries rise | Exponential backoff with jitter, then the dead-letter list; an operator replays with the same key; caps keep the impact to one tenant |
| Duplicate or out-of-order messages | One effect | Idempotency keys at the sink and the API; every write is conditional on the fencing token and state |
| PostgreSQL unreachable | No jobs fire; API returns `503 SCHEDULER_STORE_UNAVAILABLE` | Nodes stop starting work; when it returns, misfire policies handle the backlog |
| PostgreSQL primary fails (production) | Writes pause about 10–30 s; nothing acknowledged is lost | Patroni promotes only the synchronous standby; the new timeline raises the token epoch |
| Clock skew between nodes | Nothing | Due times and leases use the database clock |
| Bad payload | Run appears in the dead-letter list as `INVALID_PAYLOAD` | Dead-lettered at once, no retries |

Each row with its detection and the test that proves it: [failure-handling.md](docs/design/failure-handling.md#3-failure-mode-table).

---

## 2. How to run it

### Start everything with one command

**Requirements:**
- Docker with Compose v2;
- JDK 21 or newer, only for building and running the tests without Docker (the wrapper downloads Maven);
- `jq`, for the curl examples and the chaos scripts.

```bash
docker compose up --build
```

| Service | Port | Purpose |
|---|---|---|
| `scheduler-1`, `-2`, `-3` | 8081–8083 | The three scheduler nodes |
| `postgres` | 5432 | Job store and the cluster's clock |
| `notification-sink` | 8090 | Mock SMS/push provider (enforces keys and tokens) |
| `toxiproxy` | 8474 | Fault injection between each node and Postgres / the sink |
| `prometheus` | 9090 | Metrics and alerts: http://localhost:9090/alerts |
| `grafana` | 3000 | Dashboard: http://localhost:3000/d/scheduler-overview (anonymous, or admin / admin) |

`docker compose ps` shows when all eight are `healthy`.

`POST /dev/token`, used below to mint demo tokens, exists only in the `dev` profile that compose enables for the demo. Without `dev` the endpoint is absent, and a node refuses to start unless `SCHEDULER_JWT_HMAC_SECRET` is set to a real secret ([security](docs/design/security-and-tenant-isolation.md)).

### First API calls

```bash
# tokens (valid 1 hour, accepted by every node)
OP=$(curl -s -X POST localhost:8081/dev/token -H 'Content-Type: application/json' \
  -d '{"tenant":"platform","role":"OPERATOR"}' | jq -r .token)

# a tenant in New York time with a concurrency cap of 32
curl -s -X POST localhost:8081/api/v1/tenants -H "Authorization: Bearer $OP" \
  -H 'Content-Type: application/json' -d '{"id":"acme","timeZone":"America/New_York","cap":32}'

ADMIN=$(curl -s -X POST localhost:8081/dev/token -H 'Content-Type: application/json' \
  -d '{"tenant":"acme","role":"ADMIN"}' | jq -r .token)

# a recurring roster job at 22:00 New York time, and its next fire times (local time + offset)
ROSTER=$(curl -s -X POST localhost:8081/api/v1/jobs -H "Authorization: Bearer $ADMIN" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: roster-blr-01' \
  -d '{"kind":"RECURRING","handler":"ROSTER_GEN","cron":"0 22 * * *",
       "payload":{"shiftDate":"2026-11-01","siteId":"BLR-01"},"maxRunTime":"PT5M","misfire":"COALESCE"}' | jq -r .id)
curl -s "localhost:8081/api/v1/jobs/$ROSTER/next-fire-times?count=3" -H "Authorization: Bearer $ADMIN"

# a one-off pickup alert for three employees, 30 s from now (macOS: date -u -v+30S +%FT%TZ)
RUN_AT=$(date -u -d '+30 seconds' +%FT%TZ)
JOB=$(curl -s -X POST localhost:8081/api/v1/jobs -H "Authorization: Bearer $ADMIN" \
  -H 'Content-Type: application/json' \
  -d '{"kind":"ONE_OFF","handler":"PICKUP_ALERT","runAt":"'"$RUN_AT"'","priority":1,
       "maxRunTime":"PT5M","misfire":"SKIP",
       "payload":{"employeeIds":["emp-0001","emp-0002","emp-0003"],
                  "messageTemplate":"Your cab arrives at {pickupTime}","pickupTime":"08:05"}}' | jq -r .id)

# after it ran (wait ~30 s): its run history, then the sink's exactly-once counters
RUN=$(curl -s localhost:8081/api/v1/jobs/$JOB/runs -H "Authorization: Bearer $ADMIN" | jq -r '.items[0].runId')
curl -s "localhost:8090/api/v1/deliveries/stats?runId=$RUN"   # deliveredDistinct 3
curl -s localhost:8090/api/v1/deliveries/stats                 # maxDeliveriesPerKey 1

# manual trigger (the Idempotency-Key is required; a repeat returns the same run)
curl -s -X POST localhost:8081/api/v1/jobs/$JOB/trigger -H "Authorization: Bearer $ADMIN" -H 'Idempotency-Key: manual-1'

# pause and resume the whole tenant
curl -s -X POST localhost:8081/api/v1/tenants/acme/pause  -H "Authorization: Bearer $ADMIN"
curl -s -X POST localhost:8081/api/v1/tenants/acme/resume -H "Authorization: Bearer $ADMIN"
```

All endpoints: [operations-api.md](docs/design/operations-api.md). The Postman collection is in [`docs/postman/`](docs/postman/).

### Run the kill and freeze tests live

With the stack running, in Git Bash or any POSIX shell:

```bash
bash chaos/kill-during-alerts.sh     # kill -9 the node sending 1,200 alerts → RESULT: PASS
bash chaos/freeze-during-alerts.sh   # freeze it for 30 s → RESULT: PASS, stale write rejected
```

### Run the tests

```bash
./mvnw -B verify          # Windows: mvnw.cmd -B verify. No Docker needed (embedded PostgreSQL)
```

The build runs 868 tests, covering every hard rule of the case study, including concurrency and failure tests ([testing.md](docs/design/testing.md)).

### Run the benchmark

```bash
# spread | burst | chaos | ramp | all   (about 12 minutes per 100k scenario)
./mvnw -B -pl loadgen -am verify -Pbench -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test=InProcessBench -Dfailsafe.failIfNoSpecifiedTests=false -Dbench.scenario=spread
```

For a 5-minute smoke run, add `-Dbench.jobs=3000 -Dbench.windowSec=30`.

---

## 3. Benchmark results

**Setup:**
- three real scheduler nodes with production settings: 64 workers, 200 ms poll, 10 s run lease, 500 ms sweep;
- the real notification sink over HTTP in a second JVM, adding 20–60 ms per delivery;
- PostgreSQL with `fsync` and `synchronous_commit` on.

| Scenario | Runs | Throughput | p50 lag | **p99 lag** | Max lag | Duplicates |
|---|---|---|---|---|---|---|
| **Target:** 100,000 jobs, 50 tenants, one 10-minute window | 100,000 | 166.6 runs/s | 108 ms | **237 ms** ✅ | 849 ms | **0** |
| Same, with a node killed at 4 min and another frozen 30 s at 7 min | 100,000 | 166.6 runs/s | 109 ms | **291 ms** ✅ | 7.2 s | **0** |
| Fairness: tenant B's 20 alerts during tenant A's 50,000 | 20 | — | 142 ms | 185 ms | **185 ms** | **0** |
| Burst: tenant A's 50,000 at once, cap 32 | 50,000 | 698 runs/s (drained in 71.6 s) | 36.2 s | 70.8 s | 71.6 s | **0** |


**Machine:**
- **Hardware:** Intel Core i7-10750H laptop (6 cores / 12 threads), 16 GiB DDR4-2933, 1 TB NVMe SSD.
- **OS:** Windows 11 Home 10.0.26200.
- **Java:** HotSpot 22.0.2 (`--release 21`); benchmark JVM `-Xmx4g` shared by the 3 nodes, sink JVM `-Xmx768m`.
- **Database:** PostgreSQL 16.15, `shared_buffers=512MB`.



Full results, complexity analysis and the cost estimate: [performance-and-benchmark.md](docs/design/performance-and-benchmark.md).

---

## 4. Assumptions

- **Downstream idempotency.** Downstream providers honour idempotency keys. A mock notification sink stands in for the real SMS and push providers.
- **Precision.** Jobs need precision in seconds, not milliseconds.
- **Time zones.** Tenant zones are IANA region IDs (`Asia/Kolkata`, `America/New_York`); fixed offsets are rejected.
- **DST rules.**
  - A time in the spring-forward gap fires once at the end of the gap (03:00).
  - A time in the repeated fall-back hour fires once, at its first occurrence.
- **Clocks.**
  - The PostgreSQL primary's clock decides when jobs are due, and it never steps backwards (NTP in slew mode).
  - Node clocks may be skewed.
- **Frozen processes.** A frozen process can run for a moment after it wakes. The guarantees cover leases and effects, not what a paused process physically does.
- **Database failover.** In production a single failover manager (Patroni + etcd) runs Postgres with synchronous replication ([`deploy/patroni/`](deploy/patroni/)). The compose stack runs a single Postgres.
- **Cluster size.** At least one node stays alive, with a free worker, for the takeover bound to hold (one worker per node is reserved for this).

---

## 5. Trade-offs

- **Consistency over availability.** If the database is unreachable, nothing fires. Firing from local state could fire twice.
- **Durability over availability on failover.** Synchronous replication blocks writes when no standby is available, and no acknowledged job is lost.
- **Latency vs database load.**
  - A 200 ms poll plus an immediate claim when a slot frees gives p50 ≈ 105 ms and p99 237 ms.
  - Claim load grows with tenants and throughput.
- **PostgreSQL as the only coordinator.** One dependency, transactional state and one trusted clock, instead of etcd, ZooKeeper or Kafka. The price is a write ceiling (the knee above).
- **Hard per-tenant caps.** Isolation between tenants, at the cost of idle capacity during a single-tenant burst.
- **Lease lengths.**
  - The 10 s run lease gives ≤ 10.7 s takeovers with few false takeovers during GC pauses.
  - The 6 s partition lease is what new runs in a dead node's partitions wait for.
- **Left out on purpose:**
  - multi-region scheduling;
  - job dependencies;
  - sub-second precision;
  - an admin UI;
  - real SMS and push providers;
  - an identity provider;
  - our own DB failover manager.

All trade-offs and the reasoning: [trade-offs.md](docs/design/trade-offs.md).

---

## 6. Repository layout

```
├─ docker-compose.yml   postgres, 3 scheduler nodes, notification-sink, toxiproxy, prometheus, grafana
├─ scheduler-core/      plain Java (no Spring, no DB): cron and DST, misfire and backoff policies,
│                       partition assignment, fairness, lease clock, state machine, ports
├─ scheduler-app/       Spring Boot node: api → service → repository, engine, handlers, security
├─ notification-sink/   mock provider that enforces idempotency keys and fencing tokens
├─ loadgen/             load generator, InProcessBench and the end-to-end kill/freeze tests
├─ chaos/               kill-during-alerts.sh, freeze-during-alerts.sh
├─ deploy/patroni/      production database failover configuration
├─ ops/                 Prometheus alert rules, Grafana dashboard, Toxiproxy proxies
├─ docs/design/         architecture.md + one design doc per feature
├─ docs/                RUNBOOK.md, sink-api.md, postman/, images/
└─ .github/workflows/   CI: ./mvnw -B verify on JDK 21
```
