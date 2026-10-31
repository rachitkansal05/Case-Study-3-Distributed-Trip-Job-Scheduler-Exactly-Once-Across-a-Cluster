# Test it yourself: a reviewer's walkthrough

This guide lets you check every main claim of the project on your own machine in about 20
minutes. Each step shows the commands to run and the result you should see, and names the
case-study requirement it proves. The [demo video](media/demo-video.mp4) follows the same steps.

| Step | What you verify | Case-study requirement |
|---|---|---|
| [1](#1-start-the-stack) | The whole system starts with one command | One-command run |
| [2](#2-the-cluster-no-single-hot-lock) | 3 nodes, 128 partitions, no leader | No single hot lock |
| [3](#3-monitoring-grafana-and-alerts) | Fire-lag histogram, counts, alerts | Metrics, monitoring |
| [4](#4-jobs-time-zones-and-dst) | Cron jobs in the tenant's zone; the DST gap and overlap | Job model, time zones |
| [5](#5-exactly-once-effect-and-a-manual-trigger) | One effect per key; idempotent manual trigger | Exactly-once effect, controls |
| [6](#6-kill-a-node-mid-send) | `kill -9` mid-send, takeover, 1,200 alerts once each | Example use case 1 |
| [7](#7-freeze-a-node-stale-write-rejected) | 30 s freeze, stale write rejected | Example use case 2, leases with fencing |
| [8](#8-retries-dead-letter-and-replay) | Backoff, dead letter, replay | Retries |
| [9](#9-pause-resume-and-tenant-caps) | Pause/resume, caps, fairness | Controls, per-tenant fairness |
| [10](#10-roles-and-tenant-isolation) | 404 across tenants, 403 for wrong role, no PII | Authentication |
| [11](#11-automated-tests-and-the-benchmark) | 868 tests; the 100k benchmark | Proven guarantees, measured performance |

---

## 0. Prerequisites

- **Docker with Compose v2, running.** On Windows and macOS, open Docker Desktop and wait for "Engine running".
- **`bash`, `curl` and `jq`.**
  - On Windows, use **Git Bash**.
  - Install jq with `winget install jqlang.jq` (Windows) or your package manager.
- **JDK 21+**, only if you want to run the test suite or the benchmark (step 11).
- **Two terminals** and a browser.

## 1. Start the stack

```bash
docker compose up -d --build
docker compose ps            # repeat until all 8 services are "healthy" (1–3 minutes on first build)
```

**Expected:** `postgres`, `scheduler-1/2/3`, `notification-sink`, `toxiproxy`, `prometheus` and `grafana`, all `healthy`.

Then, in **terminal 1**, define a token helper. The compose stack enables the `dev` profile, which exposes `POST /dev/token` for demo tokens:

```bash
tok() { curl -s -X POST localhost:8081/dev/token -H 'Content-Type: application/json' \
          -d "{\"tenant\":\"$1\",\"role\":\"$2\"}" | jq -r .token; }
OP=$(tok platform OPERATOR)
```

## 2. The cluster: no single hot lock

```bash
curl -s localhost:8081/api/v1/cluster -H "Authorization: Bearer $OP" \
  | jq '{owned: .ownedPartitions, unowned: .unownedPartitions, nodes: [.liveNodes[] | {nodeId, partitionsOwned}]}'
```

**Expected:**
- `owned: 128`, `unowned: 0`;
- three live nodes with 39 / 40 / 49 partitions (rendezvous hashing of the node ids);
- no node is a leader.

When a node leaves, only its partitions move; you will see that in step 6.

## 3. Monitoring: Grafana and alerts

Generate some traffic first, because a fresh stack has nothing to graph:

```bash
bash docs/demo-traffic.sh          # about 300 alerts over the next 5 minutes, plus a job every minute
```

Open **Grafana**: http://localhost:3000/d/scheduler-overview (anonymous access). Set the time range to *Last 15 minutes* and refresh to *5s*.

**Expected, after a minute:**
- the fire-lag histogram, with p50 about 0.1 s and p99 well under 2 s;
- running / retrying / dead-lettered counts;
- queue depth, claims, partitions and leases.

Open **Prometheus alerts**: http://localhost:9090/alerts.

**Expected:** 13 rules, all inactive (green). Each has a `runbook_url` into [`RUNBOOK.md`](RUNBOOK.md).

## 4. Jobs, time zones and DST

```bash
curl -s -X POST localhost:8081/api/v1/tenants -H "Authorization: Bearer $OP" -H 'Content-Type: application/json' \
  -d '{"id":"acme","timeZone":"America/New_York","cap":32}' | jq
curl -s -X POST localhost:8081/api/v1/tenants -H "Authorization: Bearer $OP" -H 'Content-Type: application/json' \
  -d '{"id":"globex","timeZone":"Asia/Kolkata","cap":8}' | jq
ADMIN=$(tok acme ADMIN); ADMIN_B=$(tok globex ADMIN); EMP=$(tok acme EMPLOYEE)

# 01:30 New York every day: on 1 Nov 2026 the hour 01:00–01:59 happens twice
CRON=$(curl -s -X POST localhost:8081/api/v1/jobs -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"kind":"RECURRING","handler":"ROSTER_GEN","cron":"30 1 * * *",
       "payload":{"shiftDate":"2026-11-01","siteId":"BLR-01"},"maxRunTime":"PT5M","misfire":"COALESCE"}' | jq -r .id)
curl -s "localhost:8081/api/v1/jobs/$CRON/next-fire-times?count=4" -H "Authorization: Bearer $ADMIN" | jq

# 02:30 on 14 March: on 14 Mar 2027 clocks jump from 02:00 to 03:00 in New York
GAP=$(curl -s -X POST localhost:8081/api/v1/jobs -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"kind":"RECURRING","handler":"BILLING_CLOSE","cron":"30 2 14 3 *",
       "payload":{"period":"2027-03","accountId":"ACME"},"maxRunTime":"PT5M"}' | jq -r .id)
curl -s "localhost:8081/api/v1/jobs/$GAP/next-fire-times?count=1" -H "Authorization: Bearer $EMP" | jq
```

**Expected:**
- On 1 Nov 2026 the job fires **once**, at 01:30 −04:00 (05:30Z). The next day it fires at 01:30 −05:00 (06:30Z).
- The 14 Mar 2027 occurrence fires at **03:00 local** (07:00Z), when the skipped hour ends.
- The EMPLOYEE token can read fire times.

## 5. Exactly-once effect and a manual trigger

```bash
RUN_AT=$(date -u -d '+20 seconds' +%FT%TZ)          # macOS: date -u -v+20S +%FT%TZ
JOB=$(curl -s -X POST localhost:8081/api/v1/jobs -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"kind":"ONE_OFF","handler":"PICKUP_ALERT","runAt":"'"$RUN_AT"'","priority":1,"maxRunTime":"PT5M","misfire":"SKIP",
       "payload":{"employeeIds":["emp-0001","emp-0002","emp-0003"],"messageTemplate":"Your cab arrives at {pickupTime}","pickupTime":"08:05"}}' | jq -r .id)
sleep 25
curl -s localhost:8081/api/v1/jobs/$JOB/runs -H "Authorization: Bearer $ADMIN" | jq '.items[0] | {runId, state, attempt, lagMillis}'
RUN=$(curl -s localhost:8081/api/v1/jobs/$JOB/runs -H "Authorization: Bearer $ADMIN" | jq -r '.items[0].runId')
curl -s "localhost:8090/api/v1/deliveries/stats?runId=$RUN" | jq

# manual trigger, twice with the same Idempotency-Key
curl -s -X POST localhost:8081/api/v1/jobs/$JOB/trigger -H "Authorization: Bearer $ADMIN" -H 'Idempotency-Key: demo-1' | jq
curl -s -X POST localhost:8081/api/v1/jobs/$JOB/trigger -H "Authorization: Bearer $ADMIN" -H 'Idempotency-Key: demo-1' | jq
```

**Expected:**
- `state: SUCCEEDED` with `lagMillis` of a few hundred ms at most.
- The sink reports `deliveredDistinct: 3`.
- Both triggers return the **same `runId`**, and the second one has `duplicate: true`.

## 6. Kill a node mid-send

Example use case 1: *a node is killed with kill -9 while sending 1,200 pickup alerts; another node
takes over the remaining alerts within the lease time, and every employee receives exactly one
alert.*

**Automated, in terminal 2:**

```bash
bash chaos/kill-during-alerts.sh
```

It creates a 1,200-employee job, slows the sink to 25 ms per alert, waits for 300 deliveries,
then `docker kill -s KILL`s the node that owns the run.

**Expected final block:**
- `RESULT: PASS`;
- a new owner with a **higher fencing token**;
- a takeover time of about 8–10 s (bound 10.7 s: 10 s lease + 0.5 s sweep + claim);
- 1,200 distinct deliveries, `maxDeliveriesPerKey = 1`.

**Watch it in Grafana while it runs:**
- the killed node's target goes down;
- its partitions move to the other two;
- "Orphan takeovers" increases.

**By hand, if you want to see each step:**

```bash
curl -s -X POST localhost:8081/api/v1/tenants -H "Authorization: Bearer $OP" -H 'Content-Type: application/json' \
  -d '{"id":"live","timeZone":"Asia/Kolkata","cap":8}' > /dev/null
LIVE=$(tok live ADMIN)
curl -s -X PUT localhost:8090/admin/faults -H 'X-Admin-Token: dev-only-sink-admin-token' \
  -H 'Content-Type: application/json' -d '{"latencyMs":25}' > /dev/null      # each alert takes 25 ms
EMPS=$(seq -f '"emp-%04g"' 1 1200 | paste -sd, -)
BIG=$(curl -s -X POST localhost:8081/api/v1/jobs -H "Authorization: Bearer $LIVE" -H 'Content-Type: application/json' \
  -d '{"kind":"ONE_OFF","handler":"PICKUP_ALERT","runAt":"'"$(date -u -d '+5 seconds' +%FT%TZ)"'","maxRunTime":"PT5M",
       "payload":{"employeeIds":['"$EMPS"'],"messageTemplate":"Cab at {pickupTime}","pickupTime":"08:05"}}' | jq -r .id)

# owner node, fencing token and state, every second (Ctrl+C to stop)
while true; do docker compose exec -T postgres psql -U scheduler -d scheduler -tAc \
  "SELECT owner_node, fencing_epoch||'.'||fencing_seq, state FROM job_run WHERE job_id='$BIG'"; sleep 1; done
```

In terminal 2, kill the owner shown, for example scheduler-2:

```bash
docker kill -s KILL memory-allocator-main-scheduler-2-1
```

**Expected in terminal 1:** after 8–10 s the owner changes and the token goes up (for example `1.5` → `1.6`); then the state becomes `SUCCEEDED`. Then check the sink and clean up:

```bash
RUN=$(curl -s localhost:8081/api/v1/jobs/$BIG/runs -H "Authorization: Bearer $LIVE" | jq -r '.items[0].runId')
curl -s "localhost:8090/api/v1/deliveries/stats?runId=$RUN" | jq     # deliveredDistinct 1200; alreadyDelivered = re-sent and absorbed
docker compose start scheduler-2
curl -s -X DELETE localhost:8090/admin/faults -H 'X-Admin-Token: dev-only-sink-admin-token' > /dev/null
```

## 7. Freeze a node: stale write rejected

Example use case 2: *a node is frozen for 30 seconds (SIGSTOP), its lease expires and another node
finishes the job; when the frozen node resumes, its stale write is rejected by the fencing token.*

```bash
bash chaos/freeze-during-alerts.sh
```

It `docker pause`s the owning node for 30 s mid-send (the cgroup freezer, the same effect as SIGSTOP), then unpauses it.

**Expected:**
- `RESULT: PASS`;
- the run finished on another node with a higher token;
- on the frozen node, at least one fencing counter goes up from 0 to 1:
  - `stale_commit`;
  - `lease_lost{stale_409}`;
  - `lease_lost{fenced_write_0_rows}`;
  - `lease_lost{self_fence}`;
- 1,200 distinct deliveries.

A warning `staleRejected is 0` only means the freeze landed between two sends, so no old-token request reached the sink. In Grafana, see "Lease lost by reason".

## 8. Retries, dead letter and replay

```bash
# make the provider fail every request with 503
curl -s -X PUT localhost:8090/admin/faults -H 'X-Admin-Token: dev-only-sink-admin-token' \
  -H 'Content-Type: application/json' -d '{"failureRate":1.0,"failureStatus":503}'
curl -s -X POST localhost:8081/api/v1/jobs -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"kind":"ONE_OFF","handler":"PICKUP_ALERT","runAt":"'"$(date -u -d '+5 seconds' +%FT%TZ)"'","maxRunTime":"PT1M","maxAttempts":2,
       "payload":{"employeeIds":["emp-0100"],"messageTemplate":"Cab at {pickupTime}","pickupTime":"09:00"}}' | jq -r .id
sleep 20
curl -s "localhost:8081/api/v1/dead-letters?tenant=acme" -H "Authorization: Bearer $OP" | jq '.items[0]'
DEAD=$(curl -s "localhost:8081/api/v1/dead-letters?tenant=acme" -H "Authorization: Bearer $OP" | jq -r '.items[0].runId')

# fix the provider, then replay twice
curl -s -X DELETE localhost:8090/admin/faults -H 'X-Admin-Token: dev-only-sink-admin-token'
curl -s -X POST localhost:8081/api/v1/dead-letters/$DEAD/replay -H "Authorization: Bearer $OP" | jq
curl -s -X POST localhost:8081/api/v1/dead-letters/$DEAD/replay -H "Authorization: Bearer $OP" | jq '{status, code}'
```

**Expected:**
- The dead letter shows `errorCode: SINK_5XX` and the attempt count, never the payload.
- The first replay puts the **same run and idempotency key** back to `SCHEDULED`, and the run then succeeds.
- The second replay returns `409 DEAD_LETTER_ALREADY_REPLAYED`.
- In Grafana, "Dead letters by error code" shows `SINK_5XX`; in Prometheus, `SchedulerDeadLettersIncreasing` fires.

`maxAttempts: 2` keeps the wait short. The default is 5, with full-jitter backoff from 2 s up to 5 minutes.

## 9. Pause, resume and tenant caps

```bash
curl -s -X POST localhost:8081/api/v1/tenants/acme/pause  -H "Authorization: Bearer $ADMIN" | jq
curl -s -X POST localhost:8081/api/v1/tenants/acme/resume -H "Authorization: Bearer $ADMIN" | jq
curl -s -X PUT localhost:8081/api/v1/tenants/acme/cap -H "Authorization: Bearer $ADMIN" \
  -H 'Content-Type: application/json' -d '{"cap":16}' | jq
```

**Expected:** pause and resume succeed; the cap response shows `configuredCap`, `effectiveCap` and `draining`.

**Fairness live** (optional):

```bash
BURST=400 STEADY=0 bash docs/demo-traffic.sh
```

This gives tenant `demo-big` (cap 4) 400 alerts due at once, while tenant `demo-small`'s 5 alerts are due at the same moment.

**Expected:**
- `demo-big` queues at its cap, so its lag rises into seconds;
- `demo-small`'s alerts start within about 0.3 s.

Check the lag per tenant with:

```bash
docker compose exec -T postgres psql -U scheduler -d scheduler -c "SELECT tenant_id, count(*),
  round(max(extract(epoch FROM started_at - scheduled_at))::numeric, 2) AS max_lag_s
  FROM job_run WHERE tenant_id LIKE 'demo-%' AND started_at IS NOT NULL GROUP BY 1"
```

## 10. Roles and tenant isolation

```bash
curl -s localhost:8081/api/v1/jobs/$JOB -H "Authorization: Bearer $ADMIN_B" | jq '{status, code, traceId}'
curl -s -X POST localhost:8081/api/v1/jobs -H "Authorization: Bearer $EMP" -H 'Content-Type: application/json' \
  -d '{"kind":"RECURRING","handler":"ROSTER_GEN","cron":"0 22 * * *","payload":{},"maxRunTime":"PT5M"}' | jq '{status, code}'
curl -s localhost:8081/api/v1/jobs/$JOB -H "Authorization: Bearer $EMP" | jq '{payloadRedacted, payload}'
docker compose logs --no-log-prefix scheduler-1 | tail -3
```

**Expected:**
- Another tenant's job is `404 JOB_NOT_FOUND`, the same as a job that doesn't exist.
- A wrong role is `403`.
- The EMPLOYEE sees the job with `payloadRedacted: true`.
- Logs are JSON lines with `traceId` and `tenantId` and no phone numbers or other payload data.

## 11. Automated tests and the benchmark

```bash
./mvnw -B verify          # Windows: mvnw.cmd -B verify. Embedded PostgreSQL; about 10–15 minutes
```

**Expected:** 868 tests, covering every hard rule of the case study, including concurrency and failure tests. The rule-to-test map is in [design/testing.md](design/testing.md).

Faster, two key tests only:

```bash
./mvnw -B -pl scheduler-app -am verify -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test='FencingIT,NodeCrashIT' -Dfailsafe.failIfNoSpecifiedTests=false
```

The 100k benchmark takes about 12 minutes:

```bash
./mvnw -B -pl loadgen -am verify -Pbench -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test=InProcessBench -Dfailsafe.failIfNoSpecifiedTests=false -Dbench.scenario=spread
```

Add `-Dbench.jobs=3000 -Dbench.windowSec=30` for a 5-minute smoke run.

**Expected:** a lag table with p50 about 0.1 s and p99 well under 2 s, and 0 duplicates. The recorded results are in [design/performance-and-benchmark.md](design/performance-and-benchmark.md).

---

## Resetting and troubleshooting

| Problem | Fix |
|---|---|
| `failed to connect to the docker API` | Docker isn't running: open Docker Desktop and wait for "Engine running" |
| A service is not healthy | `docker compose up -d`, wait 30 s, then `docker compose ps` |
| `jq: command not found` | Install jq (`winget install jqlang.jq`) and open a new terminal |
| `$ADMIN` / `$OP` is empty | Run the `tok` lines from step 1 again in that terminal |
| A chaos script says the run finished before the kill or freeze | Run it again |
| Start over from a clean state | `docker compose down -v`, then `docker compose up -d --build` |
