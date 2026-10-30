# Demo video script (about 9 minutes)

Every command below was checked against the code (routes, bodies, roles) and the compose file. The compose stack and the chaos scripts have **not** been run on the machine these docs were written on (no Docker there), so do one dry run before recording.

**Before recording**

- Docker with Compose v2, plus `bash`, `curl` and `jq` (on Windows use Git Bash). Two terminals and a browser.
- Start the stack a few minutes early and wait until every service is `healthy`:

  ```bash
  docker compose up -d --build
  docker compose ps
  ```

  The nodes already run the `json,dev` profiles, so `POST /dev/token` works and the chaos scripts need nothing else. `chaos/docker-compose.chaos.yml` sets the same profiles and is now redundant; the scripts still pass it by default, which is harmless.
- Optional: import `docs/postman/` into Postman; every API step below is also a request there.
- Open tabs: Grafana http://localhost:3000/d/scheduler-overview (anonymous view works; admin / admin to edit), Prometheus alerts http://localhost:9090/alerts.

Shell setup used throughout (terminal 1):

```bash
tok() { curl -s -X POST localhost:8081/dev/token -H 'Content-Type: application/json' \
          -d "{\"tenant\":\"$1\",\"role\":\"$2\"}" | jq -r .token; }
OP=$(tok platform OPERATOR)
PSQL='docker compose exec -T postgres psql -U scheduler -d scheduler -c'
```

---

## 0:00 Intro (30 s)

Say: three identical scheduler nodes, Postgres as the only coordinator and clock, a mock SMS/push provider that enforces idempotency keys and fencing tokens. The claim to prove: a node can be killed or frozen mid-send and every employee still gets exactly one alert.

Show the architecture diagram in `docs/DESIGN.md` §1.

## 0:30 The cluster is up (45 s)

```bash
docker compose ps
curl -s localhost:8081/api/v1/cluster -H "Authorization: Bearer $OP" \
  | jq '{owned: .ownedPartitions, unowned: .unownedPartitions, nodes: [.liveNodes[] | {nodeId, partitionsOwned}]}'
```

Show: 3 live nodes owning all 128 partitions, split 39 / 40 / 49 (rendezvous hashing of the node ids; balance is statistical, not exact). No node is a leader.

## 1:15 Grafana and alerts (45 s)

In Grafana walk the rows: SLO overview (fire-lag p99, queue depth, running / retrying / dead-lettered), leases and takeovers, dead letters by error code, tenant caps, notification sink. In Prometheus `/alerts` show the 13 rules, all green; each has a `runbook_url` into `docs/RUNBOOK.md`. There is no Alertmanager in the demo.

## 2:00 Tenants, tokens and jobs, including DST (1 min 15 s)

```bash
curl -s -X POST localhost:8081/api/v1/tenants -H "Authorization: Bearer $OP" -H 'Content-Type: application/json' \
  -d '{"id":"acme","timeZone":"America/New_York","cap":32}' | jq
curl -s -X POST localhost:8081/api/v1/tenants -H "Authorization: Bearer $OP" -H 'Content-Type: application/json' \
  -d '{"id":"globex","timeZone":"Asia/Kolkata","cap":8}' | jq
ADMIN=$(tok acme ADMIN); ADMIN_B=$(tok globex ADMIN); EMP=$(tok acme EMPLOYEE)

# recurring roster job at 01:30 New York time: 1 Nov 2026 has two 01:30s
CRON=$(curl -s -X POST localhost:8081/api/v1/jobs -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"kind":"RECURRING","handler":"ROSTER_GEN","cron":"30 1 * * *",
       "payload":{"shiftDate":"2026-11-01","siteId":"BLR-01"},"maxRunTime":"PT5M","misfire":"COALESCE"}' | jq -r .id)
curl -s "localhost:8081/api/v1/jobs/$CRON/next-fire-times?count=4" -H "Authorization: Bearer $ADMIN" | jq

# 02:30 on 14 March: that wall time does not exist in New York in 2027
GAP=$(curl -s -X POST localhost:8081/api/v1/jobs -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"kind":"RECURRING","handler":"BILLING_CLOSE","cron":"30 2 14 3 *",
       "payload":{"period":"2027-03","accountId":"ACME"},"maxRunTime":"PT5M"}' | jq -r .id)
curl -s "localhost:8081/api/v1/jobs/$GAP/next-fire-times?count=1" -H "Authorization: Bearer $EMP" | jq
```

Show: on 1 Nov 2026 the job fires **once**, at 01:30 −04:00 (05:30Z), and the next day at 01:30 −05:00 (06:30Z). The 14 Mar 2027 occurrence fires at 03:00 local (07:00Z), when the skipped hour ends. The EMPLOYEE token can read fire times but not create jobs.

## 3:15 Exactly-once, the happy path (45 s)

```bash
RUN_AT=$(date -u -d '+20 seconds' +%FT%TZ)     # macOS: date -u -v+20S +%FT%TZ
JOB=$(curl -s -X POST localhost:8081/api/v1/jobs -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"kind":"ONE_OFF","handler":"PICKUP_ALERT","runAt":"'"$RUN_AT"'","priority":1,"maxRunTime":"PT5M","misfire":"SKIP",
       "payload":{"employeeIds":["emp-0001","emp-0002","emp-0003"],"messageTemplate":"Your cab arrives at {pickupTime}","pickupTime":"08:05"}}' | jq -r .id)
sleep 25
curl -s localhost:8081/api/v1/jobs/$JOB/runs -H "Authorization: Bearer $ADMIN" | jq '.items[0] | {runId, state, attempt, lagMillis}'
RUN=$(curl -s localhost:8081/api/v1/jobs/$JOB/runs -H "Authorization: Bearer $ADMIN" | jq -r '.items[0].runId')
curl -s "localhost:8090/api/v1/deliveries/stats?runId=$RUN" | jq
curl -s localhost:8081/api/v1/jobs/$JOB/trigger -X POST -H "Authorization: Bearer $ADMIN" -H 'Idempotency-Key: demo-1' | jq
curl -s localhost:8081/api/v1/jobs/$JOB/trigger -X POST -H "Authorization: Bearer $ADMIN" -H 'Idempotency-Key: demo-1' | jq
```

Show: `SUCCEEDED` with a lag of a few hundred ms; the sink has 3 distinct deliveries. The two triggers with the same `Idempotency-Key` return the same `runId`, the second with `duplicate: true`.

## 4:00 Kill a node mid-send (1 min 30 s)

Terminal 2:

```bash
chaos/kill-during-alerts.sh
```

Say what it does while it runs: creates a tenant, a 1,200-employee pickup-alert job, slows the sink to 25 ms per delivery, triggers the run, waits for 300 deliveries, then `docker kill -s KILL` on the node that owns the run (read from `job_run.owner_node`).

Show in Grafana: the killed node's target goes down (`SchedulerTargetDown` turns pending in Prometheus), its partitions move to the other two within about 7 s, and "Orphan takeovers" ticks up on a survivor. The script ends with `RESULT: PASS`, the new token (higher than the old one), 1,200 distinct deliveries, `maxDeliveriesPerKey = 1`, and the takeover time. Expect about 10 s: the run lease is 10 s, the orphan sweep runs every 500 ms (bound 10.7 s; the script warns only above 11.2 s). The sink's `alreadyDelivered` shows the re-sent tail of the interrupted chunk being absorbed.

Doing it by hand instead: find the owner with `$PSQL "SELECT owner_node, fencing_epoch, fencing_seq FROM job_run WHERE id = <run>"`, then `docker compose kill -s SIGKILL <owner>`, and `docker compose start <owner>` afterwards.

## 5:30 Freeze a node: stale writes rejected (1 min 30 s)

```bash
chaos/freeze-during-alerts.sh
```

It `docker pause`s the owning node for 30 s mid-send, then unpauses it. Say: the frozen node's lease expires after 10 s; another node takes the run over with a higher token and finishes it while the first is still frozen. On wake-up the old node's monotonic deadline has passed, so it stops; a send that was already in flight reaches the sink with the old token and gets `409 STALE_TOKEN`; its renew or commit with the old token matches 0 rows. If the pause caught it inside a transaction, Postgres (`idle_in_transaction_session_timeout=5s`) ended that session, so its row locks did not block the takeover.

Show: the script's before → after lines for `scheduler_stale_commit_rejected_total` and `scheduler_lease_lost_total{reason=stale_409|fenced_write_0_rows|self_fence}` on the frozen node (at least one rises; usually `self_fence`, because the node stops before reaching its commit), the sink's `staleRejected` (often 0 when the pause landed between two sends; the script only warns then), and 1,200 distinct deliveries. In Grafana: "Lease lost by reason" and "Stale commits".

## 7:00 Dead letter and replay (45 s)

```bash
curl -s -X PUT localhost:8090/admin/faults -H 'X-Admin-Token: dev-only-sink-admin-token' \
  -H 'Content-Type: application/json' -d '{"failureRate":1.0,"failureStatus":503}'
RUN_AT=$(date -u -d '+5 seconds' +%FT%TZ)
curl -s -X POST localhost:8081/api/v1/jobs -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"kind":"ONE_OFF","handler":"PICKUP_ALERT","runAt":"'"$RUN_AT"'","maxRunTime":"PT1M","maxAttempts":2,
       "payload":{"employeeIds":["emp-0100"],"messageTemplate":"Cab at {pickupTime}","pickupTime":"09:00"}}' | jq -r .id
sleep 20
curl -s "localhost:8081/api/v1/dead-letters?tenant=acme" -H "Authorization: Bearer $OP" | jq '.items[0]'
DEAD=$(curl -s "localhost:8081/api/v1/dead-letters?tenant=acme" -H "Authorization: Bearer $OP" | jq -r '.items[0].runId')
curl -s -X DELETE localhost:8090/admin/faults -H 'X-Admin-Token: dev-only-sink-admin-token'
curl -s -X POST localhost:8081/api/v1/dead-letters/$DEAD/replay -H "Authorization: Bearer $OP" | jq
```

Show: the dead letter carries only `errorCode: SINK_5XX` and `attempts`, never the payload. The replay puts the **same run and idempotency key** back to `SCHEDULED`; a second replay is `409 DEAD_LETTER_ALREADY_REPLAYED`. `maxAttempts: 2` keeps the backoff short for the video (the default is 5, with full-jitter backoff from 2 s up to 5 min). In Grafana the "Dead letters by error code" panel shows `SINK_5XX`, and `SchedulerDeadLettersIncreasing` fires in Prometheus.

## 7:45 Tenant fairness and caps (30 s)

```bash
curl -s -X PUT localhost:8081/api/v1/tenants/acme/cap -H "Authorization: Bearer $ADMIN" \
  -H 'Content-Type: application/json' -d '{"cap":16}' | jq
```

Show: `configuredCap`, `effectiveCap` (slot rows) and `draining`. Say: the cap is enforced by the database (one slot row per unit of concurrency, CHECK + FK + unique index), so it holds during kills and partition moves (`TenantCapIT` samples it every 10 ms with no tolerance). Quote the measured burst: tenant A's 50,000 alerts drained at 698 runs/s in 71.6 s while tenant B's 20 alerts started within 185 ms.

## 8:15 RBAC and tenant isolation (30 s)

```bash
curl -s -o /dev/null -w '%{http_code}\n' localhost:8081/api/v1/jobs/$JOB -H "Authorization: Bearer $ADMIN_B"   # 404
curl -s localhost:8081/api/v1/jobs/$JOB -H "Authorization: Bearer $ADMIN_B" | jq '{status, code, traceId}'
curl -s -X POST localhost:8081/api/v1/jobs -H "Authorization: Bearer $EMP" -H 'Content-Type: application/json' \
  -d '{"kind":"RECURRING","handler":"ROSTER_GEN","cron":"0 22 * * *","payload":{},"maxRunTime":"PT5M"}' | jq '{status, code}'   # 403
curl -s localhost:8081/api/v1/jobs/$JOB -H "Authorization: Bearer $EMP" | jq '{payloadRedacted, payload}'
```

Show: another tenant's job is a 404 `JOB_NOT_FOUND`, indistinguishable from a job that does not exist; a wrong role is a 403; the EMPLOYEE sees the job without its payload. Then `docker compose logs --no-log-prefix scheduler-1 | tail -3`: JSON lines with `traceId`, `tenantId`, run MDC fields and no phone numbers.

## 8:45 Tests and benchmark (45 s)

Show a finished `./mvnw -B verify` (about 6 minutes, recorded beforehand): **855 tests, 853 passed, 2 skipped** (the Docker-gated `FailoverFencingIT` and `StrictSyncIT`), on embedded PostgreSQL, no Docker. Live, run a fast subset:

```bash
./mvnw -B -pl scheduler-app -am verify -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test='FencingIT,TenantIsolationIT' -Dfailsafe.failIfNoSpecifiedTests=false
```

Then the benchmark table from `README.md` / `docs/BENCHMARK.md`: 100,000 jobs over 10 minutes on 3 nodes, **p99 237 ms** against the 2 s target, 0 duplicates; with a kill and a 30 s freeze, p99 291 ms overall and 0 duplicates, but p99 3.7 s / 5.3 s for runs that fell due in the victim's partitions during the next minute, because new runs there wait for the 6 s partition lease. The knee on this laptop is between 900 and 1,000 runs/s.

Close on the limits slide: `README.md` "Known limitations".
