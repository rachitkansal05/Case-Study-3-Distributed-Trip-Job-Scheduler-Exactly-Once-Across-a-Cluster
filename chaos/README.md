# Chaos scripts

Two end-to-end failure drills against the docker compose cluster. Each one sends 1,200 pickup alerts, breaks the node that is sending them, and then checks with the notification sink that every employee got exactly one alert.

| Script | Failure | Main assertion |
|---|---|---|
| `kill-during-alerts.sh` | `docker kill -s KILL` of the owning node (PLAN §12, row 1) | Another node takes the run over with a higher token; 1,200 distinct deliveries; `maxDeliveriesPerKey == 1`; prints the takeover time |
| `freeze-during-alerts.sh` | `docker pause` of the owning node for 30 s, then `docker unpause` (PLAN §11) | The frozen node's fencing counters rise; sink `staleRejected > 0` (warning if 0); 1,200 distinct deliveries; `maxDeliveriesPerKey == 1` |

> **Not run here.** The machine these scripts were written on has no Docker. They have been checked with `bash -n` only (shellcheck is not installed there either, but the code follows its rules). Expect to fix small things on the first real run.

## Prerequisites

- Docker with Compose v2 (`docker compose`), bash 4 or newer, `curl`, `jq`, `awk`.
- **The `dev` Spring profile on the scheduler nodes.** `POST /dev/token` exists only in the `dev` profile (`DevTokenController` is `@Profile("dev")`). `docker-compose.yml` now sets `SPRING_PROFILES_ACTIVE: json,dev` itself, so a plain `docker compose up -d --build` is enough. `chaos/docker-compose.chaos.yml` sets the same value and is now redundant; it is kept because the scripts still pass it by default (below), and it does no harm:

  ```bash
  docker compose -f docker-compose.yml -f chaos/docker-compose.chaos.yml up -d --build
  ```

  The scripts pass the same two `-f` files to every `docker compose` call unless `COMPOSE_FILE` is set. Never run the `dev` profile in production: it mints tokens for any tenant and role without authentication.

## Usage

Run them from anywhere. They `cd` to the repository root. If the executable bit was lost (for example in a Windows checkout), run them with `bash chaos/<script>.sh`.

```bash
chaos/kill-during-alerts.sh               # restarts the killed service at the end
chaos/kill-during-alerts.sh --no-restart  # leaves it stopped
chaos/freeze-during-alerts.sh
FREEZE_SECONDS=45 chaos/freeze-during-alerts.sh
```

Exit code 0 means every assertion held. On any failure the script prints `FAIL: <reason>` and exits 1. On every exit path it clears the sink fault settings, disables the test job, and unpauses or restarts the node.

## Environment

| Variable | Default | Meaning |
|---|---|---|
| `SCHEDULER_URLS` | `scheduler-1=http://localhost:8081,scheduler-2=http://localhost:8082,scheduler-3=http://localhost:8083` | Compose service name = node id → base URL. Used for the API and for `/actuator/prometheus` |
| `SINK_URL` | `http://localhost:8090` | notification-sink base URL |
| `SINK_ADMIN_TOKEN` | `dev-only-sink-admin-token` | `X-Admin-Token` for `/admin/faults` |
| `COMPOSE_FILE` | unset → `-f docker-compose.yml -f chaos/docker-compose.chaos.yml` | When set, `docker compose` reads it itself |
| `PSQL` | `docker compose <files> exec -T postgres psql -U scheduler -d scheduler` | Command that runs psql against the scheduler DB, split on whitespace. Example: `PSQL="psql -h localhost -U scheduler -d scheduler"` with `PGPASSWORD=scheduler` |
| `EMPLOYEES` | `1200` | Employees in the payload (ids `emp-0001` …) |
| `SINK_LATENCY_MS` | `25` | Latency injected into the sink (`PUT /admin/faults`) so the run lasts about 30 s and can be hit mid-run |
| `KILL_AFTER` / `FREEZE_AFTER` | `300` | Distinct deliveries the sink must show before the failure is injected |
| `FREEZE_SECONDS` | `30` | Pause length. Must be above the 10 s run lease |
| `FENCE_WAIT` | `30` | Seconds after unpause to wait for the frozen node's counters to move |
| `RUN_TIMEOUT` | `300` | Seconds to wait for `SUCCEEDED` |
| `LEASE_SECONDS` | `10` | Run lease (`scheduler.run-lease.duration`). Used only to compute the takeover time |

## What the scripts do

Shared setup (`lib.sh`, `setup_alert_run`). Each step was checked against the code:

1. `POST /dev/token` with `{"tenant","role","subject"}` → `{token}`. An OPERATOR token (tenant `platform`) and an ADMIN token for the new tenant (`DevTokenController.TokenRequest`).
2. `POST /api/v1/tenants` as **OPERATOR** (the only role allowed, see `JwtSecurityConfig`). The body is `{"id":"chaos-kill-<epoch>","timeZone":"Asia/Kolkata","cap":4}` (`CreateTenantRequest`).
3. `POST /api/v1/jobs` as **ADMIN** of that tenant (ADMIN only; the tenant comes from the token). The body is `{"kind":"ONE_OFF","handler":"PICKUP_ALERT","runAt":<now+1 day>,"priority":1,"maxRunTime":"PT15M","maxAttempts":5,"payload":{"employeeIds":["emp-0001",…],"messageTemplate":"…{employeeId}"}}` (`CreateJobRequest`, `PickupAlertHandler`). `runAt` is a day away, so the manual trigger is the only run that executes.
4. `PUT /admin/faults {"latencyMs":25}` on the sink. Without it, 1,200 deliveries finish before the script can strike.
5. `POST /api/v1/jobs/{id}/trigger` with `Idempotency-Key` → `202 {runId,…}` (`TriggerView`).
6. Poll `GET /api/v1/jobs/{id}/runs` until the run is `RUNNING`. `RunView` has no owner field, so the owner is read from `job_run.owner_node` through psql. Node ids equal the compose service names (`NODE_ID` in `docker-compose.yml`).
7. Wait until `GET /api/v1/deliveries/stats?runId=` shows `deliveredDistinct >= 300`.

### `kill-during-alerts.sh`

- Reads the DB clock (`clock_timestamp()`), then runs `docker kill -s KILL $(docker compose ps -q <owner>)`.
- Polls `job_run` every 0.5 s until the run has a different `(fencing_epoch, fencing_seq)`. It checks the token rather than the owner, because a restart policy could bring a node back under the same name. The claim instant is `lease_until − 10 s` on first sight. Renewals run every 3 s, so a 0.5 s poll catches the claim before the first renewal.
- Waits for `SUCCEEDED`, then asserts `deliveredDistinct == 1200` for the run and `maxDeliveriesPerKey == 1` in the sink totals.
- Prints the takeover time (kill → new claim, DB clock) and the time from the old `lease_until` to the new claim. Above 11.2 s (PLAN §11) it prints a warning but does not fail.
- Restarts the killed service with `docker compose start` unless `--no-restart` is given.

### `freeze-during-alerts.sh`

- Before the pause it scrapes the owner's `/actuator/prometheus` and sums every series of `scheduler_stale_commit_rejected_total`, plus `scheduler_lease_lost_total` for each reason: `stale_409`, `fenced_write_0_rows`, `self_fence` (the tags come from `LeaseLostReason.metricTag()`).
- Runs `docker pause` for `FREEZE_SECONDS`, then `docker unpause`, then waits for `SUCCEEDED`.
- **Fencing assertion:** stale commits **or** any lease-lost reason must rise on the frozen node within `FENCE_WAIT` s. The brief asks for the stale-commit counter, but per PLAN §11 a frozen node normally never reaches its commit. On wake-up it self-fences (`self_fence`), or an in-flight send gets a 409 (`stale_409`), or its next renew or checkpoint matches 0 rows (`fenced_write_0_rows`). Any of these stops the handler first. All four counters are printed.
- **Sink `staleRejected > 0`** is a *warning* when it is 0, not a failure. A 409 needs a request with the old token to reach the sink after the new owner's first send. When the pause lands between two sends (the common case), the node self-fences on wake-up and sends nothing more.
- Asserts `deliveredDistinct == 1200` and `maxDeliveriesPerKey == 1`.

## Expected output (shape)

```
[10:02:01] tenant chaos-kill-1793354521 created
[10:02:01] job 5b0c… created (1200 employees)
[10:02:01] sink latency set to 25 ms per delivery
[10:02:01] triggered run 4182
[10:02:02] run 4182 is RUNNING on scheduler-2
[10:02:10] sink shows 302 delivered for run 4182
[10:02:10] killed scheduler-2 (container 3f9a2c1d7e4b) at DB time 1793354530.412; token was 1.5531
[10:02:21] run 4182 taken over by scheduler-1 with token 1.5534
[10:02:45] run 4182 SUCCEEDED
[10:02:45] sink run stats: {"runId":4182,"deliveredDistinct":1200,"attempts":1236,"delivered":1200,"alreadyDelivered":36,"staleRejected":0}
[10:02:45] OK: 1200 distinct deliveries, maxDeliveriesPerKey = 1

RESULT: PASS
  run              4182 (1200 employees)
  killed           scheduler-2 at token 1.5531
  new owner        scheduler-1 at token 1.5534
  takeover time    10.84 s (kill -> new claim, DB clock; budget 11.2 s)
  after expiry     0.95 s (old lease_until -> new claim)
```

The freeze script ends with a similar block that lists the four counters `before -> after` and the sink's `staleRejected`.

## Known caveats

- **Row locks held by a frozen node.** `docker pause` freezes the JVM but leaves its Postgres connections open. If the pause lands inside a transaction (a checkpoint `UPDATE`, for example), the row stays locked and the orphan sweep on the other node waits until unpause. The compose Postgres now runs with `idle_in_transaction_session_timeout=5s`, which ends such a session after 5 s and releases its locks, so the takeover is delayed by at most a few seconds. The freeze script still prints a warning when the token did not change during the freeze.
- `maxDeliveriesPerKey` is global over the whole sink, so leftovers from earlier runs also count. A value above 1 is a real bug either way.
- The kill script uses `docker kill`. Docker treats it as a manual stop, so `restart: unless-stopped` should not bring the node back by itself. The script restarts it with `docker compose start`.
