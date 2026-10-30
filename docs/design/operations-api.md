# Operations: controls, API and configuration

> Design docs index: [README.md](README.md) · Full design: [architecture.md §13](architecture.md#13-api-and-access-control) · Sink API: [`docs/sink-api.md`](../sink-api.md) · Postman: [`docs/postman/`](../postman/)

**Requirement:** pause or resume all jobs of a tenant, list the next fire times of a job, and
trigger a job manually.

## 1. Endpoints

All endpoints are under `/api/v1` and need a bearer token ([security](security-and-tenant-isolation.md)).
A wrong role gets `403 FORBIDDEN`; a missing or bad token gets `401 UNAUTHORIZED`.

| Method and path | Purpose | Roles | `Idempotency-Key` |
|---|---|---|---|
| `POST /tenants` | Create a tenant: `{"id","timeZone","cap"}` (cap optional, default 32) | OPERATOR | ignored (a repeat gets `409 TENANT_EXISTS`) |
| `POST /jobs` | Create a one-off or recurring job (201) | ADMIN | optional |
| `GET /jobs/{id}` | Job definition; payload only for ADMIN, `payloadRedacted: true` otherwise | ADMIN, EMPLOYEE, OPERATOR | — |
| `GET /jobs/{id}/next-fire-times?count=10` | **Next 1–100 fire times** with local time and offset | ADMIN, EMPLOYEE | — |
| `POST /jobs/{id}/trigger` | **Manual trigger** (202); a repeat returns the same run with `duplicate: true` | ADMIN, OPERATOR | **required** |
| `DELETE /jobs/{id}` | Disable a job and cancel its unstarted runs | ADMIN | — |
| `GET /jobs/{id}/runs?page=0&size=50` | Run history: state, attempt, lag, error code | ADMIN, OPERATOR | — |
| `POST /tenants/{t}/pause` · `/resume` | **Pause or resume all of a tenant's jobs** | ADMIN (own tenant), OPERATOR | optional |
| `PUT /tenants/{t}/cap` | Change the cap (`{"cap": 0..10000}`) | ADMIN (own tenant), OPERATOR | optional |
| `GET /dead-letters?tenant=&page=&size=` | Inspect dead-lettered runs | OPERATOR, ADMIN (own) | — |
| `POST /dead-letters/{runId}/replay` | Replay with the same idempotency key | OPERATOR | optional |
| `GET /cluster` | Live nodes, partitions per node, every partition's owner and epoch | OPERATOR | — |

`POST /dev/token` (outside `/api/v1`) mints demo tokens and exists only in the `dev` profile.
Lists are paged with `page` / `size` (≤ 200) and return `{"items", "page", "size", "hasMore"}`.

## 2. How the controls work

- **Pause / resume.** Pausing does not change run states: the runs stay `SCHEDULED` and every
  claim skips the tenant (it reads `tenant.paused` directly, never from a cache), so a pause
  takes effect on the next claim on every node. Runs already running finish. Resuming stamps
  `tenant.resumed_at`; runs that fell due during the pause are then handled by the job's misfire
  policy ([misfires](misfires-retries-and-dead-letters.md)).
- **Next fire times.** Computed by the same `NextFireCalculator` and `WallClockResolver` the
  materializer uses, so what the API shows is what will run, including DST ([time zones](job-model-and-time-zones.md#4-time-zones-and-dst)).
  Cached for 30 s per `(jobId, version, count)` ([caching](caching.md)).
- **Manual trigger.** Inserts a run due now with key `{jobId}:manual:{Idempotency-Key}`, which is
  unique forever, so a double click or a client retry creates one run. Manual runs are exempt
  from the misfire policy.

## 3. Retry-safe writes

- With an `Idempotency-Key` (1–128 printable ASCII characters), the action and its stored
  response commit in **one transaction**.
- A retry gets the original status, `Location` and body with `Idempotent-Replayed: true`, and
  nothing runs a second time.
- Reusing a key with a different request is `409 IDEMPOTENCY_KEY_REUSED` (a SHA-256 of the
  canonical request is stored with the key).
- Keys are namespaced per tenant (operators have their own namespace) and honoured for
  **24 hours**. `IdempotencyPurge` deletes older records every 10 minutes in batches of 1,000,
  with `FOR UPDATE SKIP LOCKED`, so it is safe on all nodes at once.

## 4. Configuration

`scheduler-app` keys, bound to `SchedulerProperties` and validated at startup (inconsistent
timings stop the node):

| Key | Default | Env override |
|---|---|---|
| `scheduler.node-id` | `node-local` | `NODE_ID` |
| `scheduler.partitions` | `128` | |
| `scheduler.run-lease.duration` / `.renew-interval` | `10s` / `3s` | |
| `scheduler.partition-lease.duration` / `.renew-interval` / `.loop-interval` | `6s` / `2s` / `1s` | |
| `scheduler.membership.heartbeat-interval` / `.dead-after` | `2s` / `6s` | |
| `scheduler.claim.poll-interval` | `200ms` | |
| `scheduler.orphan-sweep.interval` | `500ms` | |
| `scheduler.workers.count` / `.reserved-for-recovery` | `64` / `1` | |
| `scheduler.self-fence-margin` | `2s` | |
| `scheduler.tenant.default-cap` | `32` | |
| `scheduler.misfire.default-threshold` | `60s` | |
| `scheduler.retry.max-attempts` / `.lease-lost-limit` / `.backoff-base` / `.backoff-cap` | `5` / `20` / `2s` / `5m` | |
| `scheduler.api.idempotency-purge.interval` / `.batch-size` / `.max-batches` | `10m` / `1000` / `100` | |
| `scheduler.run-purge.retention` / `.interval` / `.batch-size` / `.max-batches` | `7d` / `10m` / `1000` / `100` | |
| `scheduler.engine.*` | see `EngineProperties` (materializer, compactor, metrics, epoch guard, sink client timeouts) | |
| `scheduler.sink.base-url` | `http://localhost:8090` | `SCHEDULER_SINK_BASE_URL` |
| `scheduler.security.jwt-hmac-secret` | dev-only key (refused outside `dev`/`test`) | `SCHEDULER_JWT_HMAC_SECRET` |
| `spring.datasource.url` / `username` / `password` | local `scheduler` DB | `SPRING_DATASOURCE_URL` / `_USERNAME` / `_PASSWORD` |

## 5. Tests

| Test | What it proves |
|---|---|
| `OpsApiIT` | Pause/resume, next fire times, manual trigger, dead-letter list and replay, cluster view |
| `ApiIdempotencyIT` | Same key → same response, no second action; reused key with another body → 409 |
| `IdempotencyPurgeIT` | Records older than 24 h are purged, safely on several nodes |
| `CapApiIT` | The cap endpoint and its response |
| `JobValidationIT` | Validation errors for every job field |
| `SchedulerPropertiesTest` | Inconsistent timing knobs stop the node at startup |
