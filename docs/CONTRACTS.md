# Team contracts

This file says which team owns which code, how teams share the database and configuration, and how work gets committed. The design itself is in [`PLAN.md`](../PLAN.md). If code and plan disagree, raise it with the project manager rather than changing a shared contract on your own.

## 1. Module and package ownership

| Team | Owns | Notes |
|---|---|---|
| **Foundation** | Root build (`pom.xml`, `mvnw`, `.mvn/`), `scheduler-core` packages `domain` and `ports`, Flyway V1–V9, `application.yml` keys, `SchedulerProperties`, test support classes, `docker-compose.yml`, Dockerfiles, `.github/workflows/` | Shared contracts. Changes need agreement from every team that uses them. |
| **Core** | `scheduler-core` packages `time`, `policy`, `cluster`, `fairness`, `lease` | Plain Java only: no Spring, no JDBC, no logging framework. Unit tests use in-memory fakes of the `ports`. |
| **Sink** | `notification-sink` (all packages) and its `sink` schema migrations | Must implement the `EffectSink` contract (see the Javadoc). |
| **API** | `scheduler-app` packages `api`, `service`, `security`, `error`, plus the job, tenant and dead-letter repositories (`repository.job`, `repository.tenant`, `repository.deadletter`); Flyway V100–V199 | `security.BootstrapSecurityConfig` is a Foundation placeholder that the API team replaces. |
| **Engine** | `scheduler-app` packages `handler`, `repository.claim` (`JdbcClaimProtocol`), `repository.cluster` (`JdbcPartitionLeaseStore`, `JdbcMembershipStore`, `DbSchedulerClock`), `sink` (`HttpEffectSink`, the sink HTTP client); Flyway V200–V299 | `JdbcClaimProtocol` is the **only** engine writer of `job_run.state` and tenant slots (the API's four documented exceptions are listed in its Javadoc). The `engine` package moved to Reliability. |
| **Observability** | `scheduler-app` package `observability` (`PiiMasker`, `PiiMaskingConverter`, `PiiValueMasker`), `logback-spring.xml`, `ops/prometheus/`, `ops/grafana/`, `ops/metric-names.txt` | The metrics themselves are `engine.EngineMetrics` (Reliability). The dashboard and the 13 alert rules are checked by `GrafanaDashboardTest`, `AlertRulesTest` and `RunbookTest`. |
| **Reliability** | `scheduler-app` package `engine` (`SchedulerNode`, `Poller`, `Dispatcher`, `RunExecutor`, `OrphanSweep`, `EngineMetrics`, …), `chaos/`, `loadgen/`, `deploy/patroni/`, failure-injection ITs (`FailoverFencingIT`, `StrictSyncIT`, `NodeCrashIT`, `GcPauseIT`, `EndToEndKillIT`, `EndToEndFreezeIT`, …), `docs/BENCHMARK.md` | Toxiproxy proxies are defined in `ops/toxiproxy/toxiproxy.json`. `loadgen` test-depends on the non-repackaged `scheduler-app` jar (classifier `plain`, built into `scheduler-app/target/plain/` so the Dockerfile's `target/scheduler-app-*.jar` glob never picks it up) and on `notification-sink`. |
| **Docs** | `README.md`, `PLAN.md`, `docs/DESIGN.md`, `docs/DEMO.md`, `docs/postman/` | Every command, endpoint, property, metric and number must match the code and tests. |

Base packages: `com.moveinsync.scheduler.core.*`, `com.moveinsync.scheduler.app.*`, `com.moveinsync.scheduler.sink.*`, `com.moveinsync.scheduler.loadgen.*`.

## 2. Shared contracts (`scheduler-core`)

### `com.moveinsync.scheduler.core.domain`

| Type | Kind | Purpose |
|---|---|---|
| `JobKind` | enum | `ONE_OFF`, `RECURRING` (`job.kind`) |
| `HandlerType` | enum | `PICKUP_ALERT`, `ROSTER_GEN`, `NO_SHOW_CANCEL`, `BILLING_CLOSE` (`job.handler`) |
| `RunState` | enum | `SCHEDULED`, `PAUSED`, `RUNNING`, `RETRY_WAIT`, `SUCCEEDED`, `SKIPPED`, `DEAD_LETTERED`, `CANCELLED`; `isTerminal()`, `isClaimable()` |
| `MisfirePolicyType` | enum | `FIRE_NOW`, `SKIP`, `COALESCE` (`job.misfire`) |
| `LeaseLostReason` | enum | `STALE_TOKEN`, `FENCED_WRITE_REJECTED`, `SELF_FENCE`; `metricTag()` gives the `reason` label |
| `DeliveryOutcome` | enum | `DELIVERED`, `ALREADY_DELIVERED`, `STALE_TOKEN`; `isSuccess()` |
| `FencingToken(long epoch, long seq)` | record, `Comparable` | Epoch first, then seq. `toHeader()` / `parse(String)` use `"<epoch>.<seq>"`; `epochOf(int generation, long timeline)` = generation × 2^32 + timeline (fails closed when the timeline is ≤ 0); `isNewerThan`, `HEADER = "Fencing-Token"` |
| `TenantId(String value)` | record | 1–64 characters, no surrounding whitespace |
| `RunContext(runId, jobId, tenantId, attempt, idemKey, fencingToken, checkpointJson, deadline)` | record | What a handler receives. `itemKey(itemId)` = `idemKey:itemId` |
| `ClaimedRun(...)` | record | A committed claim: run, job, tenant, partition, jobVersion, handler, priority, slotNo, token, idemKey, scheduledAt, attempt, maxRunTime, claimedAt, leaseUntil, checkpointJson, coalescedCount, recovered |
| `RunFailure` | sealed interface | `Retry(delay, errorCode, safeMessage)` and `DeadLetter(errorCode, safeMessage)` |

### `com.moveinsync.scheduler.core.ports`

| Port | Implemented by | Methods |
|---|---|---|
| `SchedulerClock` | Engine (DB clock); tests (fake) | `Instant now()`: authoritative DB time. `long monotonicNanos()`: local clock, used only for self-fencing |
| `ClaimProtocol` | Engine `JdbcClaimProtocol` | `claimDue(nodeId, tenant, ownedPartitions, maxRuns)`, `sweepOrphans(nodeId, maxRuns)`, `renew(runId, token)`, `checkpoint(runId, token, json)`, `commitSuccess(runId, token)`, `commitFailure(runId, token, failure)`. Each fenced method returns `false` when the lease is lost. |
| `PartitionLeaseStore` | Engine | `tryAcquireOrRenew(p, nodeId, duration)`, `release(p, nodeId)`, `ownedBy(nodeId)`, `findAll()` |
| `MembershipStore` | Engine | `heartbeat(nodeId)`, `liveNodes(deadAfter)`, `deregister(nodeId)` |
| `EffectSink` | Engine (HTTP client of `notification-sink`) | `send(runId, idemKey, token, payloadJson)` returns a `DeliveryOutcome`; `highestAcceptedEpoch()` is used by the startup epoch guard |

The fenced write semantics are written out in the `ClaimProtocol` Javadoc, following PLAN §07. Every holder write must use this predicate:

```sql
WHERE id = :run AND (fencing_epoch, fencing_seq) = (:e, :s)
  AND state = 'RUNNING' AND lease_until >= clock_timestamp()
```

Locks are always taken slot row first, then run row. Time comparisons always use `clock_timestamp()`, never `now()`.

## 3. Database and Flyway

- **Scheduler** (`scheduler-app/src/main/resources/db/migration`, schema `public`): Foundation **V1–V9**, API **V100–V199**, Engine **V200–V299**. Never edit a migration once it has been applied anywhere; add a new one in your range instead. Flyway runs migrations in numeric order, so if you apply a newer migration to a local DB before an older one lands, recreate the local DB. Tests always start from an empty database.
- **Sink** (`notification-sink/src/main/resources/db/migration`, schema `sink`): numbered separately (V1, V2, …) and owned by the Sink team. The sink has its own `sink.flyway_schema_history` and never touches `public`.
- In docker-compose both services share one Postgres database (`scheduler`) and use different schemas.
- V1 notes for implementers:
  - `job_run.priority`, `job_version` and `max_run_time` are copied from `job` when a run is materialised.
  - The claim orders by `priority, due_at`, where 1 is the most urgent and the allowed range is 1–9.
  - `lease_lost_count` counts attempts lost to a lease loss, which have their own limit.
  - The `run_due` index is `(partition_id, tenant_id, priority, due_at)`.
  - `partition_lease` has 128 seeded rows with `owner_node = NULL`, so acquire must also match `owner_node IS NULL`.
  - `cluster_meta` has one row with generation 0.

## 4. Configuration keys (`scheduler-app`, `application.yml`, bound to `SchedulerProperties`)

| Key | Default | Env override |
|---|---|---|
| `scheduler.node-id` | `node-local` | `NODE_ID` |
| `scheduler.partitions` | `128` | |
| `scheduler.run-lease.duration` / `.renew-interval` | `10s` / `3s` | |
| `scheduler.partition-lease.duration` / `.renew-interval` / `.loop-interval` | `6s` / `2s` / `1s` | |
| `scheduler.membership.heartbeat-interval` / `.dead-after` | `2s` / `6s` | |
| `scheduler.claim.poll-interval` | `200ms` | |
| `scheduler.orphan-sweep.interval` | `500ms` (takeover bound 10 s + 0.5 s + 0.2 s = 10.7 s) | |
| `scheduler.workers.count` / `.reserved-for-recovery` | `64` / `1` | |
| `scheduler.self-fence-margin` | `2s` | |
| `scheduler.tenant.default-cap` | `32` | |
| `scheduler.misfire.default-threshold` | `60s` | |
| `scheduler.retry.max-attempts` / `.lease-lost-limit` / `.backoff-base` / `.backoff-cap` | `5` / `20` / `2s` / `5m` | |
| `scheduler.api.idempotency-purge.interval` / `.batch-size` / `.max-batches` | `10m` / `1000` / `100` (read with `@Value` in `IdempotencyPurge`, not part of `SchedulerProperties`; retention is fixed at 24 h) | |
| `scheduler.run-purge.retention` / `.interval` / `.batch-size` / `.max-batches` | `7d` / `10m` / `1000` / `100` (read with `@Value` in `RunPurge`, like the idempotency purge: finished runs older than the retention are deleted; dead-lettered runs are kept) | |
| `scheduler.engine.*` | see `EngineProperties` (`enabled`, materializer, compactor, metrics, clock, epoch guard, sink client timeouts) | |
| `scheduler.sink.base-url` | `http://localhost:8090` | `SCHEDULER_SINK_BASE_URL` |
| `scheduler.security.jwt-hmac-secret` | dev-only key | `SCHEDULER_JWT_HMAC_SECRET` (**set this outside dev**) |
| `spring.datasource.url` / `username` / `password` | local `scheduler` DB | `SPRING_DATASOURCE_URL` / `_USERNAME` / `_PASSWORD` |

Add new keys under `scheduler.<your-area>` and register them in `SchedulerProperties`, which is validated at startup. Ask Foundation to review new keys.

## 5. Build and tests

- Run `./mvnw -B verify` (on Windows, `mvnw.cmd -B verify`). Maven 3.9.9 is downloaded by the wrapper. Build with JDK 21 or newer; the output targets `--release 21`.
- Unit tests are named `*Test` and run under Surefire. Integration tests are named `*IT` and run under Failsafe in `verify`.
- Integration tests use **embedded PostgreSQL 16** (zonky), so Docker is not needed:
  - For a Spring context, extend `AbstractEmbeddedPostgresIT`.
  - For plain JDBC or Flyway with your own fresh database, use `EmbeddedPostgresSupport.createDatabase("my_it")`.
  - Each test module has one server per JVM. Use unique tenant IDs or your own database so tests do not interfere.
  - On Windows the binaries unpack to `%TEMP%`. Do not run the build from an elevated (Administrator) shell, because Postgres refuses to start as an administrator.
- Testcontainers tests (for example Toxiproxy or the Patroni failover tests) must be annotated `@Testcontainers(disabledWithoutDocker = true)`. Never use `@Disabled` to make the build green.

## 6. Commits

- Only team **managers** run `git add` / `git commit`. Subagents never run git commands that change state.
- Use [Conventional Commits](https://www.conventionalcommits.org/): `feat(engine): …`, `fix(api): …`, `test(core): …`, `build: …`, `docs: …`. Keep commits small and focused.
- Commits show only the author's name: no `Co-Authored-By` or other AI-attribution trailers.
- Work on your team branch (`team/<name>`). Do not push to `main`.
- `./mvnw -B verify` must pass before each commit that touches code.
- Never log payloads or personal data. Never commit secrets; dev defaults must be obviously fake and overridable by environment variables.
