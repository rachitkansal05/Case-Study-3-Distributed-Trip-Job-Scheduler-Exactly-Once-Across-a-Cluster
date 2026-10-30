# Testing: every hard rule has an automated test

> Design docs index: [README.md](README.md) · Full design: [architecture.md §16](architecture.md#16-test-matrix-every-hard-rule-is-automated)

**Requirement:** every hard rule of the case study has an automated test, including at least one
concurrency test and one failure test. Unit tests for the core logic, plus the concurrency,
failure and benchmark tests.

## 1. How to run

```bash
./mvnw -B verify          # Windows: mvnw.cmd -B verify. No Docker needed
```

- Unit tests (`*Test`) run under Surefire, integration tests (`*IT`) under Failsafe.
- Integration tests run on **embedded PostgreSQL 16** (zonky), so Docker is not needed. Each test
  class gets its own database.
- Only the two failover tests use Testcontainers (`FailoverFencingIT`, `StrictSyncIT`); they are
  `@Testcontainers(disabledWithoutDocker = true)` and skip without Docker. CI on
  `ubuntu-latest` runs them.
- On Windows, do not run the build from an Administrator shell: Postgres refuses to start as an
  administrator.
- CI: `.github/workflows/ci.yml` runs `./mvnw -B verify` on JDK 21 and uploads the test reports
  when it fails.

One test:

```bash
./mvnw -B -pl scheduler-app -am verify -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test=NodeCrashIT -Dfailsafe.failIfNoSpecifiedTests=false
```

## 2. Counts

| Module | Unit | Integration |
|---|---|---|
| `scheduler-core` | 322 | — |
| `scheduler-app` | 186 | 266 (2 Docker-gated) |
| `notification-sink` | 47 | 35 |
| `loadgen` | 9 | 3 |
| **Total** | | **868 tests** |

The core logic (`scheduler-core`: cron, DST, misfire and backoff policies, partition assignment,
fairness, lease clock, state machine) is plain Java and is tested with in-memory fakes, without
a web server or a database.

## 3. Rule-to-test matrix

| Rule | Key tests | Kind |
|---|---|---|
| One-off and recurring jobs | `JobDefinitionTest`, `JobValidationIT` | unit, integration |
| Time zones and DST | `WallClockResolverTest`, `NextFireTimesTest`, `NextFireCalculatorTest`, `CronScheduleTest`, `MaterializerIT` | unit, integration |
| Exactly-once effect | `ExactlyOnceEffectIT` (3 nodes, 10,000 jobs, failures after the send: 0 duplicates), `ConcurrentDeliveryIT` | **concurrency, failure** |
| Leases and fencing | `FencingIT`, `ClaimRaceIT` (32 threads), `SlotStealRaceIT` (both interleavings forced), `LeaseResurrectionIT`, `FencingTokenTest`, `LeaseClockTest` | **concurrency** |
| Tokens survive failover | `EpochGuardIT`, `ClusterGenerationGuardIT`, `FailoverFencingIT`, `StrictSyncIT` | failure |
| Partitions move on join and leave | `PartitionAssignerTest`, `PartitionPlannerTest`, `PartitionHasherTest`, `RebalanceIT` | unit, failure |
| Misfire policies | `MisfirePolicyTest`, `MisfireIT`, `BurstSkipIT`, `ReplayMisfireIT` | unit, integration |
| Fairness and caps | `TenantFairnessIT`, `DrainRateIT`, `TenantCapIT` (no tolerance), `LockOrderIT`, `SchemaIT`, `CapReductionIT`, `CapIncreaseIT`, `DeficitRoundRobinDispatcherTest` | **concurrency** |
| Retries and dead letters | `BackoffPolicyTest`, `RetryDeciderTest`, `RetryToDeadLetterIT`, `FaultInjectionIT` | **failure injection** |
| Run state machine | `RunStateMachineTest` | unit |
| Ops API, retry-safe writes, caching | `OpsApiIT`, `ApiIdempotencyIT`, `IdempotencyPurgeIT`, `CachingIT`, `RunPurgeIT` | integration |
| Metrics, alerts, dashboard, logs | `MetricsIT`, `EngineMetricsTest`, `AlertRulesTest`, `GrafanaDashboardTest`, `RunbookTest`, `OpsMetricNamesIT`, `LoggingIT` | integration |
| `kill -9` during 1,200 alerts | `NodeCrashIT`, `NodeCrashBacklogIT`, `EndToEndKillIT`, `chaos/kill-during-alerts.sh` | **failure** |
| 30 s freeze, stale write rejected | `GcPauseIT`, `EndToEndFreezeIT`, `chaos/freeze-during-alerts.sh` | **failure** |
| Tenant isolation, roles, no PII | `TenantIsolationIT`, `RoleMatrixIT`, `JwtSecretGuardTest`, `DevTokenIT`, `PiiMaskingTest`, `ErrorResponseTest`, `PayloadPrivacyIT` | integration |
| 100k jobs in 10 minutes, p99 < 2 s | `InProcessBench` (`-Pbench`, not part of `verify`) | benchmark ([performance](performance-and-benchmark.md)) |

## 4. Proving the tests catch the bugs

The tests for the two most recent fixes were run against the old code first, to show they fail
without the fix:

| Test | With the fix | Old code |
|---|---|---|
| `NodeCrashBacklogIT` (kill while the tenant has 300 older due runs) | taken over 10.0 s after the last renewal | 15.3–15.6 s, all 300 ran first: fails the 11.2 s bound |
| `BurstSkipIT` (400 `SKIP` runs queued behind cap 4, 1 s threshold) | 0 skipped | fails: the queued tail is skipped |

## 5. Timing-sensitive tests

`DrainRateIT`, `TenantFairnessIT`, `RebalanceIT`, `NodeCrashIT` and `TenantCapIT` assert real
time bounds. They passed on the development laptop when the benchmark numbers were recorded. On
2026-10-30 the first three also failed on the unchanged previous commit, with the laptop's system
SSD (QLC) down to 10 GB free, so a slow disk can fail them without a code change. On shared CI
runners `DrainRateIT` asserts ≥ 450 runs/s instead of 600, still far above the 160 runs/s
poll-only ceiling it guards against.
