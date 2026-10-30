# Code structure: modules, layering and object-oriented design

> Design docs index: [README.md](README.md) · Full design: [architecture.md §03](architecture.md#03-technology-choices), [§15](architecture.md#15-code-layout-and-oop-design)

**Requirement:** clean controller / service / repository layering; encapsulation, inheritance and
polymorphism for maintainable, extensible code; the core algorithm in plain classes that can be
tested without a web server or database.

## 1. Modules

```
├─ scheduler-core/      plain Java, no Spring, no JDBC, no logging framework:
│                       domain types, cron and DST, misfire and backoff policies, partition
│                       assignment, fairness, lease clock, run state machine, ports
├─ scheduler-app/       Spring Boot 3.3 application (the scheduler node)
├─ notification-sink/   mock SMS/push provider that enforces idempotency keys and fencing tokens
├─ loadgen/             compose-mode load generator; test scope: InProcessBench and end-to-end ITs
├─ chaos/               kill and freeze scripts for the Docker stack
├─ deploy/patroni/      production database failover configuration
└─ ops/                 Prometheus config and alert rules, Grafana dashboard, Toxiproxy proxies
```

Base packages: `com.moveinsync.scheduler.core.*`, `.app.*`, `.sink.*`, `.loadgen.*`.

## 2. Layering in `scheduler-app`

```
api/            controllers (JobController, TenantController, DeadLetterController,
   │            JobRunController, ClusterController) and DTOs; no business logic
   ▼
service/        business rules (JobService, TenantService, DeadLetterService, RunService,
   │            IdempotencyService, ClusterService …) and caches
   ▼
repository/     SQL only: job/, tenant/, deadletter/, idempotency/, cluster/, claim/
                every API repository query takes a TenantScope

engine/         the scheduling loops (SchedulerNode, MembershipLoop, PartitionLoop, Poller,
                Dispatcher, RunExecutor, LeaseRenewer, OrphanSweep, SlotCompactor, Materializer)
handler/        one class per job type
sink/           HttpEffectSink, the client of the notification sink
security/       JWT, roles, TenantScope, /dev/token (dev profile only)
error/          the exception hierarchy and the RFC 7807 advice
observability/  PII masking for logs
```

## 3. Encapsulation

- **One writer.** All engine writes to run state and tenant slots go through
  `JdbcClaimProtocol`, which owns the lock order (slot row, then run row) and the fenced
  predicates. Its fenced methods return `false` when the lease is lost.
- **Legal transitions only.** `RunStateMachine` holds the allowed run-state transitions.
- **Immutable values.** A claimed run is a record (`ClaimedRun`, `RunContext`); `FencingToken`
  and `TenantId` validate themselves, so invalid ones cannot be built. There is no mutable
  `JobRun` entity.

## 4. Inheritance

| Base | Subclasses | What the base provides |
|---|---|---|
| `PeriodicLoop` | `Poller`, `OrphanSweep`, `PartitionLoop`, `MembershipLoop`, `SlotCompactor`, `TaskLoop` | Scheduling, the node gate (pause/kill), error isolation |
| `JobHandler` → `ChunkedJobHandler` | `PickupAlertHandler` (chunked, checkpointed), `RosterGenerationHandler`, `NoShowCancelHandler`, `BillingCloseHandler` | Template method: validate → execute → classify errors; `ChunkedJobHandler` adds checkpointing |
| `SchedulerException` (sealed) | `NotFoundException`, `ConflictException`, `ForbiddenException`, `ValidationException`, `StoreUnavailableException` | HTTP status and stable error code |

## 5. Polymorphism

| Abstraction | Implementations |
|---|---|
| `MisfirePolicy` (sealed) | `FireNow`, `Skip`, `Coalesce`, chosen per job by `MisfirePolicy.of(type)` |
| `JobDefinition` (sealed) | `OneOffJob`, `RecurringJob` |
| `RunFailure` (sealed) | `Retry`, `DeadLetter` |
| `BackoffPolicy` | `ExponentialFullJitterBackoff` |
| `HandlerRegistry` | dispatches a run to its `JobHandler` by type |

## 6. Ports and adapters

The engine depends on interfaces from `scheduler-core.ports`, so the core runs against in-memory
fakes in unit tests and against PostgreSQL in production:

| Port | Production adapter | What it does |
|---|---|---|
| `ClaimProtocol` | `JdbcClaimProtocol` | claim, sweep orphans, fenced renew / checkpoint / commit |
| `PartitionLeaseStore` | `JdbcPartitionLeaseStore` | acquire, renew, release partition leases |
| `MembershipStore` | `JdbcMembershipStore` | heartbeats, live nodes |
| `EffectSink` | `HttpEffectSink` | send an effect with key and token; highest accepted epoch |
| `SchedulerClock` | `DbSchedulerClock` | authoritative DB time; local monotonic clock for self-fencing only |

## 7. Database migrations

Flyway migrations in `scheduler-app/src/main/resources/db/migration`, schema `public`:
`V1` (schema), `V200–V204` (engine indexes, claim order, misfire exemption, run retention index,
misfire `claimable_since`). An applied migration is never edited; changes go in a new one. The
sink has its own `sink` schema and Flyway history. In compose both share one database.

Key schema choices: `UNIQUE (job_id, scheduled_at)` and `UNIQUE (idem_key)` for idempotency;
`run_due (partition_id, tenant_id, priority, due_at)` for claims; 128 seeded `partition_lease`
rows; the slot constraints described in [fairness §3](tenant-fairness-and-caps.md#3-why-the-cap-always-holds).
