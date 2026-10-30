# Trade-offs and deliberate omissions

> Design docs index: [README.md](README.md) · Full design: [architecture.md §18](architecture.md#18-trade-offs-and-deliberate-omissions)

**Requirement:** clearly define the trade-offs made, especially consistency vs availability vs
latency, and say what was deliberately left out.

## 1. Consistency vs availability vs latency

| Choice | What we gain | What it costs |
|---|---|---|
| **Consistency over availability.** If the database is unreachable, nothing fires | No job can fire twice from stale local state | During a DB outage no job runs; misfire policies handle the backlog afterwards |
| **Durability over availability on failover.** Strict synchronous replication; only the synchronous standby can be promoted | No acknowledged job is lost and tokens never go backwards | Writes block when no standby is available; about one LAN round trip per commit (*estimate* ≈ 1 ms) |
| **Latency vs DB load.** A 200 ms poll plus an immediate claim when a slot frees | p50 lag ≈ 105 ms, p99 237 ms at the target load | Claim load grows with tenants × throughput; LISTEN/NOTIFY could cut lag further |
| **Strict lease expiry.** A write after the lease expired is rejected even if nobody took over | Slot reuse is race-free; no zombie can commit | An occasional rerun, which the idempotency keys deduplicate |

## 2. Other trade-offs

| Choice | Alternative | Why |
|---|---|---|
| PostgreSQL as the only coordinator and clock | etcd / ZooKeeper / Kafka | One dependency, transactional state and one trusted clock; accepted a write ceiling ([performance](performance-and-benchmark.md#6-capacity-summary)) |
| 10 s run lease | shorter | Takeover in ≤ 10.7 s; a shorter lease takes over faster but causes more false takeovers in GC pauses |
| 6 s partition lease | shorter | New runs in a dead node's partitions wait up to about 7 s (the benchmark's only p99 misses); shorter means more lease writes |
| Hard per-tenant caps | best-effort sharing | Isolation between tenants; capacity sits idle during a single-tenant burst |
| Exact caps via slot rows | per-node cap shares | Exact at all times, at the cost of one slot-row lock per run start; shares could exceed the cap while partitions move |
| Deferred cap reduction | revoke leases | Running jobs are never stopped; the new cap is reached within the longest remaining run time |
| Misfire measured from "became claimable" | lateness alone | A burst queued behind its cap is not skipped; a rolling restart of every node can still count as an outage |
| In-process cache (Caffeine) | Redis | Nothing correctness-critical is cached, so a shared cache would add a dependency without protecting anything ([caching](caching.md)) |
| Rendezvous hashing over 128 partitions | consistent hashing ring, more partitions | Minimal movement and no coordination; balance is statistical (max/min about 1.3); 128 partitions cap useful nodes at about 40 |

## 3. Left out on purpose

- Multi-region scheduling.
- Exactly-once effects against APIs that ignore idempotency keys (impossible for any scheduler).
- Job dependencies or DAGs, fixed-rate schedules, sub-second precision.
- An admin UI.
- Real SMS and push providers (a mock sink stands in).
- A full OAuth2 identity provider (HS256 tokens; `/dev/token` for the demo).
- Our own database failover manager (Patroni is relied on).
- APIs to change a tenant's time zone or edit a job.
