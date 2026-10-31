# Design documents

All design documentation for the Distributed Trip Job Scheduler is in this folder. Start with
the overall architecture, then open the document of the feature you need. Each feature document
states the requirement it answers, the design, its limits and the tests that prove it.

## Overall architecture

**[architecture.md](architecture.md)** is the complete, detailed design: hard rules, guarantees,
technology choices, architecture, data model, every protocol, failure modes, API, observability,
code layout, test matrix, capacity math, trade-offs and the design-review fixes. Code comments
refer to it as "plan section NN".

## Feature design documents

| Feature | Document | Case-study requirement it answers |
|---|---|---|
| Job model, time zones and DST | [job-model-and-time-zones.md](job-model-and-time-zones.md) | One-off and cron jobs; tenant time zones; the 02:30 gap and the repeated 01:30 |
| Exactly-once effect, leases and fencing | [exactly-once-leases-and-fencing.md](exactly-once-leases-and-fencing.md) | At-least-once execution with an exactly-once effect; leases with fencing; why exactly-once execution is impossible; which clock decides |
| Partitioning and membership | [partitioning-and-membership.md](partitioning-and-membership.md) | No single hot lock; partitions move when nodes join or leave |
| Misfires, retries and dead letters | [misfires-retries-and-dead-letters.md](misfires-retries-and-dead-letters.md) | Fire now / skip / coalesce; exponential backoff with jitter; dead-letter list with replay |
| Per-tenant fairness and caps | [tenant-fairness-and-caps.md](tenant-fairness-and-caps.md) | 50,000 alerts must not delay 20; per-tenant concurrency cap, with numbers |
| Operations API and configuration | [operations-api.md](operations-api.md) | Pause/resume a tenant, next fire times, manual trigger; retry-safe writes |
| Failure handling and chaos tests | [failure-handling.md](failure-handling.md) | The kill and freeze use cases; crashes, timeouts, duplicates, out-of-order and partial failures; failure-mode table; DB failover |
| Performance, benchmark and cost | [performance-and-benchmark.md](performance-and-benchmark.md) | 100,000 jobs in 10 minutes with p99 < 2 s; complexity; where it stops scaling; cost |
| Security and tenant isolation | [security-and-tenant-isolation.md](security-and-tenant-isolation.md) | Authentication, employee / admin / operator roles, tenant isolation, no PII |
| Monitoring and logging | [monitoring-and-logging.md](monitoring-and-logging.md) | Fire-lag histogram; running, retrying and dead-lettered counts; on-call counters; structured logs |
| Caching | [caching.md](caching.md) | Hot data, eviction, exactly what can be stale and for how long |
| Error handling | [error-handling.md](error-handling.md) | Meaningful, non-leaking errors; no personal data in errors or logs |
| Code structure and OOP | [code-structure.md](code-structure.md) | Controller / service / repository layering; encapsulation, inheritance, polymorphism; core logic without a server or DB |
| Testing | [testing.md](testing.md) | Every hard rule automated, including concurrency and failure tests |
| Trade-offs | [trade-offs.md](trade-offs.md) | Consistency vs availability vs latency; what was left out |

## Other documents (not design)

| Document | Purpose |
|---|---|
| [`../DEMO.md`](../DEMO.md) | Reviewer guide: test every claim yourself, step by step |
| [`../RUNBOOK.md`](../RUNBOOK.md) | On-call runbook: one section per alert, linked from every alert rule |
| [`../sink-api.md`](../sink-api.md) | HTTP API of the notification sink |
| [`../postman/`](../postman/) | Postman collection and environment |
| [`../../chaos/README.md`](../../chaos/README.md) | How to run the kill and freeze scripts |
| [`../../deploy/patroni/README.md`](../../deploy/patroni/README.md) | Production database failover setup |
