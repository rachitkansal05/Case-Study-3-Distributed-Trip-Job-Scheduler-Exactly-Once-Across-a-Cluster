# Failure handling: crashes, freezes, failover and chaos tests

> Design docs index: [README.md](README.md) · Full design: [architecture.md §11](architecture.md#11-hardest-flow-freeze-takeover-stale-write-rejected), [§12](architecture.md#12-failure-mode-table)

**Requirement:** fault-tolerant handling of crashes, timeouts, duplicate and out-of-order
messages and partial failures, with at least one failure-injection test. The two use cases:

- A node is killed with `kill -9` while sending 1,200 pickup alerts. Another node takes over the
  remaining alerts within the lease time, and every employee receives exactly one alert.
- A node is frozen for 30 seconds (SIGSTOP), its lease expires and another node finishes the job.
  When the frozen node resumes, its stale write is rejected by the fencing token.

## 1. Kill: takeover within the lease time

1. The handler sends alerts in chunks of 50 and saves `{"nextItem": n}` after each chunk with a
   fenced update; the lease is renewed every 3 s.
2. After `kill -9`, nobody renews. The run lease expires **10 s after the last renewal**, on the
   database clock.
3. Within 500 ms the **orphan sweep** of a surviving node (any node, any partition) locks the
   run's slot, reaps the run and restarts it in the same slot with a higher token, in one
   transaction. A poller that reaps the holder while claiming for the tenant restarts it the
   same way, so a backlog never delays the takeover.
4. The new owner resumes from the checkpoint. The tail of the interrupted chunk is sent again
   and the sink answers `200 ALREADY_DELIVERED` for each repeat.

**Bound: ≤ 10.7 s** from the last renewal (10 s lease + 0.5 s sweep + about 0.2 s claim), given
one live node with a free worker (one worker per node is reserved for recovered runs).

## 2. Freeze: the stale write is rejected

The sequence diagram is in the [README](../../README.md#hardest-flow-node-frozen-mid-run-taken-over-stale-write-rejected).
Three independent layers stop the woken node:

| Layer | What happens |
|---|---|
| Self-fence | Its monotonic deadline (claim + lease − 2 s) has passed, so it stops before the next send or write |
| Sink | A send already in flight carries the old token and gets `409 STALE_TOKEN` (the sink keeps the highest token per run) |
| Database | Its renew, checkpoint or commit is fenced by token and lease, so it matches 0 rows |

A session frozen inside a transaction is ended by Postgres after 5 s
(`idle_in_transaction_session_timeout`), so its row locks cannot block the takeover.

## 3. Failure-mode table

| What fails | Detection | What users see | How it recovers | Test |
|---|---|---|---|---|
| Node crash (`kill -9`) mid-run | Run lease (10 s) not renewed | Alerts pause ≤ 10.7 s, then continue; nobody gets two | Orphan sweep or claim restarts the run in the same slot with a higher token, from the checkpoint; partitions move within about 7 s | `NodeCrashIT`, `NodeCrashBacklogIT`, `EndToEndKillIT`, `RebalanceIT`, `chaos/kill-during-alerts.sh` |
| Node freeze / long GC pause | Same lease expiry; the node's own deadline on wake-up | Same as a crash | Self-fence; in-flight send gets 409; fenced writes match 0 rows; counted in `scheduler_lease_lost_total` and `scheduler_stale_commit_rejected_total` | `GcPauseIT`, `EndToEndFreezeIT`, `LeaseResurrectionIT`, `TenantCapIT`, `chaos/freeze-during-alerts.sh` |
| Renewal racing slot reuse | Both are conditional updates of one row | Nothing | Postgres serialises them; exactly one wins | `SlotStealRaceIT`, `LockOrderIT` |
| Partial failure (some alerts sent, then crash) | Lease expiry | The rest arrive after the takeover | Per-chunk checkpoints; resume from the checkpoint; repeats deduplicated | `NodeCrashIT`, `ExactlyOnceEffectIT` |
| Duplicate messages (re-sent chunk, client retry, double-clicked trigger) | Unique keys | One effect | Idempotency keys at the sink and at the API | `ExactlyOnceEffectIT`, `ApiIdempotencyIT`, `ConcurrentDeliveryIT` |
| Out-of-order or late writes | Token and state predicates | Nothing | Every write is conditional on token and state; the sink rejects lower tokens | `FencingIT`, `StaleTokenClassificationTest` |
| Sink outage, slow sink, timeouts | 5xx, 5 s request timeout | Alerts delayed, retries rise | Full-jitter backoff, then dead letter; operator replays with the same key; caps keep the damage to one tenant | `RetryToDeadLetterIT`, `FaultInjectionIT`, `HttpEffectSinkTest` |
| Network partition (node cut off from the DB) | Renewals fail | Same as a crash | The cut-off node cannot renew and self-fences; its writes fail when it returns | `FencingIT`, `LeaseResurrectionIT` |
| PostgreSQL unreachable | Claims and renewals throw | No jobs fire; API returns `503 SCHEDULER_STORE_UNAVAILABLE` | Consistency over availability; nodes self-fence; on return, misfire policies handle the backlog | `ErrorResponseTest`, `MisfireIT` |
| PostgreSQL primary fails (production) | Patroni leader lease | Writes pause about 10–30 s; nothing acknowledged is lost | Only the synchronous standby is promoted; the new timeline raises the token epoch | `FailoverFencingIT`, `StrictSyncIT` (Docker-gated) |
| Cluster rebuilt from a fresh `initdb` | Epoch below the sink's highest | Nodes refuse to start | Operator raises `cluster_generation` | `EpochGuardIT`, `ClusterGenerationGuardIT` |
| Clock skew on a node | Not needed | Nothing | Due times and leases use the DB clock; self-fencing uses the monotonic clock | `FencingIT`, `ClaimRaceIT` |
| Handler past `maxRunTime` or hung | Timer per run | Attempt fails `TIMEOUT` | Interrupted; abandoned 3 s later if it ignores that; slot freed | `MaxRunTimeIT` |
| Bad payload | Handler cannot read it | Run in the dead-letter list as `INVALID_PAYLOAD` | Dead-lettered at once | `RetryToDeadLetterIT` |

## 4. Failure injection

- **Toxiproxy** in compose: each node reaches Postgres and the sink through its own proxy, so one
  node can be cut off, slowed or partitioned on its own.
- **Sink fault API** (`PUT /admin/faults`, needs `X-Admin-Token`): latency with jitter and random
  500/503 failures before any write (`FaultInjectionIT`, `RetryToDeadLetterIT`).
- **In-process kills and freezes** in the test harness (`TestCluster.kill`, freeze gates), used by
  `NodeCrashIT`, `GcPauseIT`, `TenantCapIT` and the benchmark's chaos scenario.
- **Chaos scripts** against the Docker stack (below).

## 5. Chaos scripts on Docker

| Script | What it does | Result (2026-10-30) |
|---|---|---|
| `chaos/kill-during-alerts.sh` | Starts a 1,200-alert run, `docker kill`s the owner mid-send, waits for the takeover, checks the sink | **PASS:** taken over 7.98 s after the kill with a higher token; 1,200 distinct deliveries; max 1 per key |
| `chaos/freeze-during-alerts.sh` | Same run, `docker pause`s the owner for 30 s, unpauses, checks the fencing counters and the sink | **PASS** (twice): the run finished on another node; the frozen node's fenced write matched 0 rows (`fenced_write_0_rows` 0 → 1); 1,200 delivered once |

Usage, environment variables and troubleshooting: [`chaos/README.md`](../../chaos/README.md).

## 6. Database failover (production)

The compose stack runs a single Postgres. Production relies on Patroni with etcd ([`deploy/patroni/README.md`](../../deploy/patroni/README.md) maps
each requirement to a setting):

| # | Requirement | How it is met |
|---|---|---|
| F1 | Every acknowledged commit is on at least one standby | `synchronous_commit = on` |
| F2 | The primary never falls back to asynchronous replication | `synchronous_mode: true` + `synchronous_mode_strict: true`; with no synchronous standby, writes block |
| F3 | Only a replica holding every acknowledged commit can be promoted | 1 primary + 2 standbys with `FIRST 1`; Patroni promotes only the standby it recorded as synchronous |
| F4 | The old primary cannot accept writes after a promotion | Patroni leader lease + watchdog; JDBC `targetServerType=primary` |
| F5 | Fencing epochs strictly increase | New timeline on every promotion + `cluster_generation` on rebuilds + startup check against the sink |
