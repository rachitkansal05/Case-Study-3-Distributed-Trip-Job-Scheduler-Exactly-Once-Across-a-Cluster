# Misfires, retries and the dead-letter list

> Design docs index: [README.md](README.md) · Full design: [architecture.md §09](architecture.md#09-misfires-retries-and-the-dead-letter-list)

**Requirement:** after downtime, each job follows its policy: fire now, skip, or coalesce many
missed runs into one. Failed runs retry with exponential backoff and jitter, then move to a
dead-letter list that an operator can inspect and replay.

## 1. Misfires

A run has **misfired** when it was already more than `misfireThreshold` (default 60 s, at most
7 days) past its scheduled time **at the moment its tenant last became claimable**: after
cluster downtime, a database outage or a tenant pause.

```
claimable_since = greatest(tenant.resumed_at, min(alive_since) of the live nodes)   -- DB now if no node is live
misfired        ⇔  claimable_since − scheduled_at > misfire_threshold
```

- `tenant.resumed_at` is stamped when a tenant is unpaused.
- `scheduler_node.alive_since` is the start of a node's unbroken heartbeats; a heartbeat gap of
  at least `membership.dead-after` (6 s), from a node outage or a DB outage, restarts it.
- **Queueing is not a misfire.** A run that only waited behind its own tenant's cap is late but
  was never missed, so a large `SKIP` burst runs in full instead of losing its tail
  (`BurstSkipIT`). Occurrences that fell due after `claimable_since` always fire.
- The decision is made at claim time, inside the claim transaction, by `MisfireResolver`. The
  extra query runs only for a candidate that is already late by DB time, so on-time claims cost
  nothing.

| Policy | After 3 missed hourly runs | Typical use |
|---|---|---|
| `FIRE_NOW` (default) | All 3 run now, in order, each with its own key | Billing close: every period must close |
| `SKIP` | All 3 become `SKIPPED`; the next run is normal | Pickup alerts: a late alert is useless |
| `COALESCE` | The newest runs once with `coalescedCount = 3`; the others are `SKIPPED` | Roster generation: only the latest matters |

`MisfirePolicy` is a sealed interface with three stateless implementations (`FireNow`, `Skip`,
`Coalesce`), chosen per job by `MisfirePolicy.of(type)`.

**Replays and manual triggers ignore the misfire policy.** Both are explicit requests to run, so
the run is flagged `misfire_exempt`: it runs however late it is, is never skipped and never
coalesced with the job's scheduled occurrences. A replayed run also does not report fire lag
again; only a run's first start does.

## 2. Retries

`delay = random(0, min(5 min, 2 s × 2^attempt))`: exponential backoff with **full jitter**, so a
burst of failures does not retry in lockstep.

| Failure | Error code | Behaviour |
|---|---|---|
| Sink 5xx | `SINK_5XX` | retried |
| Sink timeout (5 s request timeout) | `SINK_TIMEOUT` | retried |
| Max run time reached | `TIMEOUT` | retried |
| Other sink 4xx | `SINK_REJECTED_<status>` | dead-lettered at once |
| Payload the handler cannot read | `INVALID_PAYLOAD` | dead-lettered at once |
| Lease lost (`409 STALE_TOKEN`, fenced write matched 0 rows) | — | not a failure: the attempt is given back and the run continues elsewhere; after 20 lease losses it is dead-lettered as `FENCING_CONFLICT` |

A retry waits as `RETRY_WAIT` with `due_at = clock_timestamp() + delay`, in the same due index as
new runs, so a retrying run competes fairly for its tenant's slots.

## 3. The dead-letter list

After `maxAttempts` (default 5, up to 100) the run becomes `DEAD_LETTERED` and a `dead_letter`
row records the error code and attempts, in the same transaction as the fenced commit.

| Operation | Endpoint | Roles |
|---|---|---|
| Inspect | `GET /api/v1/dead-letters?tenant=&page=&size=` | OPERATOR (all tenants or one), ADMIN (own tenant) |
| Replay | `POST /api/v1/dead-letters/{runId}/replay` | OPERATOR |

- The list shows error codes and counts only, never payload data.
- A replay sends the **same run row and idempotency key** back to `SCHEDULED`, with `attempt` and
  `lease_lost_count` reset and the checkpoint kept, so items delivered before the failure are
  not sent again.
- Replaying twice returns `409 DEAD_LETTER_ALREADY_REPLAYED`.
- Dead-lettered runs are never purged; finished runs are purged after 7 days.

## 4. Metrics and alerts

`scheduler_runs{state="retrying"}` and `{state="dead_lettered"}` are the retrying and
dead-lettered counts; `scheduler_dead_letters_total{error_code}` counts new dead letters. Alerts:
`SchedulerDeadLettersIncreasing` (any new dead letter) and `SchedulerFencingConflictDeadLetters`
(any `FENCING_CONFLICT`). See [monitoring](monitoring-and-logging.md).

## 5. Tests

| Test | What it proves |
|---|---|
| `MisfirePolicyTest` | The three policies on 3 missed hourly runs; the strict `>` threshold |
| `MisfireIT` | The same through the real claim; queued-while-up is not a misfire; due before an outage or a pause ended is; COALESCE after an outage; stale heartbeats only |
| `BurstSkipIT` | 400 `SKIP` runs, cap 4, 1 s threshold: 375 start over 1 s late, none skipped |
| `ReplayMisfireIT` | Replays and manual triggers run once however late; siblings still follow the policy |
| `BackoffPolicyTest`, `RetryDeciderTest` | Full-jitter bounds, retryable vs permanent codes, lease-lost limit |
| `RetryToDeadLetterIT` | Sink failures retry, then dead-letter; replay delivers once; a second replay is 409 |
| `FaultInjectionIT` | The sink's injected 5xx and latency drive the retry path |
