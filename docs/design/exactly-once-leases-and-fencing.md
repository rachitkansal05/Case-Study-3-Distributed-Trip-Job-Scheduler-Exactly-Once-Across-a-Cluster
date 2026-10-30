# Exactly-once effect: leases and fencing tokens

> Design docs index: [README.md](README.md) · Full design: [architecture.md §02](architecture.md#02-what-we-guarantee-and-why-exactly-once-execution-is-impossible), [§07](architecture.md#07-claim-fence-execute-commit), [§11](architecture.md#11-hardest-flow-freeze-takeover-stale-write-rejected)

**Requirement:** execution is at least once, and every run carries an idempotency key so the
side effect happens exactly once. A node owns a job through a time-limited lease and a fencing
token; a node that wakes from a long pause after its lease expired must be unable to commit.
Explain why exactly-once execution is impossible and which clock decides when a job is due.

## 1. Why exactly-once execution is impossible

A node can send an alert and then die before it records the send. To the rest of the cluster
that looks exactly like a node that died *before* sending. A node in a 30-second GC pause looks
the same as a dead one. So a scheduler can only retry (and maybe repeat the work) or never retry
(and maybe lose it). Losing jobs is not allowed, so this scheduler retries, and makes the
**effect** exactly-once instead.

## 2. What is guaranteed instead

| Guarantee | Mechanism |
|---|---|
| Every scheduled occurrence is executed **at least once** | Runs are rows; a lost lease puts the run back in line; retries until success or dead letter |
| Its side effect happens **exactly once** | Deterministic keys: `{jobId}:{scheduledAtUtc}` per run (`{jobId}:manual:{Idempotency-Key}` for a manual run) and `{runKey}:{employeeId}` per item. The receiver stores each key under a unique constraint and answers repeats with `200 ALREADY_DELIVERED` |
| A node whose lease expired **can write nothing** | Every renew, checkpoint and commit is fenced by `(epoch, seq)` and by `lease_until >= clock_timestamp()`; the sink rejects lower tokens with `409 STALE_TOKEN` |
| Tokens never go backwards, even across a DB failover | `epoch = cluster_generation × 2^32 + timeline`; startup epoch guard against the sink |

This relies on downstream systems honouring idempotency keys (or a dedupe table we control in
front of them). No scheduler can make a non-idempotent third-party API exactly-once.

## 3. Which clock decides when a job is due

**The PostgreSQL primary's clock.** Due-time and lease checks are SQL predicates on
`clock_timestamp()` (the current time; `now()` is the transaction's start time and could call an
expired lease valid). Node clocks only affect log timestamps, so skew between nodes changes
nothing. Each node uses its **monotonic** clock for one thing only: stopping itself early when
it has not renewed a lease in time (self-fencing, deadline = claim + lease − 2 s). Skew does not
move a monotonic clock.

## 4. Claim, execute, checkpoint, commit

All engine writes to run state and slots go through one class, `JdbcClaimProtocol`.

1. **Claim**, in one transaction, slot row first and run row second (a fixed lock order, so no
   deadlocks):
   - lock a free-looking slot of the tenant (`FOR UPDATE SKIP LOCKED`);
   - if the slot's holder looks expired, reap it with a conditional update
     (`… WHERE state = 'RUNNING' AND lease_until < clock_timestamp()`); one row updated means the
     slot is free and the reaped run is restarted in it at once, zero rows means the holder
     renewed in time. The reap and a renewal update the same row, so exactly one wins;
   - start the most urgent due run: `RUNNING`, a slot, token `(epoch, nextval('fence_seq'))`,
     `lease_until = clock_timestamp() + 10 s`.
   - The token is used only after this transaction commits.
2. **Execute.** Every outbound call carries `Idempotency-Key` and `Fencing-Token: <epoch>.<seq>`.
3. **Renew** every 3 s and **checkpoint** after each chunk of 50 with the fenced predicate:

   ```sql
   WHERE id = :run AND (fencing_epoch, fencing_seq) = (:e, :s)
     AND state = 'RUNNING' AND lease_until >= clock_timestamp()
   ```

   So **an expired lease can never be renewed**, even if nobody has taken the run over yet.
4. **Commit** with the same predicate, setting `SUCCEEDED` and freeing the slot in one
   statement. Zero rows means the lease was lost: the commit is rejected and counted
   (`scheduler_stale_commit_rejected_total`).

## 5. Fencing tokens

- A token is the pair `(fencing_epoch, fencing_seq)`, compared epoch first. No bit packing, so no
  range limit.
- `fencing_epoch = cluster_generation × 2^32 + timeline`. PostgreSQL's timeline rises on every
  promotion, so a new primary's tokens beat every token of the old one. If the timeline cannot be
  read, the claim fails and nothing starts.
- Operators raise `cluster_generation` when rebuilding from a fresh `initdb`. Nodes refuse to
  start if their epoch is below the sink's highest accepted epoch (`EpochGuard`).
- A `409 STALE_TOKEN` means the lease was lost: the handler stops and the attempt does not count
  toward `maxAttempts`. After 20 lease losses the run is dead-lettered as `FENCING_CONFLICT`.

## 6. Three layers stop a stale node

| Layer | When it acts |
|---|---|
| Self-fence (monotonic deadline) | On wake-up, before the next send or write |
| Sink highest-token check (`409`) | A send already in flight when the node froze |
| Fenced DB write (0 rows) | Any renew, checkpoint or commit with the old token |

The sequence diagram of this flow is in the [README](../../README.md#hardest-flow-node-frozen-mid-run-taken-over-stale-write-rejected).

## 7. Tests

| Test | What it proves |
|---|---|
| `ExactlyOnceEffectIT` | 3 nodes, 10,000 jobs, failures forced after the send: 10,000 distinct keys, none twice, more than 10,000 attempts |
| `FencingIT` | Stale holder fenced in the DB and the sink; claim reaps an expired holder and restarts it in the same slot |
| `ClaimRaceIT` | 32 threads claiming the same runs: each run claimed once |
| `SlotStealRaceIT` | Both interleavings of renew vs reap forced: exactly one wins |
| `LeaseResurrectionIT` | An expired lease can never be renewed, even with no takeover |
| `LockOrderIT` | Claims and commits never deadlock |
| `GcPauseIT`, `EndToEndFreezeIT` | A frozen node's in-flight send gets 409 and its commit is rejected |
| `EpochGuardIT`, `ClusterGenerationGuardIT`, `FailoverFencingIT`, `StrictSyncIT` | Tokens survive failover and rebuilds |
| `FencingTokenTest`, `StaleTokenClassificationTest` | Token order and header format; 409 classified as lease lost |
