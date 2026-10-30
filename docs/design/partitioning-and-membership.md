# Partitioning and membership: no single hot lock

> Design docs index: [README.md](README.md) · Full design: [architecture.md §06](architecture.md#06-partitioning-and-membership-no-single-hot-lock)

**Requirement:** partition the jobs so nodes do not all contend on one row. When a node joins or
leaves, its partitions move to the others.

## 1. Partitions

- Jobs are split into **128 fixed partitions**: `partition_id = murmur3(jobId) % 128`, stored on
  the job and copied to each run.
- Each partition has its own lease row in `partition_lease` (owner, lease until, epoch), so
  nodes contend per partition, never on one global lock row.
- A node polls, materialises and claims only in the partitions it owns, which spreads the load.

**Correctness does not depend on ownership.** If two nodes poll the same partition during a
handover, `FOR UPDATE SKIP LOCKED` and the run-level fencing token still allow only one claim per
run. Partition ownership is a load-spreading device, not a lock.

## 2. Membership and heartbeats

| Loop | Interval | What it does |
|---|---|---|
| Membership | 2 s | Upserts the node's row in `scheduler_node` (`heartbeat_at = clock_timestamp()`). A node is live if its heartbeat is under 6 s old by DB time |
| Partition loop | 1 s | Computes the partitions this node should own over the live nodes, then acquires, renews (6 s lease, renewed every 2 s) or releases them |

Each node also records `alive_since`, the start of its unbroken run of heartbeats; a gap of 6 s
or more restarts it. The misfire rule uses it to tell downtime from queueing
([misfires](misfires-retries-and-dead-letters.md#1-misfires)).

A node whose partition loop stalls stops heartbeating on purpose, so membership drops it and its
partitions move instead of staying owned by a node that cannot serve them.

## 3. Rendezvous hashing

Each partition goes to the live node with the highest `hash(partition, nodeId)`. When a node
joins or leaves, **only the partitions that hash to it move** (about `128 / N`); every other
partition keeps its owner. Assignment costs O(P · N) hashes per membership change (128 × 3 =
384).

Balance is statistical, not exact: with the compose node ids the split is 39 / 40 / 49, and
max/min is about 1.3 over random 3-node sets (mean 1.31, p90 1.58 over 10,000 sets).

## 4. When a node dies

| What | How long |
|---|---|
| Its **running** runs | Taken over by the orphan sweep of any node, in **any** partition, ≤ 10.7 s after the last renewal ([failure handling](failure-handling.md)) |
| **New** due runs in its partitions | Wait until its 6 s partition lease expires and a live node acquires it: up to about 7 s |

An expired partition owner is treated as dead for that partition at once, so a survivor does
not wait for the 6 s membership timeout as well. This is where the benchmark's only p99 misses
come from: runs that fall due in a dead or frozen node's partitions in the few seconds after the
event ([benchmark §3.3](performance-and-benchmark.md#33-scenario-3-spread--chaos-p99-291-ms-overall-0-duplicates-p99-in-the-affected-window-37-s-kill-and-53-s-freeze)).
A shorter partition lease would shrink that window at the cost of more lease writes.

## 5. Tests

| Test | What it proves |
|---|---|
| `PartitionAssignerTest` | Rendezvous assignment, minimal movement on join/leave, the 39/40/49 compose split |
| `PartitionPlannerTest` | Acquire / renew / release decisions |
| `ClusterStoresIT` | Partition leases and heartbeats against real Postgres; `alive_since` restarts only after a gap |
| `RebalanceIT` | Every partition owned again within 7 s of a worst-case kill |
| `ClaimRaceIT` | Two nodes polling the same partition never claim a run twice |

`GET /api/v1/cluster` (OPERATOR) shows the live nodes, partitions per node and every partition's
owner and lease epoch.
