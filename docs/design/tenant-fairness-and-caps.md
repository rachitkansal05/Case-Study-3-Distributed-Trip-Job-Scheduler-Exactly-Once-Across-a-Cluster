# Per-tenant fairness and concurrency caps

> Design docs index: [README.md](README.md) · Full design: [architecture.md §10](architecture.md#10-per-tenant-fairness-with-numbers)

**Requirement:** one client's 50,000 alerts at 8:00 AM must not delay another client's 20
alerts. Enforce a per-tenant concurrency cap, and show the numbers.

## 1. Design

- **Slots.** Each tenant has a cluster-wide cap (default 32), enforced by exactly `cap` rows in
  `tenant_slot`. A run can only be `RUNNING` while it holds one of its tenant's slots, and
  `job_run.slot_no` is the only record of who holds a slot.
- **Claims.** The poller claims per tenant, one slot per run, and never locks more slots than it
  has due runs for. When a run finishes, its worker claims for that tenant at once (at most 4
  such claims at a time per node), so a freed slot is refilled within one round trip instead of
  waiting for the 200 ms poll.
- **Dispatch.** Each node has 64 virtual-thread workers, taking from per-tenant queues in turn
  with **deficit round robin**, so a tenant with a huge backlog gets its share and no more.
  Recovered runs (taken over from a dead node) go first, with 1 worker reserved for them.

## 2. Numbers

Scenario: 3 nodes × 64 workers, both tenants' alerts due at the same instant, each alert a
20–60 ms sink call ([benchmark §3.2](performance-and-benchmark.md#32-scenario-2-burst--fairness-b-unaffected-max-185-ms-a-drained-in-716-s)).

| | Without caps (one FIFO queue), *estimate* | With caps (32 per tenant), **measured** |
|---|---|---|
| Tenant A, 50,000 alerts | drains in ≈ 10.4 s | **698 runs/s, drained in 71.6 s** |
| Tenant B, 20 alerts | waits up to 10.4 s ❌ | **max lag 185 ms** ✅ |

Each A run holds its slot for about 46 ms: the sink call plus the checkpoint, the fenced commit
and the claim that refills the slot, so 32 / 0.046 s ≈ 700 runs/s. `DrainRateIT` measures a
single tenant at 614–645 runs/s on the development laptop. Before slot-freed claims, a capped
tenant drained at about 144 runs/s.

A hard cap leaves capacity idle during a single-tenant burst: we chose isolation over peak
throughput. Caps are configurable per tenant.

## 3. Why the cap always holds

The database enforces it, with no tolerance during partition moves or crashes:

- A running run must hold a slot: `CHECK (state <> 'RUNNING' OR (slot_no IS NOT NULL AND fencing_seq IS NOT NULL))`.
- The slot must be a real slot row of the same tenant: composite foreign key
  `(tenant_id, slot_no) → tenant_slot`, `ON DELETE RESTRICT`.
- No slot is used twice: `UNIQUE (tenant_id, slot_no) WHERE state = 'RUNNING'`.
- A slot passes to a new run only after the old run is reaped by a conditional update, at the
  same moment the old holder loses the right to write.

So at most `cap` runs per tenant hold a valid lease at any moment. A frozen process may run for
a moment when it wakes; fencing rejects anything it does.

## 4. Changing a cap

Each tenant has a `configured_cap` (what the admin set) and an **effective cap**, its number of
slot rows. "Running ≤ effective cap" has no exceptions.

- **Raising** adds slot rows at once; they are usable on the next claim.
- **Lowering** is deferred. Example: 28 running, cap lowered from 32 to 16:
  1. Free slots numbered 16 or higher are deleted at once (the database refuses to delete a slot
     in use).
  2. Whenever a slot below 16 becomes free, the slot compactor moves one running run from a high
     slot into it, then deletes the empty high slot.
  3. Claims use only slots below the configured cap, so **no new run starts until fewer than 16
     are running**.
- Running jobs are never stopped; the effective cap reaches the new value within the longest
  remaining `maxRunTime` of the runs already active.
- `PUT /api/v1/tenants/{t}/cap` returns `configuredCap`, `effectiveCap` and `draining`; the
  `scheduler_tenant_cap{kind}` gauges show both numbers.

Caps and pause are never cached: the cap is the slot rows themselves, and the claim reads
`tenant.paused` directly.

## 5. Tests

| Test | What it proves |
|---|---|
| `TenantFairnessIT` | 50,000 + 20 alerts at the same instant on 3 nodes: B's p99 lag < 1 s |
| `DrainRateIT` | One tenant at cap 32 drains ≥ 600 runs/s (≥ 450 on shared CI runners); the cap holds throughout |
| `TenantCapIT` | Running ≤ cap, sampled every 10 ms through random kills, freezes and partition moves, no tolerance |
| `SchemaIT` | The database rejects every cap violation by itself |
| `CapReductionIT`, `CapIncreaseIT`, `CapApiIT` | Deferred reduction with compaction, raises, the API response |
| `LockOrderIT` | Claims (slot then run) and commits (no slot lock) never deadlock |
| `DeficitRoundRobinDispatcherTest`, `CapChangePlannerTest` | Deficit round robin shares; cap-change planning |
