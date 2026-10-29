# Benchmark results

Measured numbers for PLAN §17 (R13). Every figure here comes from a run on the machine below; the
raw reports (Markdown plus a per-run lag CSV) are written to `loadgen/target/bench/` by each run.
Nothing is estimated or extrapolated unless it says so.

## 1. Machine and software

Collected with PowerShell / CIM (`Win32_Processor`, `Win32_PhysicalMemory`, `Get-PhysicalDisk`,
`Win32_OperatingSystem`) and from the benchmark's own environment table.

| item | value |
|---|---|
| CPU | Intel Core i7-10750H @ 2.60 GHz, 6 cores / 12 threads (laptop) |
| RAM | 16 GiB DDR4-2933, 1 module (15.8 GiB visible) |
| Disk | Intel SSDPEKNW010T8, 1 TB NVMe SSD (Intel 660p, QLC) |
| OS | Windows 11 Home Single Language 10.0.26200 |
| JDK | Java HotSpot 64-Bit Server VM 22.0.2+9-70 (code compiled with `--release 21`) |
| Benchmark JVM flags | `-Xmx4g -Xms1g -XX:+UseG1GC` (the 3 scheduler nodes share this JVM) |
| Sink JVM | separate process, `-Xmx768m -XX:+UseG1GC`, Hikari pool 40 |
| Docker | none (everything in-process or as a local process) |

**PostgreSQL** (zonky embedded binaries, one server shared by the scheduler and sink databases,
like the compose setup), as read back with `SHOW`:

| setting | value |
|---|---|
| server_version | 16.15 |
| fsync / synchronous_commit / full_page_writes | on / on / on (forced; zonky turns them off by default) |
| shared_buffers | 512MB |
| wal_level | replica |
| max_connections | 400 |
| work_mem | 4MB |
| checkpoint_timeout / max_wal_size | 15min / 4GB |

**Topology**: 3 `SchedulerNode`s with the production `EngineSettings` (64 workers with 1
reserved for recovery, poll 200 ms, run lease 10 s renewed every 3 s, orphan sweep 500 ms,
partition lease 6 s, 128 partitions), each with its own Hikari pool and its own `HttpEffectSink`,
calling the real `notification-sink` over HTTP on localhost. **Simulated handler I/O**: the sink's
fault injection adds `20 ms + U[0, 40] ms` to every delivery (`PUT /admin/faults`
`{"latencyMs":20,"latencyJitterMs":40}`), so each run is one 20–60 ms sink call, one checkpoint
and one fenced commit. Jobs are one-off `PICKUP_ALERT` jobs with one employee each.

**Lag** is `started_at − scheduled_at` of each run's first start, both written by the database
(`clock_timestamp()`), read back after the run and recorded in an HdrHistogram (3 significant
digits). **Duplicates** are read from the sink itself: `delivered − deliveredDistinct` and
`maxDeliveriesPerKey` from `GET /api/v1/deliveries/stats`.

## 2. How to run it

The benchmark is a test-scope class (`loadgen/src/test/java/.../bench/InProcessBench.java`).
Its name does not match `*IT`, so `./mvnw verify` never runs it.

```bash
# one scenario: spread | burst | chaos | ramp | all (or a comma list)
./mvnw -B -pl loadgen -am verify -Pbench -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false \
  -Dit.test=InProcessBench -Dfailsafe.failIfNoSpecifiedTests=false -Dbench.scenario=spread

# knobs (defaults): bench.jobs=100000 bench.tenants=50 bench.cap=32 bench.windowSec=600
#   bench.leadSec=20 bench.burstA=50000 bench.burstB=20 bench.freezeSec=30 bench.stepSec=60
#   bench.rates=200,400,800,1200,1600,2400,3200 bench.maxP99Ms=2000 bench.sinkPool=40
# smoke run (about 5 minutes for all four):
#   ... -Dbench.scenario=all -Dbench.jobs=3000 -Dbench.windowSec=30 -Dbench.stepSec=15 -Dbench.rates=100,300
```

`-Pbench` only sizes the forked JVM. The run fails if any duplicate is found,
`maxDeliveriesPerKey` is not 1, or runs are left unfinished.

**Against a running compose stack** (when Docker is available; not run here):

```bash
./mvnw -B -pl loadgen -am package -DskipTests
java -jar loadgen/target/loadgen-0.1.0-SNAPSHOT.jar --scenario spread --jobs 100000 --tenants 50 \
  --window 600 --lead 120 --jdbc 'jdbc:postgresql://localhost:5432/scheduler?user=scheduler&password=scheduler'
```

It creates tenants and jobs through the API (`POST /api/v1/tenants`, `POST /api/v1/jobs`, with
`Idempotency-Key`), using tokens from `POST /dev/token` or, because compose does not enable the
`dev` profile, tokens it signs itself with `--hmac-secret` / `SCHEDULER_JWT_HMAC_SECRET`. It then
reads the lag from `job_run` over JDBC and prints the same tables. In this mode `runAt` comes
from the host clock, so host-to-DB clock skew shows up in the lag. `--help` lists every option.

## 3. Results

All runs were on 2026-10-30 (IST), one scenario per Maven invocation, on an otherwise idle
machine. Raw reports, named by UTC start time, are in `loadgen/target/bench/`:
`spread-20261029-220937`, `burst-20261029-222034`, `chaos-20261029-224327` (reported),
`chaos-20261029-222247` (blind timing, §3.3), `chaos-20261029-224000` (2-minute diagnostic),
`ramp-20261029-223352` (A), `ramp-20261029-225636` (B) and `ramp-20261029-225429` (C).

### 3.1 Scenario 1, spread (target p99 < 2 s): **met, p99 = 237 ms**

100,000 one-off jobs over 50 tenants (cap 32 each), with `scheduled_at` uniform over 600 s
(166.7 runs/s on average). All rows were inserted 14 s before the first due time.

| runs | succeeded | dead-lettered | retried (attempt > 1) | lease lost | throughput |
|---|---|---|---|---|---|
| 100,000 | 100,000 | 0 | 0 | 0 | 166.6 runs/s (the arrival rate; first start to last finish 600.3 s) |

| fire lag (ms) | p50 | p95 | p99 | p99.9 | max |
|---|---|---|---|---|---|
| all 100,000 runs | 107.9 | 210.0 | **236.8** | 343.6 | 848.9 |

| minute | arrivals | starts | completions | p50 ms | p99 ms | max ms |
|---|---|---|---|---|---|---|
| 0 | 10,044 | 10,036 | 10,018 | 133.2 | 283.9 | 431.9 |
| 1 | 9,963 | 9,960 | 9,966 | 108.1 | 223.0 | 252.7 |
| 2 | 10,082 | 10,075 | 10,084 | 104.7 | 222.6 | 285.4 |
| 3 | 9,949 | 9,950 | 9,947 | 105.5 | 221.2 | 244.4 |
| 4 | 10,006 | 10,009 | 9,994 | 106.4 | 219.4 | 235.4 |
| 5 | 10,052 | 10,044 | 10,065 | 105.6 | 221.2 | 312.6 |
| 6 | 10,010 | 10,008 | 10,008 | 103.0 | 217.9 | 373.5 |
| 7 | 10,133 | 10,141 | 10,137 | 104.9 | 310.3 | 848.9 |
| 8 | 9,935 | 9,947 | 9,923 | 103.1 | 215.7 | 230.3 |
| 9 | 9,826 | 9,802 | 9,830 | 104.4 | 217.2 | 372.7 |

| sink | value |
|---|---|
| deliveredDistinct / delivered / alreadyDelivered / staleRejected | 100,000 / 100,000 / 0 / 0 |
| **duplicates** (delivered − deliveredDistinct) / maxDeliveriesPerKey | **0** / 1 |

Machine CPU while runs were due averaged 15 % and peaked at 52 %. The p50 of about 105 ms is the
200 ms poll (100 ms average wait) plus the claim, as the plan's lag budget predicts (§17).

### 3.2 Scenario 2, burst + fairness: **B unaffected (max 185 ms); A drained in 71.6 s**

Tenant A has 50,000 runs (500 jobs × 100) and tenant B has 20, all due at the same instant T,
with cap 32 each.

| tenant | runs | p50 ms | p95 ms | p99 ms | max ms | drain time (max finished_at − T) | drain rate |
|---|---|---|---|---|---|---|---|
| A | 50,000 | 36,241 | 68,026 | 70,844 | 71,565 | **71.6 s** | 698 runs/s |
| B | 20 | 142.3 | 183.8 | 184.6 | **184.6** | 0.3 s | n/a |

B's 20 lags (ms): 39.0, 39.4, 39.7, 129.9, 133.3, 135.1, 136.9, 138.6, 140.5, 142.2, 144.1, 146.2,
148.5, 178.9, 180.2, 181.1, 182.0, 183.0, 183.8, 184.5. Duplicates 0, maxDeliveriesPerKey 1,
dead letters 0.

PLAN §10 predicts 62.5 s for A (32 slots / 40 ms = 800 runs/s). The measured rate is 698 runs/s,
or 71.6 s. Each A run holds its slot for longer than the sink call (20–60 ms, mean 40 ms): it
also holds it through the checkpoint, the fenced commit and the claim round trip that refills
the slot. That comes to about 46 ms, and 32 / 0.046 s ≈ 700 runs/s. Before the slot-refill
change on this branch, claims happened only on the 200 ms poll, and the same tenant drained at
about 144 runs/s. `DrainRateIT` (3 nodes, cap 32, 40 ms handler, 5,000 runs) measured 614–645
runs/s over six runs on this machine, and it asserts ≥ 600.

### 3.3 Scenario 3, spread + chaos: **p99 291 ms overall, 0 duplicates; p99 in the affected window 3.7 s (kill) and 5.3 s (freeze)**

This repeats scenario 1, with two events:

- At 240 s, node `bench-1` is killed, like `kill -9`: its threads stop and its connection pool is
  closed.
- At 420 s, node `bench-2` is frozen for 30 s, a stop-the-world pause: every loop, renewal,
  effect and commit is blocked.

Before each event, the harness waits up to 2 s until the node holds a RUNNING run, so the event
lands mid-run.

| event | node | at (s after T0) | partitions owned | RUNNING runs held just after |
|---|---|---|---|---|
| kill | bench-1 | 240.0 | 43 | 8 |
| freeze | bench-2 | 420.0 | 69 | 1 |
| unfreeze | bench-2 | 450.0 | n/a | n/a |

| fire lag (ms) | runs | p50 | p95 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|
| **all runs** | 100,000 | 108.5 | 215.7 | **290.8** | 4,603.9 | 7,168.0 |
| killed node's partitions, all runs | 33,397 | 110.0 | 218.1 | 806.9 | 4,163.6 | 4,702.2 |
| killed node's partitions, scheduled in [kill, kill + 60 s] | 3,410 | 113.7 | 1,413.1 | **3,729.4** | 4,333.6 | 4,411.4 |
| all partitions, scheduled in [kill, kill + 60 s] | 10,071 | 110.2 | 219.1 | 2,504.7 | 4,227.1 | 4,411.4 |
| frozen node's partitions, all runs | 54,277 | 108.3 | 217.7 | 1,015.8 | 5,345.3 | 7,168.0 |
| frozen node's partitions, scheduled in [freeze, freeze + 60 s] | 5,345 | 114.9 | 2,748.4 | **5,349.4** | 6,414.3 | 7,168.0 |
| all partitions, scheduled in [freeze, freeze + 60 s] | 9,863 | 110.1 | 798.7 | 4,616.2 | 6,213.6 | 7,168.0 |
| affected partitions (union), all runs | 69,623 | 108.8 | 217.5 | 794.6 | 5,095.4 | 7,168.0 |
| unaffected partitions | 30,377 | 107.8 | 211.8 | 240.3 | 453.1 | 1,334.3 |

| exactly-once and takeover | value |
|---|---|
| succeeded / dead-lettered / not finished | 100,000 / 0 / 0 |
| orphan takeovers (surviving nodes' `scheduler_orphan_takeovers_total`) | 17 |
| runs with `lease_lost_count > 0` (reaped by the sweep or by a claim reusing the slot) | 26 |
| sink: deliveredDistinct / delivered | 100,000 / 100,000 |
| sink: alreadyDelivered (re-sent after a takeover and absorbed) | 5 |
| sink: staleRejected (409 to the thawed node's in-flight sends) | 17 |
| **duplicates** (delivered − deliveredDistinct) / maxDeliveriesPerKey | **0** / 1 |

**The 2 s target is missed for runs that fall due in the dead or frozen node's partitions in the
seconds after the event.** The cause is partition handover, not takeover delay:

- *Running* runs are taken over by the orphan sweep after their 10 s lease. Their lag was already
  recorded at their first start, so the takeover does not show up in these tables.
- *New* due runs in the victim's partitions wait until its 6 s partition lease expires and a live
  node acquires the partition. PLAN §12 says such runs "wait up to about 7 s".

The worst lag was 4.4 s after the kill and 7.2 s after the freeze. The freeze is worse because
the frozen node had renewed its partition leases just before freezing, and the freeze also
stalls any claims it had in progress. Runs in unaffected partitions kept a p99 of 240 ms, and
across the whole 10 minutes p99 stayed at 291 ms. A shorter partition lease would shrink this
window, at the cost of more lease traffic.

**An earlier chaos run with blindly timed events** (`chaos-20261029-222247`) is kept for the
record. Both of its events landed while the node held no RUNNING run. Runs are claimed in
batches on each 200 ms poll and last 20–60 ms, so at 167 runs/s a node is idle for most of each
poll interval. That run therefore tested only partition handover. Its results:

- p99 overall: 277 ms
- p99 in the window: 3.7 s for the killed node's partitions, 4.8 s for the frozen node's
- takeovers: 0; duplicates: 0

The 2-minute diagnostic run (`chaos-20261029-224000`) confirmed that the node held 0 RUNNING runs
at both events. The harness now waits for a busy node, and that is the run reported above.

Takeover time itself is measured by two tests:

- `NodeCrashIT` (real timings, 500 ms sweep, bound 11.2 s): 10.08 s, 9.99 s and 10.03 s from the
  kill, plus 10.12 s and 10.13 s in the two later full builds.
- `EndToEndKillIT` (real sink over HTTP): 7.97 s and 8.22 s from the kill, in two runs.

### 3.4 Where it stops scaling: **between 900 and 1,000 runs/s (about 2,700–3,000 state transitions/s) on this laptop**

Each ramp step schedules `rate × 60 s` fresh one-off runs, spread uniformly over 60 s across 50
tenants. A step starts after the previous one has drained, and the ramp stops after the first
step whose p99 is over 2 s. A **state transition** here is one durable write to a run's state:
the claim (SCHEDULED → RUNNING), the checkpoint and the commit (RUNNING → SUCCEEDED), so 3 per run.

Ramp A (default rates):

| target runs/s | achieved starts/s | transitions/s | p50 ms | p99 ms | max ms | CPU avg / max % |
|---|---|---|---|---|---|---|
| 200 | 199 | 598 | 97.9 | 211.5 | 277.8 | 17 / 35 |
| 400 | 399 | 1,195 | 94.0 | 221.7 | 318.0 | 20 / 32 |
| 800 | 798 | 2,393 | 82.8 | 235.9 | 391.2 | 37 / 49 |
| 1,200 | 1,183 | 3,545 | 203.0 | **10,657.8** | 16,146.4 | 64 / 100 |

Ramp B (`-Dbench.rates=400,800,900,1000,1100`, to narrow down the knee):

| target runs/s | achieved starts/s | transitions/s | p50 ms | p99 ms | max ms | CPU avg / max % |
|---|---|---|---|---|---|---|
| 400 | 399 | 1,196 | 93.0 | 244.9 | 685.6 | 25 / 100 |
| 800 | 798 | 2,391 | 82.9 | 238.0 | 396.5 | 37 / 58 |
| 900 | 897 | 2,690 | 82.6 | **246.3** | 428.8 | 43 / 57 |
| 1,000 | 998 | 2,992 | 121.8 | **9,060.4** | 14,819.3 | 52 / 91 |

Ramp C (`-Dbench.rates=900,1000,1100,1200`) ran 900 runs/s as its first step, on a cold JVM and
database. Its p99 was **4,640.8 ms** (max 7,938 ms): 91 % of runs started within 500 ms, then a
long tail. So 900 runs/s is right at the edge. It passed after a warm-up and failed cold.

**Stated limit: p99 crosses 2 s between 900 and 1,000 runs/s, which is about 2,700–3,000 run
state transitions/s, with 3 nodes, the sink and PostgreSQL all on this one 6-core laptop.** No
step lost or duplicated anything: every step had 0 duplicates, maxDeliveriesPerKey 1 and no
unfinished runs.

What these numbers do and do not show:

- Every step ran at its target rate. Above the knee, runs are not failing; they are queueing,
  and lag grows to 9–16 s within the step.
- PLAN §17 guessed that PostgreSQL alone would handle 5–10k state transitions/s on commodity
  disks. The measured knee is lower, but it is not a database-only limit: the scheduler JVM, the
  sink JVM and PostgreSQL share 6 cores, and CPU reached 91–100 % in the failing steps.
- These runs did **not** isolate the cause. The likely one is the poller. Each node claims per
  tenant, one claim transaction after another, on a single thread. With 50 tenants that is up to
  50 claim transactions per node per 200 ms tick, and each one gets a few milliseconds slower as
  the database gets busier. Once a tick takes longer than 200 ms, lag grows without bound.
  Confirming this needs a profile, or a run with PostgreSQL on separate hardware.
- Scaling past this point should start with that measurement. If the poller is the limit, the
  next step is to claim several tenants per transaction, or to run claims in parallel on each
  node. If PostgreSQL is the limit, the next step is PLAN §17's: shard by partition range.

## 4. Notes and caveats

- **Single machine.** All three nodes share one JVM (each with its own pools, registry and
  threads), and the sink runs in a second JVM. These are laptop numbers. The compose mode in §2
  is the way to repeat them across containers.
- **Durability on.** `fsync`, `synchronous_commit` and `full_page_writes` were on, checked with
  `SHOW` (zonky's defaults turn them off). There is no replication, so there is no synchronous
  standby round trip; PLAN §17 estimates that adds about 1 ms per commit.
- **Lag is measured at the first start.** A run taken over later keeps its original
  `started_at`, so takeover delay does not appear in fire lag. It shows up in the takeover
  counters and is measured directly by `NodeCrashIT` and `EndToEndKillIT`.
- **Minute 7 of the spread run** had a p99 of 310 ms and a max of 849 ms with no chaos event. It
  was one short stall on the shared machine.
- **Not run here:** compose mode (`LoadGenMain`) and the chaos scripts against Docker. There is
  no Docker on this machine.
