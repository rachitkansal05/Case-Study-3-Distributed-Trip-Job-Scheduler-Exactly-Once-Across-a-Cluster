package com.moveinsync.scheduler.loadgen.bench;

import static com.moveinsync.scheduler.loadgen.MarkdownTable.f0;
import static com.moveinsync.scheduler.loadgen.MarkdownTable.f1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.moveinsync.scheduler.app.engine.EngineSettings;
import com.moveinsync.scheduler.app.engine.SchedulerNode;
import com.moveinsync.scheduler.core.cluster.PartitionHasher;
import com.moveinsync.scheduler.loadgen.LagReport;
import com.moveinsync.scheduler.loadgen.MarkdownTable;
import com.moveinsync.scheduler.loadgen.RunSample;
import com.moveinsync.scheduler.loadgen.RunTables;
import com.moveinsync.scheduler.loadgen.support.EmbeddedPg;
import com.moveinsync.scheduler.loadgen.support.SchedulerCluster;
import com.moveinsync.scheduler.loadgen.support.SinkProcess;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * In-process benchmark (plan section 17). Not an IT on purpose: it never runs in {@code verify};
 * run it explicitly (see {@code loadgen/pom.xml}, profile {@code bench}):
 *
 * <pre>
 * ./mvnw -B -pl loadgen -am verify -Pbench -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Dit.test=InProcessBench -Dfailsafe.failIfNoSpecifiedTests=false -Dbench.scenario=spread
 * </pre>
 *
 * <p>Every scenario gets a fresh setup: an embedded PostgreSQL 16 with fsync and synchronous_commit
 * on (the defaults) and a larger shared_buffers / max_wal_size, the real notification-sink jar in
 * its own JVM (fault injection: 20 ms + U[0, 40] ms per call, the simulated I/O), and three
 * {@link SchedulerNode}s with production settings in this JVM. Load is inserted straight into
 * {@code job} and {@code job_run} with a few bulk statements, after ownership of all 128
 * partitions is stable, with a lead so the insert commits before the first run is due. Results
 * are read back from {@code job_run} (DB clock) and printed as Markdown, and written to
 * {@code loadgen/target/bench/<scenario>-<timestamp>.md} with a raw lag CSV next to it.
 *
 * <p>System properties: {@code bench.scenario} = spread | burst | chaos | ramp | all (comma list
 * allowed); {@code bench.jobs} (100000), {@code bench.tenants} (50), {@code bench.cap} (32),
 * {@code bench.windowSec} (600), {@code bench.leadSec} (20, raised automatically for big inserts),
 * {@code bench.burstA} (min(50000, jobs / 2)), {@code bench.burstB} (20), {@code bench.freezeSec}
 * (30), {@code bench.stepSec} (60), {@code bench.rates} (200,400,800,1200,1600,2400,3200),
 * {@code bench.maxP99Ms} (2000), {@code bench.sinkPool} (40).
 */
class InProcessBench {

    private static final DateTimeFormatter STAMP = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
            .withZone(ZoneOffset.UTC);
    private static final int PARTITIONS = 128;
    private static final int LATENCY_MS = 20;
    private static final int JITTER_MS = 40;
    private static final int BATCH = 20_000;
    private static final String PAYLOAD = SchedulerCluster.alertPayload(1);

    private final int jobs = intProp("bench.jobs", 100_000);
    private final int tenants = intProp("bench.tenants", 50);
    private final int cap = intProp("bench.cap", 32);
    private final int windowSec = intProp("bench.windowSec", 600);
    private final int leadSec = intProp("bench.leadSec", 20);
    private final int burstA = intProp("bench.burstA", Math.min(50_000, Math.max(100, jobs / 2)));
    private final int burstB = intProp("bench.burstB", 20);
    private final int freezeSec = intProp("bench.freezeSec", 30);
    private final int stepSec = intProp("bench.stepSec", 60);
    private final int maxP99Ms = intProp("bench.maxP99Ms", 2000);
    private final int sinkPool = intProp("bench.sinkPool", 40);
    private final int[] rates = Arrays.stream(System.getProperty("bench.rates", "200,400,800,1200,1600,2400,3200")
            .split(",")).map(String::trim).filter(s -> !s.isEmpty()).mapToInt(Integer::parseInt).toArray();

    @Test
    void run() throws Exception {
        quietLogs();
        String sel = System.getProperty("bench.scenario", "spread").trim().toLowerCase(Locale.ROOT);
        List<String> scenarios = sel.equals("all") ? List.of("spread", "burst", "chaos", "ramp")
                : Arrays.stream(sel.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
        List<String> failures = new ArrayList<>();
        for (String s : scenarios) {
            switch (s) {
                case "spread" -> failures.addAll(spread());
                case "burst" -> failures.addAll(burst());
                case "chaos" -> failures.addAll(chaos());
                case "ramp" -> failures.addAll(ramp());
                default -> throw new IllegalArgumentException("unknown bench.scenario " + s);
            }
        }
        assertThat(failures).as("benchmark invariants").isEmpty();
    }

    // ================================================================== scenarios

    /** Scenario 1: N one-off alerts over T tenants, due uniformly over the window. */
    private List<String> spread() throws Exception {
        Report rep = new Report("spread");
        try (Env env = Env.start(sinkPool)) {
            rep.environment(env);
            List<String> ts = createTenants(env, tenants, "t");
            Load load = insertSpread(env, ts, jobs, windowSec);
            rep.setup(load, String.format(Locale.ROOT, "%d one-off PICKUP_ALERT jobs (1 employee each) over %d "
                    + "tenants (cap %d), one run each, scheduled_at uniform over %d s", jobs, tenants, cap, windowSec));
            CpuSampler cpu = awaitTerminal(env, load, windowSec + 300);
            List<RunSample> samples = load(env, load);
            spreadTables(rep, samples, load, cpu);
            List<String> failures = sinkSection(rep, env, samples);
            rep.write(samples);
            return failures;
        }
    }

    /** Scenario 2: tenant A floods, tenant B has a few, all due at the same instant T. */
    private List<String> burst() throws Exception {
        Report rep = new Report("burst");
        try (Env env = Env.start(sinkPool)) {
            rep.environment(env);
            env.cluster.tenant("burst_a", cap);
            env.cluster.tenant("burst_b", cap);
            int aJobs = Math.max(1, burstA / 100);
            int aRunsPerJob = burstA / aJobs;
            Load load = insertBurst(env, aJobs, aRunsPerJob, burstB);
            rep.setup(load, String.format(Locale.ROOT, "tenant A: %d jobs x %d runs (scheduled 1 us apart per job), "
                    + "tenant B: %d jobs x 1 run, all due at T; cap %d each", aJobs, aRunsPerJob, burstB, cap));
            CpuSampler cpu = awaitTerminal(env, load, 300 + aJobs * aRunsPerJob / 200);
            List<RunSample> samples = load(env, load);
            rep.section("Burst and fairness (all due at T)",
                    RunTables.burst(samples, "burst_a", "burst_b", load.t0Us()));
            rep.section("Runs", RunTables.summary(samples, 0) + "\n" + cpu.table());
            List<String> failures = sinkSection(rep, env, samples);
            rep.write(samples);
            return failures;
        }
    }

    /** Scenario 3: spread, one node killed at 0.4 x window and another frozen at 0.7 x window. */
    private List<String> chaos() throws Exception {
        Report rep = new Report("chaos");
        try (Env env = Env.start(sinkPool)) {
            rep.environment(env);
            List<String> ts = createTenants(env, tenants, "t");
            Load load = insertSpread(env, ts, jobs, windowSec);
            rep.setup(load, String.format(Locale.ROOT, "spread (%d jobs, %d tenants, %d s) with node %s killed at "
                    + "%.0f s and node %s frozen for %d s at %.0f s after T0", jobs, tenants, windowSec,
                    env.nodes.get(0).nodeId(), 0.4 * windowSec, env.nodes.get(1).nodeId(), freezeSec, 0.7 * windowSec));

            SchedulerNode victim = env.nodes.get(0);
            SchedulerNode frozen = env.nodes.get(1);
            List<Event> events = java.util.Collections.synchronizedList(new ArrayList<>());
            ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
            long toT0Ms = Duration.between(env.cluster.dbNow(), load.t0()).toMillis();
            ses.schedule(() -> {
                awaitBusy(env, victim.nodeId());
                Set<Integer> owned = Set.copyOf(victim.ownedPartitions());
                Instant at = env.cluster.dbNow();
                env.cluster.kill(victim);
                events.add(new Event("kill", victim.nodeId(), at, owned, runningOwnedBy(env, victim.nodeId())));
                System.out.printf("bench chaos: killed %s owning %d partitions%n", victim.nodeId(), owned.size());
            }, toT0Ms + Math.round(0.4 * windowSec * 1000), TimeUnit.MILLISECONDS);
            ses.schedule(() -> {
                awaitBusy(env, frozen.nodeId());
                Set<Integer> owned = Set.copyOf(frozen.ownedPartitions());
                Instant at = env.cluster.dbNow();
                frozen.freeze();
                events.add(new Event("freeze", frozen.nodeId(), at, owned, runningOwnedBy(env, frozen.nodeId())));
                System.out.printf("bench chaos: froze %s owning %d partitions%n", frozen.nodeId(), owned.size());
            }, toT0Ms + Math.round(0.7 * windowSec * 1000), TimeUnit.MILLISECONDS);
            ses.schedule(() -> {
                frozen.unfreeze();
                events.add(new Event("unfreeze", frozen.nodeId(), env.cluster.dbNow(), Set.of(), -1));
                System.out.printf("bench chaos: unfroze %s%n", frozen.nodeId());
            }, toT0Ms + Math.round(0.7 * windowSec * 1000) + freezeSec * 1000L, TimeUnit.MILLISECONDS);

            CpuSampler cpu = awaitTerminal(env, load, Math.max(windowSec + 300, (int) (0.7 * windowSec) + freezeSec + 120));
            ses.shutdown();
            ses.awaitTermination(Math.round(0.7 * windowSec) + freezeSec + 60L, TimeUnit.SECONDS);
            List<RunSample> samples = load(env, load);

            MarkdownTable ev = new MarkdownTable("event", "node", "at (s after T0, DB clock)", "partitions owned",
                    "RUNNING runs it held just after the event");
            List<Event> evs;
            synchronized (events) {
                evs = List.copyOf(events);
            }
            for (Event e : evs) {
                ev.row(e.kind(), e.node(), f1((e.at().toEpochMilli() - load.t0().toEpochMilli()) / 1000.0),
                        e.kind().equals("unfreeze") ? "-" : e.partitions().size(),
                        e.runningAtEvent() < 0 ? "-" : e.runningAtEvent());
            }
            rep.section("Chaos events", ev.toString());

            MarkdownTable lag = LagReport.table("runs");
            RunTables.lag(samples).row(lag, "all");
            Set<Integer> affected = new HashSet<>();
            for (Event e : evs) {
                if (e.kind().equals("unfreeze")) {
                    continue;
                }
                affected.addAll(e.partitions());
                long fromUs = e.at().toEpochMilli() * 1000;
                long toUs = fromUs + 60_000_000L;
                RunTables.lag(samples, s -> e.partitions().contains(s.partition())).row(lag,
                        e.kind() + " " + e.node() + ": its partitions, all runs");
                RunTables.lag(samples, s -> e.partitions().contains(s.partition())
                        && s.scheduledUs() >= fromUs && s.scheduledUs() <= toUs).row(lag,
                        e.kind() + " " + e.node() + ": its partitions, scheduled in [event, event + 60 s]");
                RunTables.lag(samples, s -> s.scheduledUs() >= fromUs && s.scheduledUs() <= toUs).row(lag,
                        "all partitions, scheduled in [" + e.kind() + ", " + e.kind() + " + 60 s]");
            }
            RunTables.lag(samples, s -> affected.contains(s.partition())).row(lag, "affected partitions (union), all runs");
            RunTables.lag(samples, s -> !affected.contains(s.partition())).row(lag, "unaffected partitions");
            rep.section("Fire lag (started_at - scheduled_at, DB clock)", lag.toString());

            MarkdownTable sig = new MarkdownTable("signal", "value");
            RunTables.Outcome o = RunTables.outcome(samples);
            sig.row("runs with lease_lost_count > 0", o.leaseLost());
            sig.row("runs with attempt > 1", o.retried());
            sig.row("dead-lettered", o.deadLettered());
            double takeovers = 0;
            for (SchedulerNode n : env.nodes) {
                if (n != victim) {
                    takeovers += n.metrics().orphanTakeoverCount();
                }
            }
            sig.row("orphan takeovers (surviving nodes' metric)", f0(takeovers));
            rep.section("Takeover signals", sig.toString());
            rep.section("Runs", RunTables.summary(samples, windowSec) + "\n" + cpu.table());
            rep.section("Per minute of the window", RunTables.perMinute(samples, load.t0Us()).toString());
            List<String> failures = sinkSection(rep, env, samples);
            if (evs.stream().filter(e -> !e.kind().equals("unfreeze")).count() < 2) {
                failures.add("chaos: not every event happened: " + evs);
            }
            rep.write(samples);
            return failures;
        }
    }

    /** Scenario 4: steps of increasing rate until p99 exceeds {@code bench.maxP99Ms}. */
    private List<String> ramp() throws Exception {
        Report rep = new Report("ramp");
        List<String> failures = new ArrayList<>();
        try (Env env = Env.start(sinkPool)) {
            rep.environment(env);
            List<String> ts = createTenants(env, tenants, "t");
            rep.text("Each step: rate x " + stepSec + " s fresh one-off runs, scheduled_at uniform over " + stepSec
                    + " s, " + tenants + " tenants (cap " + cap + "), started after the previous step drained; "
                    + "stops after the first step with p99 > " + maxP99Ms + " ms or one that does not drain.\n\n"
                    + "Definitions (DB clock, T0 = first scheduled instant of the step): achieved start rate = "
                    + "started / (max started_at - T0); completions/s = succeeded / (max finished_at - T0); "
                    + "state transitions/s = 3 x completions/s, counting per run one SCHEDULED->RUNNING claim, one "
                    + "checkpoint write (1 item = 1 chunk) and one RUNNING->SUCCEEDED commit.\n");
            MarkdownTable steps = new MarkdownTable("target runs/s", "runs", "insert s", "achieved starts/s",
                    "completions/s", "transitions/s", "p50 ms", "p99 ms", "max ms", "not drained", "lease lost",
                    "CPU avg %", "CPU max %");
            List<RunSample> all = new ArrayList<>();
            for (int rate : rates) {
                int n = rate * stepSec;
                Load load = insertSpread(env, ts, n, stepSec);
                CpuSampler cpu = awaitTerminal(env, load, stepSec + 180);
                List<RunSample> samples = load(env, load);
                all.addAll(samples);
                RunTables.Outcome o = RunTables.outcome(samples);
                LagReport lag = RunTables.lag(samples);
                long t0 = load.t0Us();
                double startSpan = o.lastStartUs() == RunSample.NONE ? 0 : (o.lastStartUs() - t0) / 1e6;
                double finishSpan = o.lastFinishUs() == RunSample.NONE ? 0 : (o.lastFinishUs() - t0) / 1e6;
                double completions = finishSpan <= 0 ? 0 : o.succeeded() / finishSpan;
                steps.row(rate, n, f1(load.insertMs() / 1000.0), startSpan <= 0 ? "-" : f0(o.started() / startSpan),
                        f0(completions), f0(3 * completions), f1(lag.percentileMillis(50)),
                        f1(lag.percentileMillis(99)), f1(lag.maxMillis()), o.notTerminal(), o.leaseLost(),
                        f0(cpu.avg()), f0(cpu.max()));
                System.out.printf("bench ramp: step %d runs/s done: p99 %.1f ms, not drained %d%n",
                        rate, lag.percentileMillis(99), o.notTerminal());
                if (lag.percentileMillis(99) > maxP99Ms || o.notTerminal() > 0) {
                    rep.text("Stopped after the " + rate + " runs/s step (p99 " + f1(lag.percentileMillis(99))
                            + " ms, not drained " + o.notTerminal() + ").\n");
                    break;
                }
            }
            rep.section("Steps", steps.toString());
            failures.addAll(sinkSection(rep, env, all));
            rep.write(all);
        }
        return failures;
    }

    // ================================================================== result sections

    private void spreadTables(Report rep, List<RunSample> samples, Load load, CpuSampler cpu) {
        rep.section("Runs", RunTables.summary(samples, load.windowSec()) + "\n" + cpu.table());
        MarkdownTable lag = LagReport.table("runs");
        RunTables.lag(samples).row(lag, "all");
        rep.section("Fire lag (started_at - scheduled_at, DB clock)", lag.toString());
        rep.section("Per minute of the window (minute of scheduled_at for lag; of started_at / finished_at for "
                + "starts / completions)", RunTables.perMinute(samples, load.t0Us()).toString());
    }

    /** Sink totals table; returns broken exactly-once invariants. */
    private List<String> sinkSection(Report rep, Env env, List<RunSample> samples) {
        SinkProcess.TotalStats t = env.sink.totals();
        long duplicates = t.delivered() - t.deliveredDistinct();
        long succeeded = samples.stream().filter(s -> s.state().equals("SUCCEEDED")).count();
        MarkdownTable m = new MarkdownTable("metric", "value");
        m.row("deliveredDistinct (distinct keys delivered)", t.deliveredDistinct());
        m.row("delivered (attempts with outcome DELIVERED)", t.delivered());
        m.row("alreadyDelivered (absorbed repeats)", t.alreadyDelivered());
        m.row("staleRejected (409 stale token)", t.staleRejected());
        m.row("attempts", t.attempts());
        m.row("duplicates = delivered - deliveredDistinct (must be 0)", duplicates);
        m.row("maxDeliveriesPerKey (must be 1)", t.maxDeliveriesPerKey());
        m.row("succeeded runs without a delivery (succeeded - deliveredDistinct)", succeeded - t.deliveredDistinct());
        rep.section("Sink (exactly-once effect)", m.toString());
        List<String> failures = new ArrayList<>();
        if (duplicates != 0) {
            failures.add(rep.scenario + ": duplicates = " + duplicates);
        }
        if (t.maxDeliveriesPerKey() != 1) {
            failures.add(rep.scenario + ": maxDeliveriesPerKey = " + t.maxDeliveriesPerKey());
        }
        long open = samples.stream().filter(s -> !s.terminal()).count();
        if (open > 0 && !rep.scenario.equals("ramp")) {
            failures.add(rep.scenario + ": " + open + " runs not terminal at the end");
        }
        return failures;
    }

    // ================================================================== load

    /** One batch of inserted runs: ids above {@code minIdExclusive}, first due at {@code t0}. */
    record Load(long minIdExclusive, Instant t0, int runs, int windowSec, long insertMs, long marginMs) {
        long t0Us() {
            return t0.toEpochMilli() * 1000;
        }
    }

    private List<String> createTenants(Env env, int n, String prefix) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String id = String.format(Locale.ROOT, "%s%02d", prefix, i);
            env.cluster.tenant(id, cap);
            ids.add(id);
        }
        return ids;
    }

    /** {@code n} one-off jobs with one run each, round robin over {@code ts}, uniform over the window. */
    private Load insertSpread(Env env, List<String> ts, int n, int window) throws SQLException {
        long minId = maxRunId(env);
        Instant now = env.cluster.dbNow();
        Instant t0 = now.plusSeconds(lead(n)).truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        SplittableRandom rnd = new SplittableRandom();
        long windowUs = window * 1_000_000L;
        long start = System.nanoTime();
        try (Connection c = env.cluster.admin().getConnection()) {
            c.setAutoCommit(false);
            for (int from = 0; from < n; from += BATCH) {
                int len = Math.min(BATCH, n - from);
                UUID[] ids = new UUID[len];
                String[] tenant = new String[len];
                Integer[] part = new Integer[len];
                Long[] off = new Long[len];
                for (int i = 0; i < len; i++) {
                    ids[i] = UUID.randomUUID();
                    tenant[i] = ts.get((from + i) % ts.size());
                    part[i] = PartitionHasher.partitionOf(ids[i]);
                    off[i] = rnd.nextLong(windowUs + 1);
                }
                insertJobsAndRuns(c, t0, ids, tenant, part, off);
                c.commit();
            }
        }
        return finishLoad(env, minId, t0, n, window, start);
    }

    private static void insertJobsAndRuns(Connection c, Instant t0, UUID[] ids, String[] tenant, Integer[] part,
                                          Long[] off) throws SQLException {
        OffsetDateTime at = OffsetDateTime.ofInstant(t0, ZoneOffset.UTC);
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO job (id, tenant_id, kind, handler, run_at, payload, priority, max_run_time,
                                 misfire, misfire_threshold, max_attempts, partition_id, version)
                SELECT u.id, u.t, 'ONE_OFF', 'PICKUP_ALERT', ?::timestamptz + u.off * interval '1 microsecond',
                       ?::jsonb, 5, interval '15 minutes', 'FIRE_NOW', interval '60 seconds', 5, u.p, 0
                  FROM unnest(?::uuid[], ?::text[], ?::int[], ?::bigint[]) AS u (id, t, p, off)""")) {
            ps.setObject(1, at);
            ps.setString(2, PAYLOAD);
            bindArrays(c, ps, 3, ids, tenant, part, off);
            ps.executeUpdate();
        }
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                     scheduled_at, due_at, idem_key, state)
                SELECT u.id, u.t, u.p, 5, 0, interval '15 minutes', s.at, s.at,
                       u.id || ':' || to_char(s.at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'), 'SCHEDULED'
                  FROM unnest(?::uuid[], ?::text[], ?::int[], ?::bigint[]) AS u (id, t, p, off)
                 CROSS JOIN LATERAL (SELECT ?::timestamptz + u.off * interval '1 microsecond' AS at) s""")) {
            bindArrays(c, ps, 1, ids, tenant, part, off);
            ps.setObject(5, at);
            ps.executeUpdate();
        }
    }

    private static void bindArrays(Connection c, PreparedStatement ps, int first, UUID[] ids, String[] tenant,
                                   Integer[] part, Long[] off) throws SQLException {
        ps.setArray(first, c.createArrayOf("uuid", ids));
        ps.setArray(first + 1, c.createArrayOf("text", tenant));
        ps.setArray(first + 2, c.createArrayOf("int4", part));
        ps.setArray(first + 3, c.createArrayOf("int8", off));
    }

    /** Burst: A {@code aJobs x aRuns} runs at T + 0..aRuns-1 us, B {@code bJobs x 1} at T. */
    private Load insertBurst(Env env, int aJobs, int aRuns, int bJobs) throws SQLException {
        long minId = maxRunId(env);
        int total = aJobs * aRuns + bJobs;
        Instant t0 = env.cluster.dbNow().plusSeconds(lead(total)).truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        OffsetDateTime at = OffsetDateTime.ofInstant(t0, ZoneOffset.UTC);
        long start = System.nanoTime();
        try (Connection c = env.cluster.admin().getConnection()) {
            c.setAutoCommit(false);
            for (Object[] tj : new Object[][] {{"burst_a", aJobs}, {"burst_b", bJobs}}) {
                int n = (Integer) tj[1];
                UUID[] ids = new UUID[n];
                Integer[] part = new Integer[n];
                for (int i = 0; i < n; i++) {
                    ids[i] = UUID.randomUUID();
                    part[i] = PartitionHasher.partitionOf(ids[i]);
                }
                try (PreparedStatement ps = c.prepareStatement("""
                        INSERT INTO job (id, tenant_id, kind, handler, run_at, payload, priority, max_run_time,
                                         misfire, misfire_threshold, max_attempts, partition_id, version)
                        SELECT u.id, ?, 'ONE_OFF', 'PICKUP_ALERT', ?::timestamptz, ?::jsonb, 5,
                               interval '15 minutes', 'FIRE_NOW', interval '60 seconds', 5, u.p, 0
                          FROM unnest(?::uuid[], ?::int[]) AS u (id, p)""")) {
                    ps.setString(1, (String) tj[0]);
                    ps.setObject(2, at);
                    ps.setString(3, PAYLOAD);
                    ps.setArray(4, c.createArrayOf("uuid", ids));
                    ps.setArray(5, c.createArrayOf("int4", part));
                    ps.executeUpdate();
                }
            }
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                         scheduled_at, due_at, idem_key, state)
                    SELECT j.id, j.tenant_id, j.partition_id, j.priority, j.version, j.max_run_time,
                           j.run_at + g * interval '1 microsecond', j.run_at + g * interval '1 microsecond',
                           j.id || ':' || g, 'SCHEDULED'
                      FROM job j
                      JOIN generate_series(0, ? - 1) g ON (j.tenant_id = 'burst_a' OR g = 0)
                     WHERE j.tenant_id IN ('burst_a', 'burst_b')""")) {
                ps.setInt(1, aRuns);
                ps.executeUpdate();
            }
            c.commit();
        }
        return finishLoad(env, minId, t0, total, 0, start);
    }

    private Load finishLoad(Env env, long minId, Instant t0, int n, int window, long startNanos) {
        long insertMs = (System.nanoTime() - startNanos) / 1_000_000;
        long marginMs = Duration.between(env.cluster.dbNow(), t0).toMillis();
        long inserted = env.cluster.jdbc().sql("SELECT count(*) FROM job_run WHERE id > ?").param(minId)
                .query(Long.class).single();
        System.out.printf("bench: inserted %d runs in %d ms, committed %d ms before the first due time%n",
                inserted, insertMs, marginMs);
        assertThat(inserted).isEqualTo(n);
        if (marginMs <= 0) {
            // runs were due before they were visible: their lag would include the insert itself
            throw new IllegalStateException("the insert committed " + (-marginMs) + " ms after the first due time; "
                    + "rerun with a larger -Dbench.leadSec (now " + leadSec + ")");
        }
        return new Load(minId, t0, n, window, insertMs, marginMs);
    }

    /** Lead before the first due time: at least bench.leadSec, and 5 s + 1 s per 8,000 rows. */
    private int lead(int rows) {
        return Math.max(leadSec, 5 + rows / 8_000);
    }

    private static long maxRunId(Env env) {
        return env.cluster.jdbc().sql("SELECT coalesce(max(id), 0) FROM job_run").query(Long.class).single();
    }

    private static List<RunSample> load(Env env, Load load) throws SQLException {
        try (Connection c = env.cluster.admin().getConnection()) {
            return RunSample.load(c, "id > ?", load.minIdExclusive());
        }
    }

    /** Waits until every run of {@code load} is terminal, at most {@code boundSec} after T0. */
    private static CpuSampler awaitTerminal(Env env, Load load, int boundSec) throws Exception {
        CpuSampler cpu = new CpuSampler();
        long deadline = load.t0().toEpochMilli() + boundSec * 1000L;
        long lastPrint = 0;
        try (Connection c = env.cluster.admin().getConnection()) {
            while (true) {
                long[] p = RunSample.progress(c, "id > ?", load.minIdExclusive());
                long nowMs = env.cluster.dbNow().toEpochMilli();
                if (nowMs >= load.t0().toEpochMilli()) {
                    cpu.sample();
                }
                boolean done = p[1] >= load.runs();
                if (done || nowMs > deadline) {
                    System.out.printf("bench: %s: runs=%d terminal=%d running=%d started=%d%n",
                            done ? "all terminal" : "DEADLINE", p[0], p[1], p[2], p[3]);
                    return cpu;
                }
                if (System.nanoTime() - lastPrint > 10_000_000_000L) {
                    System.out.printf("bench: t=%+.0f s runs=%d terminal=%d running=%d started=%d%n",
                            (nowMs - load.t0().toEpochMilli()) / 1000.0, p[0], p[1], p[2], p[3]);
                    lastPrint = System.nanoTime();
                }
                Thread.sleep(1_000);
            }
        }
    }

    // ================================================================== environment

    /** PG + sink + 3 nodes with production settings, ownership stable. */
    static final class Env implements AutoCloseable {
        final EmbeddedPg pg;
        final SinkProcess sink;
        final SchedulerCluster cluster;
        final List<SchedulerNode> nodes = new ArrayList<>();

        private Env(EmbeddedPg pg, SinkProcess sink, SchedulerCluster cluster) {
            this.pg = pg;
            this.sink = sink;
            this.cluster = cluster;
        }

        static Env start(int sinkPool) {
            EmbeddedPg pg = EmbeddedPg.durable(Map.of("shared_buffers", "512MB", "max_wal_size", "4GB",
                    "checkpoint_timeout", "15min", "fsync", "on", "synchronous_commit", "on"));
            SinkProcess sink = null;
            try {
                sink = SinkProcess.start(pg, "bench_sink", sinkPool);
                sink.setLatency(LATENCY_MS, JITTER_MS);
                SchedulerCluster cluster = new SchedulerCluster(pg, "bench_sched", sink.baseUrl());
                Env env = new Env(pg, sink, cluster);
                for (String id : List.of("bench-1", "bench-2", "bench-3")) {
                    env.nodes.add(cluster.start(EngineSettings.builder(id).build()));
                }
                awaitStableOwnership(env.nodes);
                return env;
            } catch (RuntimeException e) {
                if (sink != null) {
                    sink.close();
                }
                pg.close();
                throw e;
            }
        }

        @Override
        public void close() {
            try {
                cluster.close();
            } finally {
                try {
                    sink.close();
                } finally {
                    pg.close();
                }
            }
        }
    }

    /** All 128 partitions owned exactly once, every node owning some, unchanged for 3 s. */
    private static void awaitStableOwnership(List<SchedulerNode> nodes) {
        AtomicReference<List<Set<Integer>>> last = new AtomicReference<>();
        AtomicLong stableSince = new AtomicLong();
        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(200)).until(() -> {
            List<Set<Integer>> now = new ArrayList<>();
            Set<Integer> all = new HashSet<>();
            int sum = 0;
            for (SchedulerNode n : nodes) {
                Set<Integer> owned = Set.copyOf(n.ownedPartitions());
                now.add(owned);
                all.addAll(owned);
                sum += owned.size();
            }
            boolean complete = all.size() == PARTITIONS && sum == PARTITIONS && now.stream().noneMatch(Set::isEmpty);
            if (!complete || !now.equals(last.get())) {
                last.set(now);
                stableSince.set(System.nanoTime());
                return false;
            }
            return System.nanoTime() - stableSince.get() >= Duration.ofSeconds(3).toNanos();
        });
    }

    /**
     * Waits (at most 2 s) until {@code node} holds at least one RUNNING run, so the kill or freeze
     * lands mid-run and exercises the orphan takeover. Runs are claimed in batches on the 200 ms
     * poll and last 20-60 ms, so at spread rates a node is idle most of each poll interval; a
     * blindly timed event usually hits an idle node and tests only the partition handover.
     */
    private static void awaitBusy(Env env, String node) {
        long deadline = System.nanoTime() + 2_000_000_000L;
        while (System.nanoTime() < deadline && runningOwnedBy(env, node) == 0) {
            Thread.onSpinWait();
        }
    }

    /** RUNNING runs whose owner is {@code node} (DB), read right after a chaos event. */
    private static long runningOwnedBy(Env env, String node) {
        return env.cluster.jdbc().sql("SELECT count(*) FROM job_run WHERE state = 'RUNNING' AND owner_node = ?")
                .param(node).query(Long.class).single();
    }

    record Event(String kind, String node, Instant at, Set<Integer> partitions, long runningAtEvent) {
    }

    /** System-wide CPU load (0..100 %) sampled about once a second while runs are due. */
    static final class CpuSampler {
        private final com.sun.management.OperatingSystemMXBean os =
                (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        private double sum;
        private double max;
        private int n;

        void sample() {
            double v = os.getCpuLoad();
            if (v >= 0) {
                sum += v * 100;
                max = Math.max(max, v * 100);
                n++;
            }
        }

        double avg() {
            return n == 0 ? 0 : sum / n;
        }

        double max() {
            return max;
        }

        String table() {
            return new MarkdownTable("machine CPU while runs were due", "value")
                    .row("average %", f0(avg())).row("max %", f0(max())).row("samples", n).toString();
        }
    }

    // ================================================================== report

    /** Markdown report of one scenario: printed and written to target/bench. */
    final class Report {
        final String scenario;
        private final StringBuilder sb = new StringBuilder();
        private final String stamp = STAMP.format(Instant.now());

        Report(String scenario) {
            this.scenario = scenario;
            sb.append("# In-process benchmark: ").append(scenario).append("\n\n")
                    .append("Run ").append(Instant.now()).append(" (UTC)\n\n");
        }

        void environment(Env env) {
            Runtime rt = Runtime.getRuntime();
            var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            MarkdownTable j = new MarkdownTable("environment", "value");
            j.row("JVM", System.getProperty("java.vm.name") + " " + System.getProperty("java.runtime.version"));
            j.row("OS", System.getProperty("os.name") + " " + System.getProperty("os.version") + " "
                    + System.getProperty("os.arch"));
            String cpuId = System.getenv("PROCESSOR_IDENTIFIER");
            if (cpuId != null) {
                j.row("CPU", cpuId);
            }
            j.row("availableProcessors", rt.availableProcessors());
            j.row("physical memory (GiB)", f1(os.getTotalMemorySize() / (1024.0 * 1024 * 1024)));
            j.row("max heap (MiB)", rt.maxMemory() / (1024 * 1024));
            j.row("JVM input arguments", String.join(" ", ManagementFactory.getRuntimeMXBean().getInputArguments()));
            j.row("scheduler nodes", env.nodes.size() + " in this JVM, production EngineSettings: workers 64 (1 "
                    + "reserved for recovery), poll 200 ms, run lease 10 s (renew 3 s), orphan sweep 500 ms, "
                    + "partition lease 6 s, 128 partitions");
            j.row("sink", "notification-sink jar in its own JVM (-Xmx768m), Hikari pool " + sinkPool
                    + ", injected latency " + LATENCY_MS + " ms + U[0, " + JITTER_MS + "] ms per call");
            MarkdownTable p = new MarkdownTable("postgres setting", "value");
            for (String s : List.of("server_version", "shared_buffers", "fsync", "synchronous_commit", "full_page_writes", "wal_level",
                    "max_connections", "work_mem", "checkpoint_timeout", "max_wal_size")) {
                p.row(s, env.pg.show(s));
            }
            section("Environment", j + "\n" + p);
        }

        void setup(Load load, String what) {
            MarkdownTable t = new MarkdownTable("load", "value");
            t.row("workload", what);
            t.row("runs inserted", load.runs());
            t.row("insert time (ms)", load.insertMs());
            t.row("insert committed before the first due time by (ms)", load.marginMs());
            t.row("T0 (first due instant, DB clock)", load.t0());
            section("Load", t.toString());
        }

        void section(String title, String body) {
            String s = "## " + title + "\n\n" + body + (body.endsWith("\n") ? "\n" : "\n\n");
            sb.append(s);
            System.out.print(s);
        }

        void text(String s) {
            sb.append(s).append('\n');
            System.out.println(s);
        }

        void write(List<RunSample> samples) throws IOException {
            Path dir = Path.of("target", "bench");
            Files.createDirectories(dir);
            Path md = dir.resolve(scenario + "-" + stamp + ".md");
            Files.writeString(md, sb.toString(), StandardCharsets.UTF_8);
            RunSample.writeCsv(dir.resolve(scenario + "-" + stamp + "-lags.csv"), samples);
            System.out.println("bench: report written to " + md.toAbsolutePath());
        }
    }

    // ================================================================== misc

    private static int intProp(String name, int def) {
        String v = System.getProperty(name);
        return v == null || v.isBlank() ? def : Integer.parseInt(v.trim().replace("_", ""));
    }

    /** Engine and Hikari logs at WARN: the default logback configuration would log DEBUG to the console. */
    private static void quietLogs() {
        if (LoggerFactory.getILoggerFactory() instanceof ch.qos.logback.classic.LoggerContext ctx) {
            ctx.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(ch.qos.logback.classic.Level.WARN);
        }
    }
}
