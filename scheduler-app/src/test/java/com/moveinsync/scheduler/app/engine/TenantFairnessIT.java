package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.app.engine.support.TestCluster;
import com.moveinsync.scheduler.app.handler.BillingCloseHandler;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.app.handler.NoShowCancelHandler;
import com.moveinsync.scheduler.app.handler.RosterGenerationHandler;
import com.moveinsync.scheduler.core.cluster.PartitionHasher;
import com.moveinsync.scheduler.core.domain.HandlerType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * R7 (plan sections 10 and 16): tenant A floods 50,000 runs and tenant B has 20, both cap 32, all
 * scheduled at the same instant. B must not queue behind A: B's p99 fire lag ({@code started_at -
 * scheduled_at}, DB clock) stays under 1 s.
 *
 * <p>Setup: <b>3 nodes</b> with production claim timings (poll 200 ms, 64 workers, default
 * partition/run leases), the cluster of the plan's section 10 table. The handler simulates I/O with
 * a 20-40 ms sleep and one sink call. Runs are inserted only after all 128 partitions are owned and
 * ownership has been stable, so no partition handover can delay a run. A has 500 jobs x 100 runs
 * (scheduled 1 microsecond apart, since {@code UNIQUE (job_id, scheduled_at)}), B 20 jobs x 1 run.
 * The test stops once B is done; A is not drained.
 */
class TenantFairnessIT {

    private static final String A = "fair_a";
    private static final String B = "fair_b";
    private static final int CAP = 32;
    private static final int A_JOBS = 500;
    private static final int A_RUNS_PER_JOB = 100;
    private static final int B_JOBS = 20;
    /** Lead time of the common scheduled instant; 2 s is too short for a 50k-row insert on a loaded laptop. */
    private static final Duration LEAD = Duration.ofSeconds(10);

    private static EngineTestDb db;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("tenant_fairness_it");
    }

    @AfterAll
    static void close() {
        if (db != null) {
            db.close();
        }
    }

    @Test
    void smallTenantIsNotStuckBehindAFlood() throws Exception {
        Fixtures fx = new Fixtures(db.jdbc());
        fx.tenant(A, CAP);
        fx.tenant(B, CAP);
        insertJobs(A, A_JOBS);
        insertJobs(B, B_JOBS);

        ObjectMapper mapper = new ObjectMapper();
        FakeEffectSink sink = new FakeEffectSink();
        ItScriptedHandler alert = new ItScriptedHandler(HandlerType.PICKUP_ALERT, (ctx, execution) -> {
            Thread.sleep(ThreadLocalRandom.current().nextLong(20, 41)); // simulated I/O
            ctx.send(ctx.run().idemKey(), "{\"event\":\"CAB_ASSIGNED\"}");
        });
        HandlerRegistry registry = new HandlerRegistry(List.of(alert, new RosterGenerationHandler(mapper),
                new NoShowCancelHandler(mapper), new BillingCloseHandler(mapper)));

        try (TestCluster cluster = new TestCluster(db, sink, registry)) {
            List<SchedulerNode> nodes = new ArrayList<>();
            for (String id : List.of("fair1", "fair2", "fair3")) {
                nodes.add(cluster.start(EngineSettings.builder(id)
                        .pollInterval(Duration.ofMillis(200))
                        .workers(64)
                        .build()));
            }
            awaitStableOwnership(nodes);

            long[] insert = insertRuns();
            long insertMs = insert[0];
            long marginMs = insert[1];
            System.out.printf("TenantFairnessIT: inserted %d runs in %d ms, committed %d ms before scheduled_at%n",
                    A_JOBS * A_RUNS_PER_JOB + B_JOBS, insertMs, marginMs);
            assertThat(marginMs).as("bulk insert must commit before the runs are due").isPositive();

            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100)).until(() ->
                    fx.count("SELECT count(*) FROM job_run WHERE tenant_id = ? AND state = 'SUCCEEDED'", B) == B_JOBS);

            long aDone = fx.count("SELECT count(*) FROM job_run WHERE tenant_id = ? AND state = 'SUCCEEDED'", A);
            long aRunning = fx.count("SELECT count(*) FROM job_run WHERE tenant_id = ? AND state = 'RUNNING'", A);
            long aStarted = fx.count("SELECT count(*) FROM job_run WHERE tenant_id = ? AND started_at IS NOT NULL", A);
            List<Long> bLagMs = db.jdbc().sql("""
                    SELECT (extract(epoch FROM (started_at - scheduled_at)) * 1000)::bigint
                      FROM job_run WHERE tenant_id = ? ORDER BY 1""").param(B).query(Long.class).list();
            Map<String, Object> aLag = db.jdbc().sql("""
                    SELECT (extract(epoch FROM min(started_at - scheduled_at)) * 1000)::bigint AS min_ms,
                           (extract(epoch FROM max(started_at - scheduled_at)) * 1000)::bigint AS max_ms
                      FROM job_run WHERE tenant_id = ? AND started_at IS NOT NULL""").param(A).query().singleRow();
            long bMax = bLagMs.get(bLagMs.size() - 1);
            long bP50 = bLagMs.get(bLagMs.size() / 2);
            System.out.printf("TenantFairnessIT: nodes=3 B lag ms: p50=%d p99(max of 20)=%d all=%s; "
                            + "A at that moment: succeeded=%d running=%d started=%d of %d, A lag ms min=%s max=%s%n",
                    bP50, bMax, bLagMs, aDone, aRunning, aStarted, A_JOBS * A_RUNS_PER_JOB,
                    aLag.get("min_ms"), aLag.get("max_ms"));

            assertThat(bLagMs).hasSize(B_JOBS);
            assertThat(bMax).as("B p99 fire lag (max of 20 runs)").isLessThan(1000);
            // A was really flooding: it was running at its cap and far from drained
            assertThat(aRunning).isLessThanOrEqualTo(CAP);
            assertThat(aDone).isLessThan(A_JOBS * A_RUNS_PER_JOB);
            assertThat(fx.count("SELECT count(*) FROM job_run WHERE state = 'RUNNING'")).isLessThanOrEqualTo(2L * CAP);

            // informational only: A's drain rate over a 2 s window (plan section 10 expects ~800/s at cap 32)
            long windowStart = System.nanoTime();
            long before = fx.count("SELECT count(*) FROM job_run WHERE tenant_id = ? AND state = 'SUCCEEDED'", A);
            Thread.sleep(2_000);
            long after = fx.count("SELECT count(*) FROM job_run WHERE tenant_id = ? AND state = 'SUCCEEDED'", A);
            double seconds = (System.nanoTime() - windowStart) / 1e9;
            System.out.printf("TenantFairnessIT: A drain rate %.0f runs/s (%d runs in %.2f s, cap %d)%n",
                    (after - before) / seconds, after - before, seconds, CAP);
            for (SchedulerNode n : nodes) {
                io.micrometer.core.instrument.Timer claims = n.metrics().registry().find("scheduler.claim.batch").timer();
                if (claims != null) {
                    System.out.printf("TenantFairnessIT: %s claim transactions=%d mean=%.1f ms max=%.1f ms%n",
                            n.nodeId(), claims.count(), claims.mean(java.util.concurrent.TimeUnit.MILLISECONDS),
                            claims.max(java.util.concurrent.TimeUnit.MILLISECONDS));
                }
            }
        }
    }

    /** All 128 partitions owned exactly once, every node owning some, unchanged for 3 s. */
    private static void awaitStableOwnership(List<SchedulerNode> nodes) {
        AtomicReference<List<Set<Integer>>> last = new AtomicReference<>();
        AtomicLong stableSince = new AtomicLong();
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200)).until(() -> {
            List<Set<Integer>> now = new ArrayList<>();
            Set<Integer> all = new HashSet<>();
            int sum = 0;
            for (SchedulerNode n : nodes) {
                Set<Integer> owned = Set.copyOf(n.ownedPartitions());
                now.add(owned);
                all.addAll(owned);
                sum += owned.size();
            }
            boolean complete = all.size() == 128 && sum == 128 && now.stream().noneMatch(Set::isEmpty);
            if (!complete || !now.equals(last.get())) {
                last.set(now);
                stableSince.set(System.nanoTime());
                return false;
            }
            return System.nanoTime() - stableSince.get() >= Duration.ofSeconds(3).toNanos();
        });
    }

    /** Job definitions (not pending: their runs are inserted directly), partition = murmur3(id) % 128. */
    private static void insertJobs(String tenant, int n) throws Exception {
        UUID[] ids = new UUID[n];
        Integer[] partitions = new Integer[n];
        for (int i = 0; i < n; i++) {
            ids[i] = UUID.randomUUID();
            partitions[i] = PartitionHasher.partitionOf(ids[i]);
        }
        try (Connection c = db.admin().getConnection();
             PreparedStatement ps = c.prepareStatement("""
                     INSERT INTO job (id, tenant_id, kind, handler, run_at, payload, priority, max_run_time,
                                      misfire, misfire_threshold, max_attempts, partition_id, version)
                     SELECT u.id, ?, 'ONE_OFF', 'PICKUP_ALERT', clock_timestamp(), '{}'::jsonb, 5,
                            interval '5 minutes', 'FIRE_NOW', interval '60 seconds', 5, u.p, 0
                       FROM unnest(?::uuid[], ?::int[]) AS u (id, p)""")) {
            ps.setString(1, tenant);
            ps.setArray(2, c.createArrayOf("uuid", ids));
            ps.setArray(3, c.createArrayOf("int4", partitions));
            assertThat(ps.executeUpdate()).isEqualTo(n);
        }
    }

    /**
     * One INSERT for all 50,020 runs at T = now + {@link #LEAD} (A's runs T + 0..99 microseconds per job).
     *
     * @return {insert duration ms, ms between the commit and T}
     */
    private static long[] insertRuns() throws Exception {
        long t0 = System.nanoTime();
        try (Connection c = db.admin().getConnection();
             PreparedStatement ps = c.prepareStatement("""
                     WITH t AS (SELECT date_trunc('milliseconds', clock_timestamp()) + ? * interval '1 millisecond' AS at)
                     INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                          scheduled_at, due_at, idem_key, state)
                     SELECT j.id, j.tenant_id, j.partition_id, j.priority, j.version, j.max_run_time,
                            t.at + g * interval '1 microsecond', t.at + g * interval '1 microsecond',
                            j.id || ':' || g, 'SCHEDULED'
                       FROM job j CROSS JOIN t
                       JOIN generate_series(0, ? - 1) g ON (j.tenant_id = ? OR g = 0)""")) {
            ps.setLong(1, LEAD.toMillis());
            ps.setInt(2, A_RUNS_PER_JOB);
            ps.setString(3, A);
            assertThat(ps.executeUpdate()).isEqualTo(A_JOBS * A_RUNS_PER_JOB + B_JOBS);
        }
        long insertMs = (System.nanoTime() - t0) / 1_000_000;
        long marginMs = db.jdbc().sql("""
                SELECT (extract(epoch FROM (min(scheduled_at) - clock_timestamp())) * 1000)::bigint FROM job_run""")
                .query(Long.class).single();
        return new long[] {insertMs, marginMs};
    }
}
