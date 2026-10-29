package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.app.engine.support.TestCluster;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.core.domain.HandlerType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * R7, plan sections 10 and 21 issue 2, <b>no tolerance</b>: cap 5 for two tenants, 1,000 runs, 3+
 * nodes with random kills, freezes past the lease, joins and graceful leaves (partition moves).
 * Every 10 ms: RUNNING runs with a valid lease per tenant ≤ 5, and sink calls admitted with a
 * valid token (the strict sink checks the DB lease at admission) and still in flight ≤ 5. The sink
 * also records its own maximum per tenant, checked at every admission, not only at samples.
 */
class TenantCapIT {

    private static final int CAP = 5;
    private static final List<String> TENANTS = List.of("cap-a", "cap-b");

    private static EngineTestDb db;
    private static Fixtures fx;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("tenant_cap_it");
        fx = new Fixtures(db.jdbc());
    }

    @AfterAll
    static void close() {
        db.close();
    }

    @Test
    void capHoldsAtEverySampleUnderChaos() throws Exception {
        Map<Long, String> tenantOfRun = new ConcurrentHashMap<>();
        for (String t : TENANTS) {
            fx.tenant(t, CAP);
            for (int j = 0; j < 5; j++) {
                UUID job = fx.job(t).handler(HandlerType.ROSTER_GEN).maxAttempts(20).insert();
                fx.dueRuns(job, t.equals("cap-a") ? 120 : 80).forEach(id -> tenantOfRun.put(id, t));
            }
        }
        assertThat(tenantOfRun).hasSize(1_000);

        JdbcClient strict = JdbcClient.create(db.newPool("strict-sink", 8));
        FakeEffectSink sink = new FakeEffectSink();
        sink.setGroupOf(runId -> tenantOfRun.getOrDefault(runId, "?"));
        sink.setHoldAfterAdmission(Duration.ofMillis(5));
        sink.setAdmissionValidator((runId, token) -> strict.sql("""
                SELECT count(*) FROM job_run WHERE id = ? AND (fencing_epoch, fencing_seq) = (?, ?)
                   AND state = 'RUNNING' AND lease_until >= clock_timestamp()""")
                .params(runId, token.epoch(), token.seq()).query(Long.class).single() == 1);

        Random random = new Random(42);
        ItScriptedHandler handler = new ItScriptedHandler(HandlerType.ROSTER_GEN, (ctx, n) -> {
            ctx.send(ctx.run().idemKey(), "{\"it\":true}");
            Thread.sleep(5 + ThreadLocalRandomHolder.next(25));
        });

        List<String> violations = new CopyOnWriteArrayList<>();
        AtomicInteger samples = new AtomicInteger();
        AtomicBoolean sampling = new AtomicBoolean(true);
        try (TestCluster cluster = new TestCluster(db, sink, new HandlerRegistry(List.of(handler)))) {
            List<SchedulerNode> live = new CopyOnWriteArrayList<>();
            AtomicInteger names = new AtomicInteger();
            for (int i = 0; i < 3; i++) {
                live.add(cluster.start(TestCluster.fast("cap-n" + names.incrementAndGet()).workers(8).build()));
            }
            Thread sampler = Thread.ofPlatform().start(() -> {
                while (sampling.get()) {
                    List<Object[]> rows = db.jdbc().sql("""
                            SELECT tenant_id, count(*) FILTER (WHERE lease_until >= clock_timestamp()), count(*)
                              FROM job_run WHERE state = 'RUNNING' GROUP BY tenant_id""")
                            .query((rs, i) -> new Object[] {rs.getString(1), rs.getLong(2), rs.getLong(3)}).list();
                    for (Object[] r : rows) {
                        if ((long) r[1] > CAP || (long) r[2] > CAP) {
                            violations.add("tenant " + r[0] + " leased running " + r[1] + ", running " + r[2]);
                        }
                    }
                    for (String t : TENANTS) {
                        if (sink.inFlight(t) > CAP) {
                            violations.add("tenant " + t + " in-flight valid-token calls " + sink.inFlight(t));
                        }
                    }
                    samples.incrementAndGet();
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });

            // chaos while work is in progress: kill, freeze past the lease, join, graceful leave
            List<String> actions = new ArrayList<>();
            long chaosEnd = System.nanoTime() + Duration.ofSeconds(12).toNanos();
            // a fixed cycle so every kind of fault happens in every run; victims are random (seeded)
            for (int step = 0; step < 8 && System.nanoTime() < chaosEnd && remaining() > 100; step++) {
                int action = step % 4;
                SchedulerNode victim = live.get(random.nextInt(live.size()));
                switch (action) {
                    case 0 -> { // kill -9 and replace
                        cluster.kill(victim);
                        live.remove(victim);
                        live.add(cluster.start(TestCluster.fast("cap-n" + names.incrementAndGet()).workers(8).build()));
                        actions.add("kill " + victim.nodeId());
                    }
                    case 1 -> { // freeze past the 3 s lease
                        victim.freeze();
                        Thread.sleep(3_500);
                        victim.unfreeze();
                        actions.add("freeze " + victim.nodeId());
                    }
                    case 2 -> { // join: partitions move to the new node
                        live.add(cluster.start(TestCluster.fast("cap-n" + names.incrementAndGet()).workers(8).build()));
                        actions.add("join");
                    }
                    default -> { // graceful leave, if enough nodes remain
                        if (live.size() > 2) {
                            live.remove(victim);
                            victim.stop(Duration.ofSeconds(5));
                            actions.add("leave " + victim.nodeId());
                        }
                    }
                }
                Thread.sleep(400);
            }

            await().atMost(Duration.ofSeconds(90)).until(() -> remaining() == 0);
            sampling.set(false);
            sampler.join();

            System.out.println("TenantCapIT: chaos=" + actions + " samples=" + samples.get()
                    + " maxInFlight a=" + sink.maxInFlight("cap-a") + " b=" + sink.maxInFlight("cap-b")
                    + " stale=" + sink.staleRejections());
            assertThat(violations).isEmpty();
            assertThat(samples.get()).isGreaterThan(300);
            for (String t : TENANTS) {
                assertThat(sink.maxInFlight(t)).isLessThanOrEqualTo(CAP);
            }
            assertThat(actions).isNotEmpty();
            assertThat(sink.distinctDeliveries()).isEqualTo(1_000);
            assertThat(sink.maxDeliveriesPerKey()).isEqualTo(1);
        }
    }

    private static long remaining() {
        return fx.count("SELECT count(*) FROM job_run WHERE tenant_id IN ('cap-a', 'cap-b') AND state <> 'SUCCEEDED'");
    }

    /** Per-thread randomness for handler sleeps. */
    private static final class ThreadLocalRandomHolder {
        static int next(int bound) {
            return java.util.concurrent.ThreadLocalRandom.current().nextInt(bound);
        }
    }
}
