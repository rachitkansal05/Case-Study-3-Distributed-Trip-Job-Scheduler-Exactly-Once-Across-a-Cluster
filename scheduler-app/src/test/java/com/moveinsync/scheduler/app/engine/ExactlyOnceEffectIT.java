package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.app.engine.support.TestCluster;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.app.handler.NoShowCancelHandler;
import com.moveinsync.scheduler.app.handler.PickupAlertHandler;
import com.moveinsync.scheduler.app.handler.BillingCloseHandler;
import com.moveinsync.scheduler.core.cluster.PartitionHasher;
import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.policy.RetryableJobException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * R3 (plan section 16): 3 in-process nodes, 10,000 one-off jobs over 10 tenants (cap 32 each),
 * random handler failures <b>after</b> the effect was sent, so every retry resends the same key.
 * The sink must see exactly 10,000 distinct keys, none delivered twice, and more than 10,000
 * attempts.
 */
class ExactlyOnceEffectIT {

    private static final int TENANTS = 10;
    private static final int CAP = 32;
    private static final int JOBS = 10_000;
    private static final int MAX_ATTEMPTS = 12;
    /** From this attempt on the handler never fails, so no job can dead-letter by bad luck. */
    private static final int LAST_FAILING_ATTEMPT = 9;
    private static final double FAILURE_RATE = 0.3;

    private static EngineTestDb db;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("exactly_once_effect_it");
    }

    @AfterAll
    static void close() {
        if (db != null) {
            db.close();
        }
    }

    @Test
    void tenThousandJobsWithRandomFailuresDeliverEachKeyExactlyOnce() throws Exception {
        Fixtures fx = new Fixtures(db.jdbc());
        for (int t = 0; t < TENANTS; t++) {
            fx.tenant(tenant(t), CAP);
        }
        insertPendingOneOffs(JOBS);

        FakeEffectSink sink = new FakeEffectSink();
        ObjectMapper mapper = new ObjectMapper();
        ItScriptedHandler handler = new ItScriptedHandler(HandlerType.ROSTER_GEN, (ctx, execution) -> {
            ctx.send(ctx.run().idemKey(), "{\"event\":\"ROSTER_READY\"}");
            if (ctx.run().attempt() <= LAST_FAILING_ATTEMPT
                    && ThreadLocalRandom.current().nextDouble() < FAILURE_RATE) {
                throw new RetryableJobException("IT_FAIL_AFTER_SEND", "injected failure after the effect");
            }
        });
        HandlerRegistry registry = new HandlerRegistry(List.of(handler, new PickupAlertHandler(mapper),
                new NoShowCancelHandler(mapper), new BillingCloseHandler(mapper)));

        long t0 = System.nanoTime();
        long elapsedMs;
        double success = 0;
        double retryable = 0;
        double permanent = 0;
        double timeout = 0;
        double staleCommits = 0;
        try (TestCluster cluster = new TestCluster(db, sink, registry)) {
            List<SchedulerNode> nodes = List.of(
                    cluster.start(TestCluster.fast("eo1").workers(32).build()),
                    cluster.start(TestCluster.fast("eo2").workers(32).build()),
                    cluster.start(TestCluster.fast("eo3").workers(32).build()));

            await().atMost(Duration.ofSeconds(100)).pollInterval(Duration.ofMillis(250)).until(() ->
                    fx.count("SELECT count(*) FROM job_run WHERE state = 'SUCCEEDED'") == JOBS);
            elapsedMs = (System.nanoTime() - t0) / 1_000_000;

            for (SchedulerNode n : nodes) {
                success += n.metrics().outcomeCount("success");
                retryable += n.metrics().outcomeCount("retryable");
                permanent += n.metrics().outcomeCount("permanent");
                timeout += n.metrics().outcomeCount("timeout");
                staleCommits += n.metrics().staleCommitRejectedCount();
            }
        }

        long alreadyDelivered = sink.attempts().stream()
                .filter(a -> a.outcome() == DeliveryOutcome.ALREADY_DELIVERED).count();
        System.out.printf("ExactlyOnceEffectIT: jobs=%d distinctDeliveries=%d maxDeliveriesPerKey=%d "
                        + "sinkAttempts=%d alreadyDelivered=%d staleRejections=%d handlerCalls=%d "
                        + "outcomes{success=%.0f retryable=%.0f permanent=%.0f timeout=%.0f} staleCommits=%.0f "
                        + "duration=%d ms%n",
                JOBS, sink.distinctDeliveries(), sink.maxDeliveriesPerKey(), sink.attemptCount(), alreadyDelivered,
                sink.staleRejections(), handler.totalExecutions(), success, retryable, permanent, timeout,
                staleCommits, elapsedMs);

        // every job got exactly one run, and every run succeeded
        assertThat(fx.count("SELECT count(*) FROM job_run")).isEqualTo(JOBS);
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE state <> 'SUCCEEDED'")).isZero();
        assertThat(fx.count("SELECT count(*) FROM job WHERE next_fire_at IS NOT NULL")).isZero();
        assertThat(fx.count("SELECT count(*) FROM dead_letter")).isZero();
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE attempt > 1")).isPositive();

        // the effect side: one delivery per key, never two, with retries resending the same keys
        assertThat(sink.distinctDeliveries()).isEqualTo(JOBS);
        assertThat(sink.maxDeliveriesPerKey()).isEqualTo(1);
        assertThat(sink.attemptCount()).isGreaterThan(JOBS);
        assertThat(alreadyDelivered).isPositive();
        assertThat(sink.attemptCount()).isEqualTo(JOBS + alreadyDelivered + sink.staleRejections());

        // the engine side: exactly one successful commit per run, failures were classified retryable
        assertThat(success).isEqualTo(JOBS);
        assertThat(retryable).isPositive();
        assertThat(permanent).isZero();
        assertThat(elapsedMs).isLessThan(Duration.ofMinutes(2).toMillis());
    }

    private static String tenant(int t) {
        return "eo_t" + t;
    }

    /** One multi-row INSERT of pending one-off jobs with partition_id = murmur3(id) % 128. */
    private static void insertPendingOneOffs(int n) throws Exception {
        UUID[] ids = new UUID[n];
        String[] tenants = new String[n];
        Integer[] partitions = new Integer[n];
        for (int i = 0; i < n; i++) {
            ids[i] = UUID.randomUUID();
            tenants[i] = tenant(i % TENANTS);
            partitions[i] = PartitionHasher.partitionOf(ids[i]);
        }
        try (Connection c = db.admin().getConnection();
             PreparedStatement ps = c.prepareStatement("""
                     INSERT INTO job (id, tenant_id, kind, handler, run_at, payload, priority, max_run_time,
                                      misfire, misfire_threshold, max_attempts, partition_id, version, next_fire_at)
                     SELECT u.id, u.tenant, 'ONE_OFF', 'ROSTER_GEN', t.at, '{}'::jsonb, 5, interval '5 minutes',
                            'FIRE_NOW', interval '60 seconds', ?, u.p, 0, t.at
                       FROM unnest(?::uuid[], ?::text[], ?::int[]) AS u (id, tenant, p),
                            (SELECT date_trunc('milliseconds', clock_timestamp()) AS at) t""")) {
            ps.setInt(1, MAX_ATTEMPTS);
            ps.setArray(2, c.createArrayOf("uuid", ids));
            ps.setArray(3, c.createArrayOf("text", tenants));
            ps.setArray(4, c.createArrayOf("int4", partitions));
            assertThat(ps.executeUpdate()).isEqualTo(n);
        }
    }
}
