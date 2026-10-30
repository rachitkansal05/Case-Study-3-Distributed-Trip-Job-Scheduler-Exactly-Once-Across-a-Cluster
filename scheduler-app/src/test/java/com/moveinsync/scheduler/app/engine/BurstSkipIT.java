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
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A burst queued behind its own tenant's cap is late, not missed (plan section 09, V204). One
 * tenant, cap 4, 400 one-off {@code SKIP} jobs with a 1 s misfire threshold, all due at the same
 * instant, a 50 ms handler: draining takes about 400 × 50 ms / 4 = 5 s, so most runs start more
 * than the threshold after their scheduled time. The cluster was up the whole time, so none may be
 * {@code SKIPPED}: every run executes once.
 */
class BurstSkipIT {

    private static final String TENANT = "burst-skip";
    private static final int CAP = 4;
    private static final int RUNS = 400;
    private static final long HANDLER_MS = 50;
    private static final Duration THRESHOLD = Duration.ofSeconds(1);
    private static final Duration LEAD = Duration.ofSeconds(2);

    private static EngineTestDb db;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("burst_skip_it");
    }

    @AfterAll
    static void close() {
        if (db != null) {
            db.close();
        }
    }

    @Test
    void queuedBehindTheCapIsNotAMisfire() throws Exception {
        Fixtures fx = new Fixtures(db.jdbc());
        fx.tenant(TENANT, CAP);

        ObjectMapper mapper = new ObjectMapper();
        FakeEffectSink sink = new FakeEffectSink();
        ItScriptedHandler alert = new ItScriptedHandler(HandlerType.PICKUP_ALERT, (ctx, execution) -> {
            Thread.sleep(HANDLER_MS);
            ctx.send(ctx.run().idemKey(), "{\"event\":\"CAB_ASSIGNED\"}");
        });
        HandlerRegistry registry = new HandlerRegistry(List.of(alert, new RosterGenerationHandler(mapper),
                new NoShowCancelHandler(mapper), new BillingCloseHandler(mapper)));

        try (TestCluster cluster = new TestCluster(db, sink, registry)) {
            for (String id : List.of("bs1", "bs2", "bs3")) {
                cluster.start(EngineSettings.builder(id).build());
            }
            // let the nodes heartbeat for longer than the threshold before the runs fall due
            await().atMost(Duration.ofSeconds(30)).until(() -> fx.count(
                    "SELECT count(*) FROM scheduler_node WHERE alive_since < clock_timestamp() - interval '2 seconds'") == 3);
            insertJobsAndRuns();

            await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100)).until(() -> fx.count(
                    "SELECT count(*) FROM job_run WHERE tenant_id = ? AND state IN ('SUCCEEDED', 'SKIPPED', 'DEAD_LETTERED')",
                    TENANT) == RUNS);

            long late = fx.count("SELECT count(*) FROM job_run WHERE tenant_id = ? AND state = 'SUCCEEDED' "
                    + "AND started_at - scheduled_at > interval '1 second'", TENANT);
            System.out.printf("BurstSkipIT: %d runs, cap %d: %d started more than %s late%n",
                    RUNS, CAP, late, THRESHOLD);
            assertThat(late).as("the burst really waited past the threshold").isGreaterThan(RUNS / 2);
            assertThat(fx.count("SELECT count(*) FROM job_run WHERE tenant_id = ? AND state = 'SKIPPED'", TENANT))
                    .as("queued runs skipped as misfires").isZero();
            assertThat(sink.distinctDeliveries()).isEqualTo(RUNS);
            assertThat(sink.maxDeliveriesPerKey()).isEqualTo(1);
        }
    }

    /** RUNS one-off SKIP jobs, one run each, all due at now + LEAD. */
    private static void insertJobsAndRuns() throws Exception {
        UUID[] ids = new UUID[RUNS];
        Integer[] partitions = new Integer[RUNS];
        for (int i = 0; i < RUNS; i++) {
            ids[i] = UUID.randomUUID();
            partitions[i] = PartitionHasher.partitionOf(ids[i]);
        }
        try (Connection c = db.admin().getConnection()) {
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO job (id, tenant_id, kind, handler, run_at, payload, priority, max_run_time,
                                     misfire, misfire_threshold, max_attempts, partition_id, version)
                    SELECT u.id, ?, 'ONE_OFF', 'PICKUP_ALERT', clock_timestamp(), '{}'::jsonb, 5,
                           interval '5 minutes', 'SKIP', ? * interval '1 millisecond', 5, u.p, 0
                      FROM unnest(?::uuid[], ?::int[]) AS u (id, p)""")) {
                ps.setString(1, TENANT);
                ps.setLong(2, THRESHOLD.toMillis());
                ps.setArray(3, c.createArrayOf("uuid", ids));
                ps.setArray(4, c.createArrayOf("int4", partitions));
                assertThat(ps.executeUpdate()).isEqualTo(RUNS);
            }
            try (PreparedStatement ps = c.prepareStatement("""
                    WITH t AS (SELECT date_trunc('milliseconds', clock_timestamp()) + ? * interval '1 millisecond' AS at)
                    INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                         scheduled_at, due_at, idem_key, state)
                    SELECT j.id, j.tenant_id, j.partition_id, j.priority, j.version, j.max_run_time,
                           t.at, t.at, j.id || ':0', 'SCHEDULED'
                      FROM job j CROSS JOIN t
                     WHERE j.tenant_id = ?""")) {
                ps.setLong(1, LEAD.toMillis());
                ps.setString(2, TENANT);
                assertThat(ps.executeUpdate()).isEqualTo(RUNS);
            }
        }
    }
}
