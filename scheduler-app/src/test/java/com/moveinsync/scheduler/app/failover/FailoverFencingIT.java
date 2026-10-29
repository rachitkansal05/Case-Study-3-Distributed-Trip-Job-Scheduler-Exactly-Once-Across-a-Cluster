package com.moveinsync.scheduler.app.failover;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.SchedulerNode;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.app.engine.support.TestCluster;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * R4 failure injection, plan sections 07 ("Fencing token across failover") and 12 (F1, F3, F5):
 * a Postgres 16 primary with one synchronous standby; two scheduler nodes work through 200 runs;
 * the primary is killed (SIGKILL) mid-flight and the standby is promoted.
 *
 * <ul>
 *   <li>F1/F3: every run that was SUCCEEDED (acknowledged) before the kill is still SUCCEEDED on
 *       the promoted standby, and no acknowledged claim (fencing_seq) went backwards.
 *   <li>F5: the promotion raised the timeline, so the epoch rose; every token used after the
 *       failover beats the highest epoch the sink accepted before it; a token from the old epoch
 *       is rejected by the sink. The new nodes pass the startup epoch guard.
 *   <li>Runs finish: all 200 SUCCEEDED, nothing dead-lettered, one delivery per key.
 * </ul>
 *
 * <p>Node handling: the nodes that ran against the old primary are killed (like their hosts
 * dying with it) and fresh nodes are started after the promotion. Riding through the outage with
 * the same in-memory nodes would also test reconnect/backoff timing, which is not what this test is
 * about; the guarantee under test is the data and token side. The fresh nodes take over the
 * in-flight runs through the normal path (expired run leases, orphan sweep, higher token) and the
 * sink dedups their replayed effects.
 *
 * <p>Docker-gated: skipped when no Docker daemon is available.
 */
@Testcontainers(disabledWithoutDocker = true)
class FailoverFencingIT {

    private static final String TENANT = "failover_t";
    private static final int CAP = 4;
    private static final int RUNS = 200;
    /** Kill the primary once this many runs are acknowledged. */
    private static final int SUCCEEDED_BEFORE_KILL = 30;

    @Test
    void acknowledgedWorkSurvivesFailoverAndNewTokensBeatOldOnes() throws Exception {
        FakeEffectSink sink = new FakeEffectSink();
        // each effect takes ~200 ms, so with cap 4 the 200 runs take ~10 s: the kill lands mid-flight
        sink.setLatency(Duration.ofMillis(200));
        HandlerRegistry handlers = HandlerRegistry.defaults(new ObjectMapper());
        List<HikariDataSource> pools = new ArrayList<>();
        List<SchedulerNode> nodes = new ArrayList<>();

        try (PgReplicationPair pair = PgReplicationPair.start()) {
            assertThat(pair.syncState()).isEqualTo("sync");
            pair.migratePrimary();
            String failoverUrl = pair.failoverJdbcUrl(); // read the mapped ports while both run

            List<Long> runIds = new ArrayList<>();
            try (HikariDataSource setup = pair.pool("setup", pair.primaryJdbcUrl(), 2)) {
                Fixtures fx = new Fixtures(JdbcClient.create(setup));
                fx.tenant(TENANT, CAP);
                for (int i = 0; i < RUNS; i++) {
                    UUID job = fx.job(TENANT).handler(HandlerType.ROSTER_GEN)
                            .payload("{\"shiftDate\":\"2026-11-01\",\"siteId\":\"s" + i + "\"}")
                            .maxAttempts(10)
                            .insert();
                    runIds.addAll(fx.dueRuns(job, 1));
                }
            }
            assertThat(runIds).hasSize(RUNS);

            long oldTimeline;
            long oldEpoch;
            try (Connection c = pair.primaryConnection()) {
                oldTimeline = PgReplicationPair.timeline(c);
                oldEpoch = PgReplicationPair.epoch(c);
            }

            // ---- phase 1: two nodes against the primary ------------------------------------------
            for (String id : List.of("fo1", "fo2")) {
                nodes.add(startNode(pair, id, failoverUrl, sink, handlers, pools));
            }
            await("some runs acknowledged").atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100))
                    .until(() -> countOnPrimary(pair, "SELECT count(*) FROM job_run WHERE state = 'SUCCEEDED'")
                            >= SUCCEEDED_BEFORE_KILL);

            // snapshot, then a synchronous barrier commit: afterwards everything in the snapshot is
            // acknowledged (on the standby), not merely visible on the primary
            Set<Long> succeededBefore = new HashSet<>();
            Map<Long, Long> seqBefore = new HashMap<>();
            try (Connection c = pair.primaryConnection(); Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id, state, fencing_seq FROM job_run")) {
                while (rs.next()) {
                    long id = rs.getLong(1);
                    if ("SUCCEEDED".equals(rs.getString(2))) {
                        succeededBefore.add(id);
                    }
                    long seq = rs.getLong(3);
                    if (!rs.wasNull()) {
                        seqBefore.put(id, seq);
                    }
                }
            }
            pair.syncBarrier();
            long sinkHighestBefore = sink.highestAcceptedEpoch();
            assertThat(succeededBefore).hasSizeGreaterThanOrEqualTo(SUCCEEDED_BEFORE_KILL).hasSizeLessThan(RUNS);
            assertThat(sinkHighestBefore).isEqualTo(oldEpoch);

            // ---- the failure -----------------------------------------------------------------------
            pair.killPrimary();
            Thread.sleep(1_000); // let in-flight statements of the old nodes fail against the dead primary
            for (SchedulerNode n : nodes) {
                n.kill();
            }
            nodes.clear();
            pools.forEach(HikariDataSource::close);
            pools.clear();

            pair.promoteStandby();

            // ---- checks on the promoted standby, before any new node runs --------------------------
            Set<Long> unfinishedAtCut = new HashSet<>();
            long newEpoch;
            try (Connection c = pair.standbyConnection()) {
                long newTimeline = PgReplicationPair.timeline(c);
                newEpoch = PgReplicationPair.epoch(c);
                assertThat(newTimeline).as("promotion starts a new timeline").isGreaterThan(oldTimeline);
                assertThat(newEpoch).as("F5: epoch strictly increases").isGreaterThan(oldEpoch)
                        .isGreaterThan(sinkHighestBefore);

                Map<Long, String> stateAfter = new HashMap<>();
                Map<Long, Long> seqAfter = new HashMap<>();
                try (Statement st = c.createStatement();
                     ResultSet rs = st.executeQuery("SELECT id, state, fencing_seq FROM job_run")) {
                    while (rs.next()) {
                        long id = rs.getLong(1);
                        stateAfter.put(id, rs.getString(2));
                        long seq = rs.getLong(3);
                        if (!rs.wasNull()) {
                            seqAfter.put(id, seq);
                        }
                    }
                }
                assertThat(stateAfter.keySet()).containsExactlyInAnyOrderElementsOf(runIds);
                for (long id : succeededBefore) {
                    assertThat(stateAfter.get(id)).as("F1/F3: acknowledged success of run %d survived", id)
                            .isEqualTo("SUCCEEDED");
                }
                for (Map.Entry<Long, Long> e : seqBefore.entrySet()) {
                    assertThat(seqAfter.get(e.getKey())).as("acknowledged claim of run %d survived", e.getKey())
                            .isNotNull().isGreaterThanOrEqualTo(e.getValue());
                }
                stateAfter.forEach((id, state) -> {
                    if (!"SUCCEEDED".equals(state)) {
                        unfinishedAtCut.add(id);
                    }
                });
            }
            assertThat(unfinishedAtCut).as("the kill landed mid-flight").isNotEmpty();

            // ---- phase 2: fresh nodes; the multi-host URL now resolves to the promoted standby -----
            // start() runs the epoch guard (F5): new epoch >= sink's highest, so it must pass
            for (String id : List.of("fo3", "fo4")) {
                nodes.add(startNode(pair, id, failoverUrl, sink, handlers, pools));
            }
            await("all runs finished after failover").atMost(Duration.ofSeconds(120))
                    .pollInterval(Duration.ofMillis(250))
                    .until(() -> countOnStandby(pair,
                            "SELECT count(*) FROM job_run WHERE state = 'SUCCEEDED'") == RUNS);
            for (SchedulerNode n : nodes) {
                n.stop(Duration.ofSeconds(5));
            }
            nodes.clear();

            // ---- final state ---------------------------------------------------------------------
            assertThat(countOnStandby(pair, "SELECT count(*) FROM job_run WHERE state = 'DEAD_LETTERED'")).isZero();
            assertThat(countOnStandby(pair, "SELECT count(*) FROM dead_letter")).isZero();

            Map<Long, Long> finalEpoch = new HashMap<>();
            Map<Long, String> idemKeys = new HashMap<>();
            try (Connection c = pair.standbyConnection(); Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT id, fencing_epoch, idem_key FROM job_run")) {
                while (rs.next()) {
                    finalEpoch.put(rs.getLong(1), rs.getLong(2));
                    idemKeys.put(rs.getLong(1), rs.getString(3));
                }
            }
            for (long id : unfinishedAtCut) {
                assertThat(finalEpoch.get(id)).as("run %d finished under a post-failover token", id)
                        .isEqualTo(newEpoch).isGreaterThan(sinkHighestBefore);
                FencingToken fence = sink.fenceOf(id);
                assertThat(fence).as("sink fence of run %d", id).isNotNull();
                assertThat(fence.epoch()).as("sink fence of run %d beats the pre-failover highest", id)
                        .isGreaterThan(sinkHighestBefore);
            }
            assertThat(sink.highestAcceptedEpoch()).isEqualTo(newEpoch);
            assertThat(sink.distinctDeliveries()).isEqualTo(RUNS);
            assertThat(sink.maxDeliveriesPerKey()).isEqualTo(1);

            // "old-primary clients rejected": the old primary is gone, so a client still holding an
            // old-epoch token (even with a huge sequence) is rejected by the sink's fence
            long probe = unfinishedAtCut.iterator().next();
            DeliveryOutcome stale = sink.send(probe, idemKeys.get(probe),
                    new FencingToken(sinkHighestBefore, Long.MAX_VALUE), "{\"event\":\"ROSTER_READY\"}");
            assertThat(stale).isEqualTo(DeliveryOutcome.STALE_TOKEN);
        } finally {
            for (SchedulerNode n : nodes) {
                n.kill();
            }
            pools.forEach(HikariDataSource::close);
        }
    }

    private static SchedulerNode startNode(PgReplicationPair pair, String id, String url, FakeEffectSink sink,
                                           HandlerRegistry handlers, List<HikariDataSource> pools) {
        HikariDataSource pool = pair.pool("pool-" + id, url, 24);
        pools.add(pool);
        SchedulerNode node = new SchedulerNode(TestCluster.fast(id).build(), pool, sink, handlers,
                new SimpleMeterRegistry());
        node.start();
        return node;
    }

    private static long countOnPrimary(PgReplicationPair pair, String sql) throws Exception {
        try (Connection c = pair.primaryConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static long countOnStandby(PgReplicationPair pair, String sql) throws Exception {
        try (Connection c = pair.standbyConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
