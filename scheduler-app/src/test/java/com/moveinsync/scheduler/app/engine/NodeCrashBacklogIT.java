package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.app.engine.support.TestCluster;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.domain.HandlerType;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * The takeover bound holds while the tenant has a backlog (plan section 06.6), REAL timings. Tenant
 * cap 1: a 1,200-alert run holds the only slot and 300 older due runs wait behind it. The node
 * sending the alerts is killed. The pollers of the other nodes keep polling this tenant (it has
 * due work), so a poller usually reaps the expired holder before the orphan sweep sees it. The
 * reaped run must be restarted in that slot at once, not queued behind the 300 older runs: taken
 * over within 11.2 s of its last renewal (DB clock), every employee alerted exactly once.
 */
class NodeCrashBacklogIT {

    private static final String TENANT = "crash-backlog";
    private static final int EMPLOYEES = 1200;
    private static final int BACKLOG = 300;
    private static final Duration RUN_LEASE = Duration.ofSeconds(10);
    private static final Duration TAKEOVER_BOUND = Duration.ofMillis(11_200);

    private static EngineTestDb db;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("node_crash_backlog_it");
    }

    @AfterAll
    static void close() {
        if (db != null) {
            db.close();
        }
    }

    @Test
    void killDuringABacklogIsTakenOverWithin11_2s() throws Exception {
        JdbcClient jdbc = db.jdbc();
        Fixtures fx = new Fixtures(jdbc);
        FakeEffectSink sink = new FakeEffectSink();
        sink.setLatency(Duration.ofMillis(10));
        fx.tenant(TENANT, 1);
        UUID alertJob = fx.job(TENANT).handler(HandlerType.PICKUP_ALERT).payload(payload(EMPLOYEES)).insert();
        List<UUID> backlogJobs = new java.util.ArrayList<>();
        for (int i = 0; i < BACKLOG; i++) {
            backlogJobs.add(fx.job(TENANT).handler(HandlerType.PICKUP_ALERT).payload(payload(1)).insert());
        }

        try (TestCluster cluster = new TestCluster(db, sink, HandlerRegistry.defaults(new ObjectMapper()))) {
            List<SchedulerNode> nodes = List.of(
                    cluster.start(EngineSettings.builder("ncb-n1").build()),
                    cluster.start(EngineSettings.builder("ncb-n2").build()),
                    cluster.start(EngineSettings.builder("ncb-n3").build()));
            long runId = fx.dueRuns(alertJob, 1).get(0);
            await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(50))
                    .until(() -> sink.deliveriesFor(runId).size() >= 100);

            // the backlog: due earlier than the alert run will be after a reap (due_at = now)
            Instant older = ItRunRow.dbNow(jdbc).minusSeconds(30);
            for (int i = 0; i < BACKLOG; i++) {
                fx.run(backlogJobs.get(i), older.plusMillis(i));
            }

            ItRunRow before = ItRunRow.read(jdbc, runId);
            assertThat(before.running()).isTrue();
            SchedulerNode victim = nodes.stream().filter(n -> n.nodeId().equals(before.ownerNode())).findFirst()
                    .orElseThrow();
            FencingToken victimToken = before.token();
            cluster.kill(victim);
            // the victim's last renewal: lease_until after the kill, minus the run lease (DB clock)
            Instant renewDb = ItRunRow.read(jdbc, runId).leaseUntil().minus(RUN_LEASE);

            AtomicReference<ItRunRow> taken = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(40)).pollInterval(Duration.ofMillis(50)).until(() -> {
                ItRunRow r = ItRunRow.read(jdbc, runId);
                if (r.running() && r.token() != null && !r.token().equals(victimToken)) {
                    taken.set(r);
                    return true;
                }
                return false;
            });
            ItRunRow takeover = taken.get();
            Duration sinceRenewal = Duration.between(renewDb, takeover.leaseUntil().minus(RUN_LEASE));
            long backlogDone = fx.count("SELECT count(*) FROM job_run WHERE tenant_id = ? AND state = 'SUCCEEDED' "
                    + "AND id <> ?", TENANT, runId);
            System.out.printf("NodeCrashBacklogIT: victim %s token %s; taken over by %s token %s %d ms after the "
                            + "last renewal (DB clock); backlog runs finished before the takeover: %d of %d%n",
                    victim.nodeId(), victimToken, takeover.ownerNode(), takeover.token(), sinceRenewal.toMillis(),
                    backlogDone, BACKLOG);

            assertThat(takeover.ownerNode()).isNotEqualTo(victim.nodeId());
            assertThat(takeover.token()).isGreaterThan(victimToken);
            assertThat(sinceRenewal).as("takeover time since the last renewal (DB clock)")
                    .isLessThanOrEqualTo(TAKEOVER_BOUND);

            await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(100)).until(() -> fx.count(
                    "SELECT count(*) FROM job_run WHERE tenant_id = ? AND state = 'SUCCEEDED'", TENANT) == BACKLOG + 1);
            assertThat(sink.deliveriesFor(runId)).hasSize(EMPLOYEES);
            assertThat(sink.distinctDeliveries()).isEqualTo(EMPLOYEES + BACKLOG);
            assertThat(sink.maxDeliveriesPerKey()).isEqualTo(1);
        }
    }

    private static String payload(int n) {
        StringBuilder sb = new StringBuilder("{\"employeeIds\":[");
        for (int i = 1; i <= n; i++) {
            if (i > 1) {
                sb.append(',');
            }
            sb.append(String.format("\"emp-%04d\"", i));
        }
        return sb.append("],\"messageTemplate\":\"Your cab is assigned, {employeeId}\"}").toString();
    }
}
