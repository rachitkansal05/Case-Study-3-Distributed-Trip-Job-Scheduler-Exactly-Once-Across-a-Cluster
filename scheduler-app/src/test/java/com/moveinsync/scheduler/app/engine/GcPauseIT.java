package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink.Attempt;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.app.engine.support.TestCluster;
import com.moveinsync.scheduler.app.handler.BillingCloseHandler;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.app.handler.PickupAlertHandler;
import com.moveinsync.scheduler.app.handler.RosterGenerationHandler;
import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.LeaseLostReason;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * R12 (plan sections 07, 11 and 16), short timings ({@link TestCluster#fast}, run lease 3 s): a
 * stop-the-world pause ({@link SchedulerNode#freeze()}) on the node executing a run, kept past its
 * lease. The other node takes the run over and finishes it; when the frozen node resumes, its
 * stale effect is rejected by the sink (409) and its stale commit by the database (0 rows).
 */
class GcPauseIT {

    private static final int ITEMS = 300;
    private static final int CHUNK = 50;

    private static EngineTestDb db;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("gc_pause_it");
    }

    @AfterAll
    static void close() {
        if (db != null) {
            db.close();
        }
    }

    /** Freeze mid-send: the send in flight at the freeze is rejected by the sink fence after the thaw. */
    @Test
    void frozenNodeMidSendIsFencedBySinkAndDatabase() throws Exception {
        JdbcClient jdbc = db.jdbc();
        Fixtures fx = new Fixtures(jdbc);
        FakeEffectSink sink = new FakeEffectSink();
        sink.setLatency(Duration.ofMillis(10));
        fx.tenant("gc-send", 2);
        UUID job = fx.job("gc-send").handler(HandlerType.PICKUP_ALERT).payload(payload(ITEMS)).insert();

        try (TestCluster cluster = new TestCluster(db, sink, HandlerRegistry.defaults(new ObjectMapper()))) {
            List<SchedulerNode> nodes = List.of(
                    cluster.start(TestCluster.fast("gc-a1").build()),
                    cluster.start(TestCluster.fast("gc-b1").build()));
            long runId = fx.dueRuns(job, 1).get(0);
            ItRunRow claimed = awaitRunning(jdbc, runId);
            SchedulerNode a = node(nodes, claimed.ownerNode());
            SchedulerNode b = nodes.stream().filter(n -> n != a).findFirst().orElseThrow();
            FencingToken aToken = claimed.token();

            // mid-send, away from a chunk boundary (so the thread is not about to checkpoint)
            await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(2)).until(() -> {
                int d = sink.deliveriesFor(runId).size();
                return d >= 60 && d % CHUNK >= 5 && d % CHUNK <= 40;
            });
            long freezeNanos = System.nanoTime();
            a.freeze();
            int deliveredAtFreeze = sink.deliveriesFor(runId).size();

            // B takes over with a higher token and finishes while A is still frozen
            AtomicReference<ItRunRow> done = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(50)).until(() -> {
                ItRunRow r = ItRunRow.read(jdbc, runId);
                done.set(r);
                return "SUCCEEDED".equals(r.state());
            });
            ItRunRow finished = done.get();
            assertThat(a.metrics().outcomeCount("success")).isZero();
            assertThat(finished.token()).isGreaterThan(aToken);
            assertThat(finished.ownerNode()).isEqualTo(b.nodeId());
            long frozenMs = (System.nanoTime() - freezeNanos) / 1_000_000;

            a.unfreeze();
            await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20))
                    .until(() -> leaseLost(a) >= 1 && a.activeRuns() == 0);

            List<Attempt> aStale = sink.attempts().stream()
                    .filter(x -> x.runId() == runId && x.token().equals(aToken)
                            && x.outcome() == DeliveryOutcome.STALE_TOKEN)
                    .toList();
            System.out.printf("GcPauseIT mid-send: A=%s token %s frozen %d ms at %d delivered; B=%s token %s "
                            + "finished; sink staleRejections=%d (A's 409s=%d); A leaseLost stale_409=%.0f "
                            + "fenced_write_0_rows=%.0f self_fence=%.0f renewFailures=%.0f staleCommits=%.0f%n",
                    a.nodeId(), aToken, frozenMs, deliveredAtFreeze, b.nodeId(), finished.token(),
                    sink.staleRejections(), aStale.size(), a.metrics().leaseLostCount(LeaseLostReason.STALE_TOKEN),
                    a.metrics().leaseLostCount(LeaseLostReason.FENCED_WRITE_REJECTED),
                    a.metrics().leaseLostCount(LeaseLostReason.SELF_FENCE), a.metrics().renewFailureCount(),
                    a.metrics().staleCommitRejectedCount());

            assertThat(sink.staleRejections()).isPositive();
            assertThat(aStale).isNotEmpty();
            assertThat(a.metrics().leaseLostCount(LeaseLostReason.STALE_TOKEN)).isGreaterThanOrEqualTo(1.0);
            assertThat(a.metrics().outcomeCount("success")).isZero();
            assertThat(b.metrics().outcomeCount("success")).isEqualTo(1.0);
            assertThat(sink.deliveriesFor(runId)).hasSize(ITEMS);
            assertThat(sink.distinctDeliveries()).isEqualTo(ITEMS);
            assertThat(sink.maxDeliveriesPerKey()).isEqualTo(1);
            assertThat(fx.state(runId)).isEqualTo("SUCCEEDED");
        }
    }

    /**
     * Freeze after the last effect and before the commit: B finishes; A's commit after the thaw
     * updates 0 rows and counts {@code scheduler_stale_commit_rejected_total}.
     */
    @Test
    void frozenNodeBeforeCommitHasItsCommitRejected() throws Exception {
        JdbcClient jdbc = db.jdbc();
        Fixtures fx = new Fixtures(jdbc);
        FakeEffectSink sink = new FakeEffectSink();
        ObjectMapper mapper = new ObjectMapper();
        ItBlockingEffectHandler blocking = new ItBlockingEffectHandler(mapper, HandlerType.NO_SHOW_CANCEL);
        HandlerRegistry handlers = new HandlerRegistry(List.of(new PickupAlertHandler(mapper),
                new RosterGenerationHandler(mapper), new BillingCloseHandler(mapper), blocking));
        fx.tenant("gc-commit", 2);
        UUID job = fx.job("gc-commit").handler(HandlerType.NO_SHOW_CANCEL).insert();

        try (TestCluster cluster = new TestCluster(db, sink, handlers)) {
            List<SchedulerNode> nodes = List.of(
                    cluster.start(TestCluster.fast("gc-a2").build()),
                    cluster.start(TestCluster.fast("gc-b2").build()));
            long runId = fx.dueRuns(job, 1).get(0);

            assertThat(blocking.effectSent.await(20, TimeUnit.SECONDS)).as("first attempt sent its effect").isTrue();
            ItRunRow claimed = ItRunRow.read(jdbc, runId);
            assertThat(claimed.running()).isTrue();
            SchedulerNode a = node(nodes, claimed.ownerNode());
            SchedulerNode b = nodes.stream().filter(n -> n != a).findFirst().orElseThrow();
            FencingToken aToken = claimed.token();

            a.freeze();
            blocking.release(); // the worker leaves the handler and stops at the gate before the commit
            long freezeNanos = System.nanoTime();

            AtomicReference<ItRunRow> done = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(50)).until(() -> {
                ItRunRow r = ItRunRow.read(jdbc, runId);
                done.set(r);
                return "SUCCEEDED".equals(r.state());
            });
            ItRunRow finished = done.get();
            assertThat(finished.ownerNode()).isEqualTo(b.nodeId());
            assertThat(finished.token()).isGreaterThan(aToken);
            long frozenMs = (System.nanoTime() - freezeNanos) / 1_000_000;

            a.unfreeze();
            await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20))
                    .until(() -> a.metrics().staleCommitRejectedCount() >= 1.0 && a.activeRuns() == 0);

            System.out.printf("GcPauseIT before-commit: A=%s token %s frozen %d ms; B=%s token %s committed; "
                            + "A staleCommits=%.0f fenced_write_0_rows=%.0f renewFailures=%.0f; sink attempts=%s%n",
                    a.nodeId(), aToken, frozenMs, b.nodeId(), finished.token(),
                    a.metrics().staleCommitRejectedCount(),
                    a.metrics().leaseLostCount(LeaseLostReason.FENCED_WRITE_REJECTED),
                    a.metrics().renewFailureCount(),
                    sink.attempts().stream().map(x -> x.token() + ":" + x.outcome()).toList());

            assertThat(a.metrics().staleCommitRejectedCount()).isEqualTo(1.0);
            assertThat(a.metrics().leaseLostCount(LeaseLostReason.FENCED_WRITE_REJECTED)).isGreaterThanOrEqualTo(1.0);
            assertThat(a.metrics().outcomeCount("success")).isZero();
            assertThat(b.metrics().outcomeCount("success")).isEqualTo(1.0);
            ItRunRow after = ItRunRow.read(jdbc, runId);
            assertThat(after.state()).isEqualTo("SUCCEEDED");
            assertThat(after.token()).isEqualTo(finished.token()); // A's stale commit changed nothing
            assertThat(fx.count("SELECT count(*) FROM job_run WHERE job_id = ? AND state = 'SUCCEEDED'", job))
                    .isEqualTo(1);
            assertThat(sink.distinctDeliveries()).isEqualTo(1);
            assertThat(sink.maxDeliveriesPerKey()).isEqualTo(1);
            assertThat(sink.attempts()).anyMatch(x -> x.token().equals(finished.token())
                    && x.outcome() == DeliveryOutcome.ALREADY_DELIVERED);
        }
    }

    private static double leaseLost(SchedulerNode n) {
        double sum = 0;
        for (LeaseLostReason r : LeaseLostReason.values()) {
            sum += n.metrics().leaseLostCount(r);
        }
        return sum;
    }

    private static ItRunRow awaitRunning(JdbcClient jdbc, long runId) {
        AtomicReference<ItRunRow> row = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(20)).until(() -> {
            ItRunRow r = ItRunRow.read(jdbc, runId);
            row.set(r);
            return r.running();
        });
        return row.get();
    }

    private static SchedulerNode node(List<SchedulerNode> nodes, String id) {
        return nodes.stream().filter(n -> n.nodeId().equals(id)).findFirst()
                .orElseThrow(() -> new AssertionError("no node " + id));
    }

    private static String payload(int n) {
        StringBuilder sb = new StringBuilder("{\"employeeIds\":[");
        for (int i = 1; i <= n; i++) {
            if (i > 1) {
                sb.append(',');
            }
            sb.append(String.format("\"emp-%04d\"", i));
        }
        return sb.append("],\"messageTemplate\":\"Cab assigned for {employeeId}\"}").toString();
    }
}
