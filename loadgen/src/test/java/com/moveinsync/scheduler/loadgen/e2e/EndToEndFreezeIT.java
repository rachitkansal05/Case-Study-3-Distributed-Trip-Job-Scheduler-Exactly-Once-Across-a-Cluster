package com.moveinsync.scheduler.loadgen.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.moveinsync.scheduler.app.engine.EngineSettings;
import com.moveinsync.scheduler.app.engine.SchedulerNode;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.domain.LeaseLostReason;
import com.moveinsync.scheduler.loadgen.support.EmbeddedPg;
import com.moveinsync.scheduler.loadgen.support.SchedulerCluster;
import com.moveinsync.scheduler.loadgen.support.SchedulerCluster.RunRow;
import com.moveinsync.scheduler.loadgen.support.SinkProcess;
import com.moveinsync.scheduler.loadgen.support.SinkProcess.RunStats;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * R12 end to end (plan section 11, the "hardest flow"), production timings, the <b>real</b> sink
 * over HTTP: the node sending 1,200 pickup alerts is frozen ({@link SchedulerNode#freeze()}, a
 * stop-the-world pause like SIGSTOP or a long GC) for 30 s mid-send. Its lease expires, another node
 * takes the run over with a higher token and finishes it while the first is still frozen. On thaw
 * the frozen node's in-flight send (it was parked between its self-fence check and the HTTP call)
 * reaches the real sink with the old token and gets {@code 409 STALE_TOKEN}; its fenced commit
 * with that token returns {@code false}. 1,200 delivered, none twice.
 */
class EndToEndFreezeIT {

    private static final int EMPLOYEES = 1200;
    private static final int CHUNK = 50;
    private static final Duration FREEZE = Duration.ofSeconds(30);

    private static SinkProcess sink;

    @BeforeAll
    static void startSink() {
        sink = SinkProcess.start(EmbeddedPg.forTests(), "e2e_freeze_sink", 20);
        sink.setLatency(5, 0);
    }

    @AfterAll
    static void stopSink() {
        if (sink != null) {
            sink.close();
        }
    }

    @Test
    void frozenNodeIsFencedByTheRealSinkAndTheDatabase() throws Exception {
        try (SchedulerCluster cluster = new SchedulerCluster(EmbeddedPg.forTests(), "e2e_freeze_sched",
                sink.baseUrl())) {
            List<SchedulerNode> nodes = List.of(
                    cluster.start(EngineSettings.builder("e2e-f1").build()),
                    cluster.start(EngineSettings.builder("e2e-f2").build()),
                    cluster.start(EngineSettings.builder("e2e-f3").build()));
            cluster.tenant("e2e-freeze", 4);
            UUID job = cluster.pickupAlertJob("e2e-freeze", EMPLOYEES);
            long runId = cluster.dueRun(job);

            AtomicReference<RunRow> first = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(50)).until(() -> {
                RunRow r = cluster.run(runId);
                first.set(r);
                return r.running();
            });
            FencingToken staleToken = first.get().token();
            SchedulerNode frozen = cluster.node(first.get().ownerNode());

            await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(5)).until(() -> {
                long d = sink.runStats(runId).deliveredDistinct();
                return d >= 200 && d % CHUNK >= 5 && d % CHUNK <= 40;
            });
            frozen.freeze();
            long freezeNanos = System.nanoTime();
            long deliveredAtFreeze = sink.runStats(runId).deliveredDistinct();

            // another node takes over after the 10 s lease and finishes while the owner is frozen
            AtomicReference<RunRow> done = new AtomicReference<>();
            await().atMost(FREEZE.minusSeconds(1)).pollInterval(Duration.ofMillis(100)).until(() -> {
                RunRow r = cluster.run(runId);
                done.set(r);
                return "SUCCEEDED".equals(r.state());
            });
            RunRow finished = done.get();
            assertThat(finished.ownerNode()).isNotEqualTo(frozen.nodeId());
            assertThat(finished.token()).isGreaterThan(staleToken);
            long finishedAfterMs = (System.nanoTime() - freezeNanos) / 1_000_000;
            RunStats beforeThaw = sink.runStats(runId);

            long remaining = FREEZE.toNanos() - (System.nanoTime() - freezeNanos);
            if (remaining > 0) {
                Thread.sleep(remaining / 1_000_000);
            }
            frozen.unfreeze();
            await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(20))
                    .until(() -> frozen.activeRuns() == 0 && leaseLost(frozen) >= 1);

            // the stale node's commit, with its token, after the thaw
            boolean staleCommit = frozen.claimProtocol().commitSuccess(runId, staleToken);
            RunStats stats = sink.runStats(runId);
            SinkProcess.TotalStats totals = sink.totals();
            RunRow after = cluster.run(runId);

            System.out.printf("EndToEndFreezeIT: %s token %s frozen %d s at %d delivered; %s token %s finished "
                            + "%d ms after the freeze (staleRejected before thaw=%d); after thaw: sink %s, "
                            + "lease lost stale_409=%.0f fenced_write=%.0f self_fence=%.0f, stale commit returned %s, "
                            + "maxDeliveriesPerKey=%d%n",
                    frozen.nodeId(), staleToken, FREEZE.toSeconds(), deliveredAtFreeze, finished.ownerNode(),
                    finished.token(), finishedAfterMs, beforeThaw.staleRejected(), stats,
                    frozen.metrics().leaseLostCount(LeaseLostReason.STALE_TOKEN),
                    frozen.metrics().leaseLostCount(LeaseLostReason.FENCED_WRITE_REJECTED),
                    frozen.metrics().leaseLostCount(LeaseLostReason.SELF_FENCE), staleCommit,
                    totals.maxDeliveriesPerKey());

            assertThat(beforeThaw.staleRejected()).isZero();
            assertThat(stats.staleRejected()).as("the frozen node's in-flight send got 409 from the sink")
                    .isPositive();
            assertThat(frozen.metrics().leaseLostCount(LeaseLostReason.STALE_TOKEN)).isGreaterThanOrEqualTo(1.0);
            assertThat(frozen.metrics().outcomeCount("success")).isZero();
            assertThat(staleCommit).as("fenced commit with the stale token").isFalse();
            assertThat(after.state()).isEqualTo("SUCCEEDED");
            assertThat(after.token()).isEqualTo(finished.token());
            assertThat(stats.deliveredDistinct()).isEqualTo(EMPLOYEES);
            assertThat(stats.delivered()).isEqualTo(EMPLOYEES);
            assertThat(totals.maxDeliveriesPerKey()).isEqualTo(1);
            assertThat(nodes.stream().filter(n -> n != frozen).mapToDouble(n -> n.metrics().outcomeCount("success"))
                    .sum()).isEqualTo(1.0);
        }
    }

    private static double leaseLost(SchedulerNode n) {
        double sum = 0;
        for (LeaseLostReason r : LeaseLostReason.values()) {
            sum += n.metrics().leaseLostCount(r);
        }
        return sum;
    }
}
