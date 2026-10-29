package com.moveinsync.scheduler.loadgen.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.moveinsync.scheduler.app.engine.EngineSettings;
import com.moveinsync.scheduler.app.engine.SchedulerNode;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.loadgen.support.EmbeddedPg;
import com.moveinsync.scheduler.loadgen.support.SchedulerCluster;
import com.moveinsync.scheduler.loadgen.support.SchedulerCluster.RunRow;
import com.moveinsync.scheduler.loadgen.support.SinkProcess;
import com.moveinsync.scheduler.loadgen.support.SinkProcess.RunStats;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * R11 end to end (plan sections 11, 12, 16): three real scheduler nodes with the production
 * timings (run lease 10 s, renew 3 s, orphan sweep 500 ms) call the <b>real</b> notification sink
 * over HTTP (separate JVM, own database). One pickup-alert run for 1,200 employees; the node
 * sending it is killed ({@code kill -9}: threads stop, connections dropped) in the middle of a
 * chunk. Another node takes over within 11.2 s (DB clock), resumes from the last checkpoint, and
 * the sink's own counters prove every employee got exactly one alert; the re-sent tail of the
 * interrupted chunk came back {@code ALREADY_DELIVERED}.
 */
class EndToEndKillIT {

    private static final int EMPLOYEES = 1200;
    private static final int CHUNK = 50;
    private static final Duration RUN_LEASE = Duration.ofSeconds(10);
    private static final Duration TAKEOVER_BOUND = Duration.ofMillis(11_200);

    private static SinkProcess sink;

    @BeforeAll
    static void startSink() {
        sink = SinkProcess.start(EmbeddedPg.forTests(), "e2e_kill_sink", 20);
        sink.setLatency(10, 0); // ~1,200 x 12 ms: the run lasts long enough to be killed mid-send
    }

    @AfterAll
    static void stopSink() {
        if (sink != null) {
            sink.close();
        }
    }

    @Test
    void killTheSendingNodeMidChunkAndEveryEmployeeGetsExactlyOneAlert() {
        try (SchedulerCluster cluster = new SchedulerCluster(EmbeddedPg.forTests(), "e2e_kill_sched",
                sink.baseUrl())) {
            List<SchedulerNode> nodes = List.of(
                    cluster.start(EngineSettings.builder("e2e-k1").build()),
                    cluster.start(EngineSettings.builder("e2e-k2").build()),
                    cluster.start(EngineSettings.builder("e2e-k3").build()));
            cluster.tenant("e2e-kill", 4);
            UUID job = cluster.pickupAlertJob("e2e-kill", EMPLOYEES);
            long runId = cluster.dueRun(job);

            AtomicReference<RunRow> first = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(50)).until(() -> {
                RunRow r = cluster.run(runId);
                first.set(r);
                return r.running();
            });
            FencingToken victimToken = first.get().token();
            SchedulerNode victim = cluster.node(first.get().ownerNode());

            // mid-send, inside a chunk (not at a checkpoint boundary), so some items get re-sent
            await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(5)).until(() -> {
                long d = sink.runStats(runId).deliveredDistinct();
                return d >= 300 && d % CHUNK >= 5 && d % CHUNK <= 40;
            });
            Instant killDb = cluster.dbNow();
            cluster.kill(victim);
            long killNanos = System.nanoTime();
            long deliveredAtKill = sink.runStats(runId).deliveredDistinct();

            AtomicReference<RunRow> taken = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).until(() -> {
                RunRow r = cluster.run(runId);
                if (r.token() != null && !r.token().equals(victimToken)) {
                    taken.set(r);
                    return true;
                }
                return false;
            });
            RunRow takeover = taken.get();
            long seenMs = (System.nanoTime() - killNanos) / 1_000_000;
            // the claim set lease_until = claim time + 10 s (DB clock); renewals come 3 s later
            Instant claimDb = takeover.running() ? takeover.leaseUntil().minus(RUN_LEASE) : takeover.dbNow();
            Duration takeoverTime = Duration.between(killDb, claimDb);

            await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(100))
                    .until(() -> "SUCCEEDED".equals(cluster.run(runId).state()));
            RunStats stats = sink.runStats(runId);
            SinkProcess.TotalStats totals = sink.totals();

            System.out.printf("EndToEndKillIT: victim %s token %s killed at %d delivered; takeover by %s token %s "
                            + "%d ms after the kill (DB clock), seen after %d ms (wall); sink run stats %s; "
                            + "totals maxDeliveriesPerKey=%d%n",
                    victim.nodeId(), victimToken, deliveredAtKill, takeover.ownerNode(), takeover.token(),
                    takeoverTime.toMillis(), seenMs, stats, totals.maxDeliveriesPerKey());

            assertThat(takeover.ownerNode()).isNotEqualTo(victim.nodeId());
            assertThat(takeover.token()).isGreaterThan(victimToken);
            assertThat(takeoverTime).as("takeover time, DB clock").isLessThanOrEqualTo(TAKEOVER_BOUND);
            assertThat(stats.deliveredDistinct()).isEqualTo(EMPLOYEES);
            assertThat(stats.delivered()).isEqualTo(EMPLOYEES);
            assertThat(totals.maxDeliveriesPerKey()).isEqualTo(1);
            assertThat(stats.alreadyDelivered()).as("re-sent tail of the interrupted chunk").isPositive()
                    .isLessThanOrEqualTo(CHUNK);
            assertThat(stats.staleRejected()).isZero(); // a killed node sends nothing more
            assertThat(nodes.stream().filter(n -> n != victim).mapToDouble(n -> n.metrics().orphanTakeoverCount())
                    .sum()).isEqualTo(1.0);
        }
    }
}
