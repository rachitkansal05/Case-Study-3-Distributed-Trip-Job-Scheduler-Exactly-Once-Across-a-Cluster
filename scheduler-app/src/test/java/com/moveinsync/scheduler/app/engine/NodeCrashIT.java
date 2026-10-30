package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink.Attempt;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.app.engine.support.TestCluster;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.domain.HandlerType;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * R11 (plan sections 06.6, 11 and 16), REAL timings (run lease 10 s, renew 3 s, sweep 500 ms): kill
 * -9 the node sending 1,200 pickup alerts mid-send, right after its last successful run-lease
 * renewal (worst case). Another node takes the run over within 11.2 s (DB clock), resumes from
 * the last checkpoint, and every employee gets exactly one alert. The survivor's {@code
 * scheduler_orphan_takeover_time_seconds} records the same interval the test measures (last
 * renewal to takeover, DB clock), between one run lease and the 11.2 s alert threshold.
 */
class NodeCrashIT {

    private static final int EMPLOYEES = 1200;
    private static final int CHUNK = 50;
    private static final Duration RUN_LEASE = Duration.ofSeconds(10);
    private static final Duration TAKEOVER_BOUND = Duration.ofMillis(11_200);

    private static EngineTestDb db;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("node_crash_it");
    }

    @AfterAll
    static void close() {
        if (db != null) {
            db.close();
        }
    }

    @Test
    void killMidSendRightAfterLeaseRenewalIsTakenOverWithin11_2sWithoutDuplicates() throws Exception {
        JdbcClient jdbc = db.jdbc();
        Fixtures fx = new Fixtures(jdbc);
        FakeEffectSink sink = new FakeEffectSink();
        sink.setLatency(Duration.ofMillis(10));
        fx.tenant("crash", 4);
        UUID job = fx.job("crash").handler(HandlerType.PICKUP_ALERT).payload(payload(EMPLOYEES)).insert();

        try (TestCluster cluster = new TestCluster(db, sink, HandlerRegistry.defaults(new ObjectMapper()))) {
            List<SchedulerNode> nodes = List.of(
                    cluster.start(EngineSettings.builder("nc-n1").build()),
                    cluster.start(EngineSettings.builder("nc-n2").build()),
                    cluster.start(EngineSettings.builder("nc-n3").build()));
            long runId = fx.dueRuns(job, 1).get(0);
            long startNanos = System.nanoTime();

            AtomicReference<ItRunRow> first = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(50)).until(() -> {
                ItRunRow r = ItRunRow.read(jdbc, runId);
                first.set(r);
                return r.running();
            });
            ItRunRow claimed = first.get();
            SchedulerNode victim = nodes.stream().filter(n -> n.nodeId().equals(claimed.ownerNode())).findFirst()
                    .orElseThrow();
            FencingToken victimToken = claimed.token();

            // mid-send: some hundreds delivered
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20))
                    .until(() -> sink.deliveriesFor(runId).size() >= 250);

            // worst case: right after a successful lease renewal, and not on a chunk boundary
            Instant lastLease = ItRunRow.read(jdbc, runId).leaseUntil();
            Instant renewedLease = null;
            long watchEnd = System.nanoTime() + Duration.ofSeconds(8).toNanos();
            while (System.nanoTime() < watchEnd) {
                Instant now = ItRunRow.read(jdbc, runId).leaseUntil();
                if (now != null && now.isAfter(lastLease)) {
                    renewedLease = now;
                    break;
                }
                Thread.sleep(5);
            }
            assertThat(renewedLease).as("the victim renewed its run lease").isNotNull();
            long renewSeenNanos = System.nanoTime();
            long spinEnd = renewSeenNanos + Duration.ofMillis(300).toNanos();
            while (System.nanoTime() < spinEnd) {
                int mod = sink.deliveriesFor(runId).size() % CHUNK;
                if (mod >= 3 && mod <= 46) {
                    break;
                }
                Thread.sleep(2);
            }
            Instant killDb = ItRunRow.dbNow(jdbc);
            cluster.kill(victim);
            long killNanos = System.nanoTime();
            int deliveredAtKill = sink.deliveriesFor(runId).size();
            Instant renewDb = renewedLease.minus(RUN_LEASE);

            // takeover: RUNNING again with a new token on another node
            AtomicReference<ItRunRow> taken = new AtomicReference<>();
            await().atMost(Duration.ofSeconds(25)).pollInterval(Duration.ofMillis(50)).until(() -> {
                ItRunRow r = ItRunRow.read(jdbc, runId);
                if (r.token() != null && !r.token().equals(victimToken)) {
                    taken.set(r);
                    return true;
                }
                return false;
            });
            ItRunRow takeover = taken.get();
            long takeoverSeenNanos = System.nanoTime();
            assertThat(takeover.ownerNode()).isNotEqualTo(victim.nodeId());
            assertThat(takeover.token()).isGreaterThan(victimToken);
            // the claim set lease_until = claim time + 10 s on the DB clock; the first renewal is 3 s later
            Instant takeoverDb = takeover.running() && takeover.leaseUntil() != null
                    ? takeover.leaseUntil().minus(RUN_LEASE) : takeover.dbNow();
            Duration takeoverTime = Duration.between(killDb, takeoverDb);
            Duration sinceRenewal = Duration.between(renewDb, takeoverDb);

            await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100))
                    .until(() -> "SUCCEEDED".equals(fx.state(runId)));
            Duration total = Duration.ofNanos(System.nanoTime() - startNanos);

            FencingToken newToken = takeover.token();
            List<Attempt> byNew = new ArrayList<>();
            for (Attempt a : sink.attempts()) {
                if (a.runId() == runId && a.token().equals(newToken)) {
                    byNew.add(a);
                }
            }
            long resent = byNew.stream().filter(a -> a.outcome() == DeliveryOutcome.ALREADY_DELIVERED).count();
            int resumeFrom = indexOf(byNew.get(0).idemKey());

            System.out.printf("NodeCrashIT: victim %s token %s killed %d ms after its renewal was seen "
                            + "(%d delivered); takeover by %s token %s: %d ms after kill (DB), %d ms after the last "
                            + "renewal (DB), seen %d ms after kill (wall); resumed at item %d, re-sent %d (deduplicated); "
                            + "total %d ms%n",
                    victim.nodeId(), victimToken, (killNanos - renewSeenNanos) / 1_000_000, deliveredAtKill,
                    takeover.ownerNode(), newToken, takeoverTime.toMillis(), sinceRenewal.toMillis(),
                    (takeoverSeenNanos - killNanos) / 1_000_000, resumeFrom, resent, total.toMillis());

            assertThat(takeoverTime).as("takeover time (DB clock)").isLessThanOrEqualTo(TAKEOVER_BOUND);
            assertThat(sinceRenewal).as("takeover time since the last renewal (DB clock)")
                    .isLessThanOrEqualTo(TAKEOVER_BOUND);
            assertThat(sink.deliveriesFor(runId)).hasSize(EMPLOYEES);
            assertThat(sink.distinctDeliveries()).isEqualTo(EMPLOYEES);
            assertThat(sink.maxDeliveriesPerKey()).isEqualTo(1);
            assertThat(sink.attempts()).noneMatch(a -> a.outcome() == DeliveryOutcome.STALE_TOKEN);
            // resumed from a checkpoint, not from item 0, and re-sent only the tail of one chunk
            assertThat(resumeFrom).isPositive();
            assertThat(resumeFrom % CHUNK).isZero();
            assertThat(byNew).hasSize(EMPLOYEES - resumeFrom);
            assertThat(resent).isPositive().isLessThanOrEqualTo(CHUNK + 2);
            assertThat(nodes.stream().filter(n -> n != victim).mapToDouble(n -> n.metrics().orphanTakeoverCount())
                    .sum()).isEqualTo(1.0);
            // the histogram measures last renewal -> takeover, the interval the 11.2 s alert bounds
            Duration recorded = nodes.stream().filter(n -> n != victim)
                    .map(n -> n.metrics().orphanTakeoverTimeTotal()).reduce(Duration.ZERO, Duration::plus);
            System.out.printf("NodeCrashIT: scheduler_orphan_takeover_time_seconds recorded %d ms%n",
                    recorded.toMillis());
            assertThat(recorded).as("recorded takeover time").isBetween(RUN_LEASE, TAKEOVER_BOUND);
            assertThat(recorded.minus(sinceRenewal).abs()).as("recorded vs measured since the last renewal")
                    .isLessThanOrEqualTo(Duration.ofMillis(50));
        }
    }

    private static int indexOf(String itemKey) {
        String emp = itemKey.substring(itemKey.lastIndexOf(':') + 1); // emp-0001 -> index 0
        return Integer.parseInt(emp.substring("emp-".length())) - 1;
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
