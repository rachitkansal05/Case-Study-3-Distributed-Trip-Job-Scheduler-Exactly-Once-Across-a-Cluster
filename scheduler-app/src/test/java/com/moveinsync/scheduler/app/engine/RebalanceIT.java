package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.TestCluster;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * R5 (plan sections 06 and 16): with the REAL timings (partition lease 6 s, renew 2 s, loop 1 s,
 * heartbeat 2 s, dead after 6 s), kill one of 3 nodes right after a partition-loop tick that
 * renewed leases (worst case: its leases are fresh), and measure how long until the two survivors
 * own all 128 partitions, both in memory and in {@code partition_lease} (live owner, lease valid
 * by DB time). Bound: 7 s. Repeated 3 times, a fresh node joining in between.
 */
class RebalanceIT {

    private static final int PARTITIONS = 128;
    private static final Duration BOUND = Duration.ofSeconds(7);
    private static final int ROUNDS = 3;

    private static EngineTestDb db;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("rebalance_it");
    }

    @AfterAll
    static void close() {
        if (db != null) {
            db.close();
        }
    }

    /** One kill measurement. */
    private record Measurement(String victim, int victimPartitions, Duration wall, Duration db,
                               Duration sinceRenewTick, int samples) {
    }

    @Test
    void survivorsOwnAll128PartitionsWithin7sAfterWorstCaseKill() throws Exception {
        JdbcClient jdbc = db.jdbc();
        try (TestCluster cluster = new TestCluster(db, new FakeEffectSink(),
                HandlerRegistry.defaults(new ObjectMapper()))) {
            List<SchedulerNode> live = new ArrayList<>();
            for (int i = 1; i <= 3; i++) {
                live.add(cluster.start(EngineSettings.builder("rb-n" + i).build()));
            }
            List<Measurement> results = new ArrayList<>();
            for (int round = 0; round < ROUNDS; round++) {
                awaitStable(jdbc, live, Duration.ofSeconds(30), Duration.ofMillis(2500));
                SchedulerNode victim = pickVictim(live, round);
                List<SchedulerNode> survivors = new ArrayList<>(live);
                survivors.remove(victim);

                Measurement m = killAndMeasure(cluster, jdbc, victim, survivors);
                results.add(m);
                System.out.printf("RebalanceIT round %d: victim %s (%d partitions) -> all 128 owned by survivors "
                                + "after wall=%d ms, db=%d ms (since renewing tick %d ms), %d samples%n",
                        round + 1, m.victim(), m.victimPartitions(), m.wall().toMillis(), m.db().toMillis(),
                        m.sinceRenewTick().toMillis(), m.samples());

                live = survivors;
                if (round + 1 < ROUNDS) {
                    live.add(cluster.start(EngineSettings.builder("rb-n" + (4 + round)).build()));
                }
            }
            for (Measurement m : results) {
                assertThat(m.wall()).as("wall-clock recovery after killing %s", m.victim()).isLessThanOrEqualTo(BOUND);
                assertThat(m.db()).as("DB-clock recovery after killing %s", m.victim()).isLessThanOrEqualTo(BOUND);
            }
        }
    }

    private static SchedulerNode pickVictim(List<SchedulerNode> live, int round) {
        for (int i = 0; i < live.size(); i++) {
            SchedulerNode n = live.get((round + i) % live.size());
            if (!n.ownedPartitions().isEmpty()) {
                return n;
            }
        }
        throw new AssertionError("no node owns partitions");
    }

    private static Measurement killAndMeasure(TestCluster cluster, JdbcClient jdbc, SchedulerNode victim,
                                              List<SchedulerNode> survivors) throws InterruptedException {
        CountDownLatch renewedTick = new CountDownLatch(1);
        AtomicBoolean armed = new AtomicBoolean(true);
        AtomicLong tickNanos = new AtomicLong();
        victim.onPartitionTick(t -> {
            if (!t.renewed().isEmpty() && armed.compareAndSet(true, false)) {
                tickNanos.set(t.monotonicNanos());
                renewedTick.countDown();
            }
        });
        assertThat(renewedTick.await(10, TimeUnit.SECONDS)).as("victim renewed its partition leases").isTrue();
        long killNanos = System.nanoTime();
        cluster.kill(victim);
        Instant killDb = ItRunRow.dbNow(jdbc);
        int victimPartitions = victim.ownedPartitions().size();

        Set<String> survivorIds = new TreeSet<>();
        survivors.forEach(s -> survivorIds.add(s.nodeId()));
        List<String> ids = List.copyOf(survivorIds);
        long deadline = killNanos + Duration.ofSeconds(20).toNanos();
        int samples = 0;
        while (System.nanoTime() < deadline) {
            samples++;
            Set<Integer> a = survivors.get(0).ownedPartitions();
            Set<Integer> b = survivors.get(1).ownedPartitions();
            Set<Integer> both = new HashSet<>(a);
            both.retainAll(b);
            assertThat(both).as("partitions owned by both survivors (sample %d)", samples).isEmpty();
            Set<Integer> union = new HashSet<>(a);
            union.addAll(b);
            long nowNanos = System.nanoTime();
            if (union.size() == PARTITIONS) {
                DbSample s = dbSample(jdbc, ids);
                if (s.validLiveOwned() == PARTITIONS) {
                    // keep sampling a little to check no overlap appears right after recovery
                    checkNoOverlap(survivors, Duration.ofSeconds(1));
                    return new Measurement(victim.nodeId(), victimPartitions,
                            Duration.ofNanos(nowNanos - killNanos), Duration.between(killDb, s.dbNow()),
                            Duration.ofNanos(nowNanos - tickNanos.get()), samples);
                }
            }
            Thread.sleep(50);
        }
        DbSample s = dbSample(jdbc, ids);
        return fail("survivors did not own all partitions within 20 s: in memory %d+%d, valid live rows %d",
                survivors.get(0).ownedPartitions().size(), survivors.get(1).ownedPartitions().size(),
                s.validLiveOwned());
    }

    private static void checkNoOverlap(List<SchedulerNode> nodes, Duration window) throws InterruptedException {
        long end = System.nanoTime() + window.toNanos();
        while (System.nanoTime() < end) {
            assertThat(overlap(nodes)).as("partitions owned by two nodes").isEmpty();
            Thread.sleep(50);
        }
    }

    private static Set<Integer> overlap(List<SchedulerNode> nodes) {
        Set<Integer> seen = new HashSet<>();
        Set<Integer> dup = new TreeSet<>();
        for (SchedulerNode n : nodes) {
            for (int p : n.ownedPartitions()) {
                if (!seen.add(p)) {
                    dup.add(p);
                }
            }
        }
        return dup;
    }

    private record DbSample(long validLiveOwned, Instant dbNow) {
    }

    private static DbSample dbSample(JdbcClient jdbc, List<String> liveIds) {
        return jdbc.sql("""
                SELECT count(*) FILTER (WHERE owner_node = ANY (string_to_array(?, ','))
                                          AND lease_until >= clock_timestamp()) AS ok,
                       clock_timestamp() AS db_now
                  FROM partition_lease""")
                .param(String.join(",", liveIds))
                .query((rs, n) -> new DbSample(rs.getLong("ok"),
                        rs.getObject("db_now", OffsetDateTime.class).toInstant()))
                .single();
    }

    /**
     * Waits until the live nodes own all 128 partitions exactly once (each owning some), the table
     * agrees, and nothing changes for {@code stableFor}.
     */
    private static void awaitStable(JdbcClient jdbc, List<SchedulerNode> live, Duration atMost, Duration stableFor)
            throws InterruptedException {
        List<String> ids = live.stream().map(SchedulerNode::nodeId).toList();
        long deadline = System.nanoTime() + atMost.toNanos();
        long stableSince = -1;
        List<Set<Integer>> last = null;
        while (System.nanoTime() < deadline) {
            List<Set<Integer>> owned = live.stream().map(n -> (Set<Integer>) new TreeSet<>(n.ownedPartitions()))
                    .toList();
            int sum = owned.stream().mapToInt(Set::size).sum();
            Set<Integer> union = new HashSet<>();
            owned.forEach(union::addAll);
            boolean ok = sum == PARTITIONS && union.size() == PARTITIONS && owned.stream().noneMatch(Set::isEmpty)
                    && dbSample(jdbc, ids).validLiveOwned() == PARTITIONS;
            long now = System.nanoTime();
            if (ok && owned.equals(last)) {
                if (stableSince < 0) {
                    stableSince = now;
                }
                if (now - stableSince >= stableFor.toNanos()) {
                    return;
                }
            } else {
                stableSince = -1;
            }
            last = ok ? owned : null;
            Thread.sleep(100);
        }
        fail("partition ownership did not stabilise within %s: %s", atMost,
                live.stream().map(n -> n.nodeId() + "=" + n.ownedPartitions().size()).toList());
    }
}
