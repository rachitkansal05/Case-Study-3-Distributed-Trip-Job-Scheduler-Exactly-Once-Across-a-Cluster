package com.moveinsync.scheduler.app.repository.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.core.ports.MembershipStore;
import com.moveinsync.scheduler.core.ports.PartitionLeaseStore.PartitionLease;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Partition leases, membership and the DB clock against real PostgreSQL. */
class ClusterStoresIT {

    private static EngineTestDb db;
    private JdbcPartitionLeaseStore leases;
    private JdbcMembershipStore members;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("cluster_stores_it");
    }

    @AfterAll
    static void close() {
        db.close();
    }

    @BeforeEach
    void reset() {
        db.jdbc().sql("UPDATE partition_lease SET owner_node = NULL, lease_until = NULL, epoch = 0").update();
        db.jdbc().sql("DELETE FROM scheduler_node").update();
        leases = new JdbcPartitionLeaseStore(db.admin());
        members = new JdbcMembershipStore(db.admin());
    }

    @Test
    void seededUnownedRowsCanBeAcquired() {
        Optional<PartitionLease> lease = leases.tryAcquireOrRenew(5, "n1", Duration.ofSeconds(6));
        assertThat(lease).isPresent();
        assertThat(lease.get().ownerNode()).isEqualTo("n1");
        assertThat(lease.get().epoch()).isEqualTo(1);
        assertThat(leases.ownedBy("n1")).containsExactly(5);
        assertThat(leases.findAll()).hasSize(128);
    }

    @Test
    void validLeaseOfAnotherNodeIsNotTakenButExpiredOneIs() {
        assertThat(leases.tryAcquireOrRenew(7, "n1", Duration.ofSeconds(6))).isPresent();
        assertThat(leases.tryAcquireOrRenew(7, "n2", Duration.ofSeconds(6))).isEmpty();
        assertThat(leases.tryAcquireOrRenew(7, "n1", Duration.ofSeconds(6))).isPresent(); // renew
        db.jdbc().sql("UPDATE partition_lease SET lease_until = clock_timestamp() - interval '1 ms' "
                + "WHERE partition_id = 7").update();
        assertThat(leases.ownedBy("n1")).isEmpty();
        assertThat(leases.tryAcquireOrRenew(7, "n2", Duration.ofSeconds(6)).orElseThrow().ownerNode())
                .isEqualTo("n2");
    }

    @Test
    void releaseOnlyReleasesOwnLease() {
        leases.tryAcquireOrRenew(9, "n1", Duration.ofSeconds(6));
        assertThat(leases.release(9, "n2")).isFalse();
        assertThat(leases.release(9, "n1")).isTrue();
        PartitionLease row = leases.findAll().get(9);
        assertThat(row.ownerNode()).isNull();
        assertThat(row.leaseUntil()).isNull();
        assertThat(leases.tryAcquireOrRenew(9, "n2", Duration.ofSeconds(6))).isPresent();
    }

    @Test
    void concurrentAcquirersExactlyOneWins() throws Exception {
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger winners = new AtomicInteger();
        try {
            List<Future<?>> fs = new java.util.ArrayList<>();
            for (int i = 0; i < threads; i++) {
                String node = "n" + i;
                fs.add(pool.submit(() -> {
                    start.await();
                    if (leases.tryAcquireOrRenew(11, node, Duration.ofSeconds(6)).isPresent()) {
                        winners.incrementAndGet();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : fs) {
                f.get();
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(winners).hasValue(1);
    }

    @Test
    void membershipUsesDbTime() {
        members.heartbeat("a");
        members.heartbeat("b");
        assertThat(members.liveNodes(Duration.ofSeconds(6))).extracting(MembershipStore.NodeInfo::nodeId)
                .containsExactly("a", "b");
        db.jdbc().sql("UPDATE scheduler_node SET heartbeat_at = clock_timestamp() - interval '7 s' "
                + "WHERE node_id = 'a'").update();
        assertThat(members.liveNodes(Duration.ofSeconds(6))).extracting(MembershipStore.NodeInfo::nodeId)
                .containsExactly("b");
        members.heartbeat("a");
        assertThat(members.liveNodes(Duration.ofSeconds(6))).hasSize(2);
        members.deregister("a");
        assertThat(members.liveNodes(Duration.ofSeconds(6))).extracting(MembershipStore.NodeInfo::nodeId)
                .containsExactly("b");
    }

    @Test
    void dbClockTracksDatabaseTime() {
        DbSchedulerClock clock = new DbSchedulerClock(db.admin(),
                java.time.Clock.offset(java.time.Clock.systemUTC(), Duration.ofMinutes(5)));
        assertThat(clock.now()).isAfter(Instant.now().plus(Duration.ofMinutes(4))); // skewed before refresh
        clock.refresh();
        assertThat(Duration.between(Instant.now(), clock.now()).abs()).isLessThan(Duration.ofSeconds(1));
        long a = clock.monotonicNanos();
        assertThat(clock.monotonicNanos()).isGreaterThanOrEqualTo(a);
    }

    private Instant aliveSince(String node) {
        return db.jdbc().sql("SELECT alive_since FROM scheduler_node WHERE node_id = ?").param(node)
                .query(java.time.OffsetDateTime.class).single().toInstant();
    }

    /** V204: alive_since survives regular heartbeats and restarts after a gap of deadAfter or more. */
    @Test
    void aliveSinceRestartsOnlyAfterAHeartbeatGap() {
        members.heartbeat("n1");
        db.jdbc().sql("UPDATE scheduler_node SET alive_since = clock_timestamp() - interval '1 hour', "
                + "heartbeat_at = clock_timestamp() - interval '2 seconds' WHERE node_id = 'n1'").update();
        Instant hourAgo = aliveSince("n1");
        members.heartbeat("n1"); // 2 s gap < 6 s dead-after: same unbroken run
        assertThat(aliveSince("n1")).isEqualTo(hourAgo);

        db.jdbc().sql("UPDATE scheduler_node SET heartbeat_at = clock_timestamp() - interval '7 seconds' "
                + "WHERE node_id = 'n1'").update();
        members.heartbeat("n1"); // 7 s gap: down or cut off from the DB, so the run restarts
        assertThat(aliveSince("n1")).isAfter(hourAgo.plus(Duration.ofMinutes(59)));

        db.jdbc().sql("UPDATE scheduler_node SET alive_since = NULL WHERE node_id = 'n1'").update();
        members.heartbeat("n1"); // a row from before V204 gets a value
        assertThat(aliveSince("n1")).isNotNull();
    }
}
