package com.moveinsync.scheduler.app.repository.claim;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.TenantId;
import com.moveinsync.scheduler.core.policy.ExponentialFullJitterBackoff;
import com.moveinsync.scheduler.core.policy.FailureClassifier;
import com.moveinsync.scheduler.core.policy.RetryDecider;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Plan section 21 issue 3: slot reuse racing a renewal. Two connections are forced, with latches
 * and lock-wait detection (no timing guesses), into each interleaving; exactly one side wins.
 *
 * <ul>
 *   <li>Order 1: A's renew is evaluated (lease still valid) and holds the row lock; the lease then
 *       expires by DB time; B locks the slot, reads the old committed lease as expired and sends the
 *       conditional reap, which waits on A; A commits; B's reap re-checks the new row version and
 *       updates 0 rows. A wins, B starts nothing in that slot.
 *   <li>Order 2: B reaps the expired holder (row lock held, uncommitted); A's renew waits on B; B
 *       commits; A's renew re-checks and updates 0 rows. B wins.
 * </ul>
 */
class SlotStealRaceIT {

    private static EngineTestDb db;
    private static Fixtures fx;
    private static DataSource pool;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("slot_steal_race_it");
        fx = new Fixtures(db.jdbc());
        pool = db.newPool("steal", 6);
    }

    @AfterAll
    static void close() {
        db.close();
    }

    private static RetryDecider decider() {
        return new RetryDecider(ExponentialFullJitterBackoff.withDefaults(), new FailureClassifier());
    }

    /** Tenant with one slot, holder running in it, and one more due run waiting for the slot. */
    private record Setup(long holder, ClaimedRun claim, long waiting) {
    }

    private Setup setup(String tenant) {
        fx.tenant(tenant, 1);
        UUID job = fx.job(tenant).insert();
        long holder = fx.dueRuns(job, 1).get(0);
        JdbcClaimProtocol plain = new JdbcClaimProtocol(pool, Duration.ofSeconds(10), decider(), ClaimObserver.NONE);
        ClaimedRun claim = plain.claimDue("node-a", TenantId.of(tenant), Fixtures.ALL_PARTITIONS, 1).get(0);
        long waiting = fx.dueRuns(job, 1).get(0);
        return new Setup(holder, claim, waiting);
    }

    @Test
    void bReadsExpiredThenARenewsThenBReapsAWins() throws Exception {
        Setup s = setup("steal-1");
        // lease about to expire
        db.jdbc().sql("UPDATE job_run SET lease_until = clock_timestamp() + interval '300 milliseconds' WHERE id = ?")
                .param(s.holder()).update();

        try (Connection a = pool.getConnection()) {
            a.setAutoCommit(false);
            int renewed;
            try (PreparedStatement ps = a.prepareStatement(JdbcClaimProtocol.RENEW)) {
                ps.setLong(1, 10_000);
                ps.setLong(2, s.holder());
                ps.setLong(3, s.claim().token().epoch());
                ps.setLong(4, s.claim().token().seq());
                renewed = ps.executeUpdate(); // evaluated while valid; row lock held, not committed
            }
            assertThat(renewed).isEqualTo(1);
            // the committed lease (old version) now expires by DB time
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> fx.count(
                    "SELECT count(*) FROM job_run WHERE id = ? AND lease_until < clock_timestamp()", s.holder()) == 1);

            CountDownLatch bAboutToReap = new CountDownLatch(1);
            JdbcClaimProtocol b = new JdbcClaimProtocol(pool, Duration.ofSeconds(10), decider(), ClaimObserver.NONE,
                    new ClaimHooks() {
                        @Override
                        public void beforeReap(long holderRunId) {
                            bAboutToReap.countDown();
                        }
                    });
            CompletableFuture<List<ClaimedRun>> bClaim = CompletableFuture.supplyAsync(() ->
                    b.claimDue("node-b", TenantId.of("steal-1"), Fixtures.ALL_PARTITIONS, 1));
            assertThat(bAboutToReap.await(10, TimeUnit.SECONDS)).as("B saw the expired lease").isTrue();
            awaitLockWaiters(1); // B's conditional reap is blocked on A's row lock
            a.commit();          // A's renewal becomes visible
            assertThat(bClaim.get(10, TimeUnit.SECONDS)).as("B started nothing").isEmpty();
        }
        assertThat(fx.state(s.holder())).isEqualTo("RUNNING");
        assertThat(fx.state(s.waiting())).isEqualTo("SCHEDULED");
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE id = ? AND fencing_seq = ? "
                + "AND lease_until > clock_timestamp() + interval '5 seconds' AND lease_lost_count = 0",
                s.holder(), s.claim().token().seq())).isEqualTo(1);
    }

    @Test
    void bReapsThenARenewsBWins() throws Exception {
        Setup s = setup("steal-2");
        fx.expireLease(s.holder());

        CountDownLatch reaped = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        JdbcClaimProtocol b = new JdbcClaimProtocol(pool, Duration.ofSeconds(10), decider(), ClaimObserver.NONE,
                new ClaimHooks() {
                    @Override
                    public void afterReap(long holderRunId, boolean freed) {
                        reaped.countDown();
                        try {
                            release.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
        CompletableFuture<List<ClaimedRun>> bClaim = CompletableFuture.supplyAsync(() ->
                b.claimDue("node-b", TenantId.of("steal-2"), Fixtures.ALL_PARTITIONS, 1));
        assertThat(reaped.await(10, TimeUnit.SECONDS)).isTrue();

        // A renews while B's reap is uncommitted. The lease is expired by DB time, so the fenced
        // predicate already fails on the committed version: A loses without waiting on B's lock.
        JdbcClaimProtocol a = new JdbcClaimProtocol(pool, Duration.ofSeconds(10), decider(), ClaimObserver.NONE);
        assertThat(a.renew(s.holder(), s.claim().token())).as("A's renewal is rejected").isFalse();
        release.countDown();
        List<ClaimedRun> bRuns = bClaim.get(10, TimeUnit.SECONDS);
        // and after B committed, A's token stays dead
        assertThat(a.renew(s.holder(), s.claim().token())).isFalse();
        assertThat(a.commitSuccess(s.holder(), s.claim().token())).isFalse();
        assertThat(bRuns).hasSize(1);
        assertThat(bRuns.get(0).slotNo()).isZero();
        assertThat(bRuns.get(0).token().isNewerThan(s.claim().token())).isTrue();
        assertThat(fx.intColumn(s.holder(), "lease_lost_count")).isEqualTo(1);
        // exactly one RUNNING run holds the only slot, with B's token
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE tenant_id = 'steal-2' AND state = 'RUNNING'"))
                .isEqualTo(1);
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE tenant_id = 'steal-2' AND state = 'RUNNING' "
                + "AND fencing_seq = ?", bRuns.get(0).token().seq())).isEqualTo(1);
    }

    private static void awaitLockWaiters(int n) {
        Awaitility.await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(10)).until(() -> fx.count(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
                        + "AND wait_event_type = 'Lock'") >= n);
    }
}
