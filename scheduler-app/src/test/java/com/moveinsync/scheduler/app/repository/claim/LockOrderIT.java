package com.moveinsync.scheduler.app.repository.claim;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.RunFailure;
import com.moveinsync.scheduler.core.domain.TenantId;
import com.moveinsync.scheduler.core.policy.ExponentialFullJitterBackoff;
import com.moveinsync.scheduler.core.policy.FailureClassifier;
import com.moveinsync.scheduler.core.policy.RetryDecider;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Plan section 07 lock order (slot row, then run row; commits never lock slots): claims, renewals,
 * commits (success, retry, dead letter), reaps of short leases, the orphan sweep and cap changes
 * with compaction all run concurrently for several seconds. No statement may fail with a deadlock
 * (40P01) and {@code pg_stat_database.deadlocks} must not move. Any other error fails the test too
 * (e.g. a unique violation on {@code one_run_per_slot} would mean a slot was handed out twice).
 */
class LockOrderIT {

    private static final List<String> TENANTS = List.of("lock-a", "lock-b", "lock-c");
    private static EngineTestDb db;
    private static Fixtures fx;
    private static DataSource pool;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("lock_order_it");
        fx = new Fixtures(db.jdbc());
        pool = db.newPool("lock-order", 30);
        for (String t : TENANTS) {
            fx.tenant(t, 8);
            for (int j = 0; j < 4; j++) {
                UUID job = fx.job(t).maxAttempts(1000).insert();
                fx.dueRuns(job, 500);
            }
        }
    }

    @AfterAll
    static void close() {
        db.close();
    }

    @Test
    void mixedLoadProducesNoDeadlocks() throws Exception {
        long deadlocksBefore = deadlocks();
        // 400 ms leases: many holders expire and are reaped by claims and the sweep
        JdbcClaimProtocol protocol = new JdbcClaimProtocol(pool, Duration.ofMillis(400),
                new RetryDecider(ExponentialFullJitterBackoff.withDefaults(), new FailureClassifier(), 1000),
                ClaimObserver.NONE);
        JdbcSlotCompaction compaction = new JdbcSlotCompaction(pool);
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong claims = new AtomicLong();
        AtomicLong commits = new AtomicLong();
        AtomicLong compactions = new AtomicLong();
        ExecutorService exec = Executors.newFixedThreadPool(20);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 14; i++) {
            String node = "lock-node-" + i;
            futures.add(exec.submit(() -> guard(errors, stop, () -> {
                ThreadLocalRandom r = ThreadLocalRandom.current();
                String tenant = TENANTS.get(r.nextInt(TENANTS.size()));
                List<ClaimedRun> got = protocol.claimDue(node, TenantId.of(tenant), Fixtures.ALL_PARTITIONS, 1 + r.nextInt(4));
                claims.addAndGet(got.size());
                for (ClaimedRun c : got) {
                    int roll = r.nextInt(10);
                    if (roll < 2) {
                        protocol.renew(c.runId(), c.token());
                    }
                    if (roll < 5) {
                        protocol.commitSuccess(c.runId(), c.token());
                    } else if (roll < 7) {
                        protocol.commitFailure(c.runId(), c.token(), new RunFailure.Retry(Duration.ZERO, "IT_RETRY", "x"));
                    } else if (roll < 8) {
                        protocol.checkpoint(c.runId(), c.token(), "{\"nextItem\":1}");
                    } else if (roll < 9) {
                        protocol.commitFailure(c.runId(), c.token(), new RunFailure.DeadLetter("IT_DEAD", "x"));
                    }
                    // else: abandoned, its lease expires and someone reaps it
                    commits.incrementAndGet();
                }
            })));
        }
        futures.add(exec.submit(() -> guard(errors, stop, () -> protocol.sweepOrphans("sweeper", 16))));
        for (int i = 0; i < 2; i++) {
            futures.add(exec.submit(() -> guard(errors, stop, () -> {
                ThreadLocalRandom r = ThreadLocalRandom.current();
                String tenant = TENANTS.get(r.nextInt(TENANTS.size()));
                int cap = new int[] {3, 8, 12}[r.nextInt(3)];
                db.jdbc().sql("UPDATE tenant SET configured_cap = ? WHERE id = ?").params(cap, tenant).update();
                for (JdbcSlotCompaction.TenantSlots t : compaction.tenantsNeedingWork()) {
                    compaction.apply(t);
                }
                compactions.incrementAndGet();
                Thread.sleep(20);
            })));
        }
        Thread.sleep(8_000);
        stop.set(true);
        exec.shutdown();
        assertThat(exec.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        for (Future<?> f : futures) {
            f.get();
        }
        System.out.println("LockOrderIT: claims=" + claims + " writes=" + commits + " compactions=" + compactions
                + " deadlocks=" + (deadlocks() - deadlocksBefore));
        assertThat(errors).as("errors (40P01 = deadlock)").isEmpty();
        assertThat(deadlocks()).isEqualTo(deadlocksBefore);
        assertThat(claims.get()).isGreaterThan(500);
        assertThat(compactions.get()).isGreaterThan(20);
        // invariant still intact at the end: running per tenant <= slot rows
        assertThat(fx.count("""
                SELECT count(*) FROM tenant t
                 WHERE (SELECT count(*) FROM job_run r WHERE r.tenant_id = t.id AND r.state = 'RUNNING')
                     > (SELECT count(*) FROM tenant_slot s WHERE s.tenant_id = t.id)""")).isZero();
    }

    @FunctionalInterface
    private interface Step {
        void run() throws Exception;
    }

    private static Void guard(List<Throwable> errors, AtomicBoolean stop, Step step) {
        while (!stop.get()) {
            try {
                step.run();
            } catch (Throwable t) {
                errors.add(t);
                return null;
            }
        }
        return null;
    }

    private static long deadlocks() {
        return fx.count("SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()");
    }
}
