package com.moveinsync.scheduler.app.repository.claim;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.domain.TenantId;
import com.moveinsync.scheduler.core.policy.ExponentialFullJitterBackoff;
import com.moveinsync.scheduler.core.policy.FailureClassifier;
import com.moveinsync.scheduler.core.policy.RetryDecider;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** R4: 32 threads on separate connections claim the same 1,000 due runs; each is claimed once. */
class ClaimRaceIT {

    private static final int THREADS = 32;
    private static final int RUNS = 1_000;

    private static EngineTestDb db;
    private static Fixtures fx;
    private static HikariDataSource pool;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("claim_race_it");
        fx = new Fixtures(db.jdbc());
        pool = db.newPool("race", THREADS + 2);
    }

    @AfterAll
    static void close() {
        db.close();
    }

    private static JdbcClaimProtocol protocol() {
        return new JdbcClaimProtocol(pool, Duration.ofSeconds(30),
                new RetryDecider(ExponentialFullJitterBackoff.withDefaults(), new FailureClassifier()),
                ClaimObserver.NONE);
    }

    /** Wide cap: every thread races for the same rows; SKIP LOCKED must hand out disjoint runs. */
    @Test
    void everyRunClaimedExactlyOnceWithUniqueTokens() throws Exception {
        fx.tenant("race-wide", RUNS);
        UUID job = fx.job("race-wide").insert();
        fx.dueRuns(job, RUNS);
        List<ClaimedRun> claims = race("race-wide", 7, false);

        assertThat(claims).hasSize(RUNS);
        assertThat(claims.stream().map(ClaimedRun::runId).distinct().count()).isEqualTo(RUNS);
        assertThat(claims.stream().map(ClaimedRun::token).distinct().count()).isEqualTo(RUNS);
        assertThat(claims.stream().map(ClaimedRun::slotNo).distinct().count()).isEqualTo(RUNS);
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE job_id = ? AND state = 'RUNNING' AND attempt = 1",
                job)).isEqualTo(RUNS);
    }

    /** Narrow cap with commits: slots are reused all the time, still one claim per run and token. */
    @Test
    void narrowCapWithCommitsClaimsEachRunOnce() throws Exception {
        fx.tenant("race-narrow", 16);
        UUID job = fx.job("race-narrow").insert();
        fx.dueRuns(job, RUNS);
        List<ClaimedRun> claims = race("race-narrow", 2, true);

        assertThat(claims).hasSize(RUNS);
        Map<Long, Integer> perRun = new ConcurrentHashMap<>();
        Set<FencingToken> tokens = new HashSet<>();
        for (ClaimedRun c : claims) {
            perRun.merge(c.runId(), 1, Integer::sum);
            assertThat(tokens.add(c.token())).isTrue();
            assertThat(c.slotNo()).isBetween(0, 15);
        }
        assertThat(perRun.values()).allMatch(n -> n == 1);
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE job_id = ? AND state = 'SUCCEEDED' "
                + "AND slot_no IS NULL", job)).isEqualTo(RUNS);
    }

    private List<ClaimedRun> race(String tenant, int batch, boolean commit) throws Exception {
        JdbcClaimProtocol protocol = protocol();
        List<ClaimedRun> claims = new CopyOnWriteArrayList<>();
        AtomicInteger commitsRejected = new AtomicInteger();
        CyclicBarrier barrier = new CyclicBarrier(THREADS);
        ExecutorService exec = Executors.newFixedThreadPool(THREADS);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < THREADS; t++) {
                String node = "node-" + t;
                futures.add(exec.submit(() -> {
                    barrier.await();
                    int idle = 0;
                    while (idle < 50) {
                        List<ClaimedRun> got = protocol.claimDue(node, TenantId.of(tenant), Fixtures.ALL_PARTITIONS, batch);
                        claims.addAll(got);
                        if (got.isEmpty()) {
                            idle++;
                            if (remaining(tenant) == 0) {
                                break;
                            }
                            Thread.sleep(2); // other threads hold the slots; let them commit
                        } else {
                            idle = 0;
                        }
                        if (commit) {
                            for (ClaimedRun c : got) {
                                if (!protocol.commitSuccess(c.runId(), c.token())) {
                                    commitsRejected.incrementAndGet();
                                }
                            }
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> f : futures) {
                f.get(120, TimeUnit.SECONDS);
            }
        } finally {
            exec.shutdownNow();
        }
        assertThat(commitsRejected).hasValue(0);
        return claims;
    }

    private static long remaining(String tenant) {
        return fx.count("SELECT count(*) FROM job_run WHERE tenant_id = ? AND state = 'SCHEDULED'", tenant);
    }
}
