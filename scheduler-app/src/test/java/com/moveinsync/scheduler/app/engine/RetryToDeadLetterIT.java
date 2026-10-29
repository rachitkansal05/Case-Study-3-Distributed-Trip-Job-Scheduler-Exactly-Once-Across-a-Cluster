package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.app.engine.support.TestCluster;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.app.handler.JobHandler;
import com.moveinsync.scheduler.app.handler.PickupAlertHandler;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.LeaseLostReason;
import com.moveinsync.scheduler.core.policy.PermanentJobException;
import com.moveinsync.scheduler.core.policy.RetryableJobException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * R8 (plan sections 09 and 16): retryable failures use up {@code max_attempts} with backoff, then
 * dead-letter; permanent failures and invalid payloads dead-letter after one attempt; lease losses
 * do not use up attempts. One node with the fast timings of {@link TestCluster#fast} (backoff base
 * 50 ms, cap 400 ms, run lease 3 s, orphan sweep 200 ms).
 *
 * <p>Handlers: ROSTER_GEN always throws a retryable failure, NO_SHOW_CANCEL a permanent one,
 * PICKUP_ALERT is the real handler, BILLING_CLOSE blocks on its first three executions (the test
 * expires their leases) and fails retryably afterwards.
 */
class RetryToDeadLetterIT {

    private static final int MAX_ATTEMPTS = 5;
    private static final int LEASE_LOSSES = 3;

    private static EngineTestDb db;
    private static Fixtures fx;
    private static TestCluster cluster;
    private static SchedulerNode node;

    private static final Map<Long, List<Long>> retryCallNanos = new ConcurrentHashMap<>();
    private static final BlockingQueue<Integer> blockedExecutions = new LinkedBlockingQueue<>();

    private static final ItScriptedHandler alwaysRetryable = new ItScriptedHandler(HandlerType.ROSTER_GEN,
            (ctx, execution) -> {
                retryCallNanos.computeIfAbsent(ctx.run().runId(), id -> new CopyOnWriteArrayList<>())
                        .add(System.nanoTime());
                throw new RetryableJobException("IT_ALWAYS_FAILS", "injected retryable failure");
            });

    private static final ItScriptedHandler permanent = new ItScriptedHandler(HandlerType.NO_SHOW_CANCEL,
            (ctx, execution) -> {
                throw new PermanentJobException("IT_PERMANENT", "injected permanent failure");
            });

    private static final ItScriptedHandler leaseLosing = new ItScriptedHandler(HandlerType.BILLING_CLOSE,
            (ctx, execution) -> {
                if (execution <= LEASE_LOSSES) {
                    blockedExecutions.add(execution);
                    new CountDownLatch(1).await(); // until the renewer interrupts us for the lost lease
                    throw new AssertionError("blocked execution was not interrupted");
                }
                throw new RetryableJobException("IT_AFTER_LEASE_LOSS", "injected failure after lease losses");
            });

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("retry_to_dead_letter_it");
        fx = new Fixtures(db.jdbc());
        HandlerRegistry registry = new HandlerRegistry(List.of(alwaysRetryable, permanent, leaseLosing,
                new PickupAlertHandler(new ObjectMapper())));
        cluster = new TestCluster(db, new FakeEffectSink(), registry);
        node = cluster.start(TestCluster.fast("dlq1").build());
    }

    @AfterAll
    static void close() {
        if (cluster != null) {
            cluster.close();
        }
        if (db != null) {
            db.close();
        }
    }

    @Test
    void alwaysFailingHandlerIsDeadLetteredAfterMaxAttempts() {
        fx.tenant("dlq_retry", 4);
        UUID job = fx.job("dlq_retry").handler(HandlerType.ROSTER_GEN).maxAttempts(MAX_ATTEMPTS).insert();
        long run = fx.dueRuns(job, 1).get(0);

        awaitDeadLettered(run);

        assertDeadLetter(run, "IT_ALWAYS_FAILS", MAX_ATTEMPTS, "IT_ALWAYS_FAILS: injected retryable failure");
        assertThat(alwaysRetryable.executionsOf(run)).isEqualTo(MAX_ATTEMPTS);
        assertThat(fx.intColumn(run, "lease_lost_count")).isZero();
        // full-jitter backoff is random(0, min(cap, base * 2^attempt)), so individual gaps need not
        // grow; they are reported, not asserted
        List<Long> calls = retryCallNanos.get(run);
        List<Long> gapsMs = new ArrayList<>();
        for (int i = 1; i < calls.size(); i++) {
            gapsMs.add((calls.get(i) - calls.get(i - 1)) / 1_000_000);
        }
        System.out.println("RetryToDeadLetterIT(a): attempts=" + calls.size() + " gapsMs=" + gapsMs);
    }

    @Test
    void permanentFailureIsDeadLetteredAfterOneAttempt() {
        fx.tenant("dlq_perm", 4);
        UUID job = fx.job("dlq_perm").handler(HandlerType.NO_SHOW_CANCEL).maxAttempts(MAX_ATTEMPTS).insert();
        long run = fx.dueRuns(job, 1).get(0);

        awaitDeadLettered(run);

        assertDeadLetter(run, "IT_PERMANENT", 1, "IT_PERMANENT: injected permanent failure");
        assertThat(permanent.executionsOf(run)).isEqualTo(1);
    }

    @Test
    void invalidPayloadIsDeadLetteredAfterOneAttempt() {
        fx.tenant("dlq_payload", 4);
        UUID job = fx.job("dlq_payload").handler(HandlerType.PICKUP_ALERT).payload("{}")
                .maxAttempts(MAX_ATTEMPTS).insert();
        long run = fx.dueRuns(job, 1).get(0);

        awaitDeadLettered(run);

        String lastError = lastError(run);
        assertThat(lastError).startsWith(JobHandler.INVALID_PAYLOAD + ": ");
        assertDeadLetter(run, JobHandler.INVALID_PAYLOAD, 1, lastError);
    }

    @Test
    void leaseLossesDoNotConsumeAttempts() throws Exception {
        double fencedBefore = node.metrics().leaseLostCount(LeaseLostReason.FENCED_WRITE_REJECTED);
        fx.tenant("dlq_lease", 4);
        UUID job = fx.job("dlq_lease").handler(HandlerType.BILLING_CLOSE).maxAttempts(MAX_ATTEMPTS).insert();
        long run = fx.dueRuns(job, 1).get(0);

        for (int i = 1; i <= LEASE_LOSSES; i++) {
            Integer blocked = blockedExecutions.poll(20, TimeUnit.SECONDS);
            assertThat(blocked).as("execution %d started and blocked", i).isEqualTo(i);
            assertThat(fx.state(run)).isEqualTo("RUNNING");
            assertThat(fx.intColumn(run, "attempt")).as("a lease loss gives its attempt back").isEqualTo(1);
            assertThat(fx.intColumn(run, "lease_lost_count")).isEqualTo(i - 1);
            // the renewer's fenced renew now updates 0 rows; the orphan sweep restarts the run
            fx.expireLease(run);
        }

        awaitDeadLettered(run);

        assertDeadLetter(run, "IT_AFTER_LEASE_LOSS", MAX_ATTEMPTS,
                "IT_AFTER_LEASE_LOSS: injected failure after lease losses");
        assertThat(fx.intColumn(run, "lease_lost_count")).isEqualTo(LEASE_LOSSES);
        assertThat(leaseLosing.executionsOf(run)).isEqualTo(LEASE_LOSSES + MAX_ATTEMPTS);
        // every blocked execution was stopped by a 0-row fenced renew
        await().atMost(Duration.ofSeconds(10)).until(() ->
                node.metrics().leaseLostCount(LeaseLostReason.FENCED_WRITE_REJECTED) - fencedBefore >= LEASE_LOSSES);
        assertThat(node.metrics().leaseLostCount(LeaseLostReason.FENCED_WRITE_REJECTED) - fencedBefore)
                .isEqualTo(LEASE_LOSSES);
        assertThat(node.metrics().staleCommitRejectedCount()).isZero();
    }

    // ------------------------------------------------------------------ helpers

    private static void awaitDeadLettered(long run) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(50))
                .until(() -> "DEAD_LETTERED".equals(fx.state(run)));
    }

    private static String lastError(long run) {
        return db.jdbc().sql("SELECT last_error FROM job_run WHERE id = ?").param(run).query(String.class).single();
    }

    private static void assertDeadLetter(long run, String code, int attempts, String lastError) {
        assertThat(fx.state(run)).isEqualTo("DEAD_LETTERED");
        assertThat(fx.intColumn(run, "attempt")).isEqualTo(attempts);
        assertThat(lastError(run)).isEqualTo(lastError);
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE id = ? AND finished_at IS NOT NULL"
                + " AND slot_no IS NULL", run)).isEqualTo(1);
        assertThat(fx.count("SELECT count(*) FROM dead_letter WHERE run_id = ?", run)).isEqualTo(1);
        Map<String, Object> row = db.jdbc()
                .sql("SELECT error_code, attempts, tenant_id, replayed_at FROM dead_letter WHERE run_id = ?")
                .param(run).query().singleRow();
        assertThat(row.get("error_code")).isEqualTo(code);
        assertThat(((Number) row.get("attempts")).intValue()).isEqualTo(attempts);
        assertThat(row.get("replayed_at")).isNull();
    }
}
