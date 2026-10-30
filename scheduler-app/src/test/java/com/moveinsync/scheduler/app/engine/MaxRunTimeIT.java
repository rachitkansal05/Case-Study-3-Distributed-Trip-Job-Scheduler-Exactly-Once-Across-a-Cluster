package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.app.engine.support.TestCluster;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.LeaseLostReason;
import com.moveinsync.scheduler.core.policy.RetryableJobException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * R1 (plan sections 07, 09 and 16): {@code max_run_time} is a hard stop. A handler that runs past
 * it is interrupted, the attempt is recorded as {@code TIMEOUT} (retryable: backoff, then the dead
 * letter after {@code max_attempts}), and it never commits a success. A handler that ignores the
 * interrupt is abandoned one renew interval later: the engine records {@code TIMEOUT} for it and
 * frees its slot, and the abandoned thread can no longer send effects or write an outcome. One
 * node with the fast timings of {@link TestCluster#fast} (run lease 3 s, renew 1 s); max run time
 * 400 ms.
 *
 * <p>Handlers: BILLING_CLOSE sleeps (interruptibly) on every execution; ROSTER_GEN sleeps on its
 * first execution and succeeds afterwards; NO_SHOW_CANCEL swallows the interrupt on its first
 * execution and returns normally, then succeeds; PICKUP_ALERT ignores interrupts on its very first
 * execution (of any run) until the test releases it, then tries one effect; later ones succeed.
 */
class MaxRunTimeIT {

    private static final Duration MAX_RUN_TIME = Duration.ofMillis(400);
    private static final Duration RENEW_INTERVAL = Duration.ofSeconds(1);
    private static final String TIMEOUT_ERROR = "TIMEOUT: max run time exceeded";

    private static EngineTestDb db;
    private static Fixtures fx;
    private static FakeEffectSink sink;
    private static TestCluster cluster;
    private static SchedulerNode node;

    /** runId -> how long each interrupted execution ran before the interrupt reached it (ms). */
    private static final Map<Long, List<Long>> stoppedAfterMs = new ConcurrentHashMap<>();

    private static final ItScriptedHandler sleeper = new ItScriptedHandler(HandlerType.BILLING_CLOSE,
            (ctx, execution) -> sleepUntilInterrupted(ctx.run().runId()));

    private static final ItScriptedHandler slowOnce = new ItScriptedHandler(HandlerType.ROSTER_GEN,
            (ctx, execution) -> {
                if (execution == 1) {
                    sleepUntilInterrupted(ctx.run().runId());
                }
            });

    private static final ItScriptedHandler swallowsOnce = new ItScriptedHandler(HandlerType.NO_SHOW_CANCEL,
            (ctx, execution) -> {
                if (execution == 1) {
                    try {
                        sleepUntilInterrupted(ctx.run().runId());
                    } catch (InterruptedException ignored) {
                        // a badly written handler: drops the interrupt and returns as if it had finished
                    }
                }
            });

    private static final CountDownLatch releaseStuck = new CountDownLatch(1);
    private static final AtomicBoolean stuckOnce = new AtomicBoolean();
    private static final AtomicBoolean stuckRunning = new AtomicBoolean();
    private static final AtomicReference<Throwable> stuckEffect = new AtomicReference<>();

    private static final ItScriptedHandler ignoresInterrupt = new ItScriptedHandler(HandlerType.PICKUP_ALERT,
            (ctx, execution) -> {
                if (!stuckOnce.compareAndSet(false, true)) {
                    return; // only the very first execution gets stuck
                }
                stuckRunning.set(true);
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (System.nanoTime() < end) {
                    try {
                        if (releaseStuck.await(50, TimeUnit.MILLISECONDS)) {
                            break;
                        }
                    } catch (InterruptedException ignored) {
                        // ignores the max-run-time interrupt on purpose
                    }
                }
                stuckRunning.set(false);
                try {
                    ctx.send(ctx.run().idemKey() + ":late", "{}");
                } catch (RuntimeException e) {
                    stuckEffect.set(e);
                    throw e;
                }
            });

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("max_run_time_it");
        fx = new Fixtures(db.jdbc());
        sink = new FakeEffectSink();
        HandlerRegistry registry = new HandlerRegistry(List.of(sleeper, slowOnce, swallowsOnce, ignoresInterrupt));
        cluster = new TestCluster(db, sink, registry);
        node = cluster.start(TestCluster.fast("mrt1").runRenewInterval(RENEW_INTERVAL).build());
    }

    @AfterAll
    static void close() {
        releaseStuck.countDown();
        if (cluster != null) {
            cluster.close();
        }
        if (db != null) {
            db.close();
        }
    }

    @Test
    void handlerRunningPastMaxRunTimeIsStoppedRetriedAndDeadLetteredAsTimeout() {
        double timeoutsBefore = node.metrics().outcomeCount("timeout");
        fx.tenant("mrt_dead", 2);
        UUID job = fx.job("mrt_dead").handler(HandlerType.BILLING_CLOSE).maxRunTime(MAX_RUN_TIME).maxAttempts(3)
                .insert();
        long run = fx.dueRuns(job, 1).get(0);

        awaitState(run, "DEAD_LETTERED");

        assertThat(sleeper.executionsOf(run)).isEqualTo(3);
        assertThat(fx.intColumn(run, "attempt")).isEqualTo(3);
        assertThat(fx.intColumn(run, "lease_lost_count")).as("a timeout is a failure, not a lease loss").isZero();
        assertThat(lastError(run)).isEqualTo(TIMEOUT_ERROR);
        assertThat(fx.count("SELECT count(*) FROM dead_letter WHERE run_id = ? AND error_code = 'TIMEOUT'"
                + " AND attempts = 3", run)).isEqualTo(1);
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE id = ? AND slot_no IS NULL", run)).isEqualTo(1);
        // each execution was interrupted at the max run time, long before the 3 s lease could matter
        assertThat(stoppedAfterMs.get(run)).hasSize(3).allSatisfy(ms -> assertThat(ms)
                .isBetween(MAX_RUN_TIME.toMillis() - 50, MAX_RUN_TIME.toMillis() + 1_500));
        await().atMost(Duration.ofSeconds(5)).until(() ->
                node.metrics().outcomeCount("timeout") - timeoutsBefore == 3);
        assertThat(node.metrics().deadLetterCount("TIMEOUT")).isEqualTo(1.0);
    }

    @Test
    void timedOutAttemptIsRetriedAndTheRetryCanSucceed() {
        fx.tenant("mrt_retry", 2);
        UUID job = fx.job("mrt_retry").handler(HandlerType.ROSTER_GEN).maxRunTime(MAX_RUN_TIME).maxAttempts(3)
                .insert();
        long run = fx.dueRuns(job, 1).get(0);

        awaitState(run, "SUCCEEDED");

        assertThat(slowOnce.executionsOf(run)).isEqualTo(2);
        assertThat(fx.intColumn(run, "attempt")).isEqualTo(2);
        assertThat(lastError(run)).as("error of the timed-out first attempt").isEqualTo(TIMEOUT_ERROR);
    }

    @Test
    void handlerThatSwallowsTheInterruptAndReturnsIsStillATimeoutNotASuccess() {
        fx.tenant("mrt_swallow", 2);
        UUID job = fx.job("mrt_swallow").handler(HandlerType.NO_SHOW_CANCEL).maxRunTime(MAX_RUN_TIME)
                .maxAttempts(3).insert();
        long run = fx.dueRuns(job, 1).get(0);

        awaitState(run, "SUCCEEDED");

        assertThat(swallowsOnce.executionsOf(run)).isEqualTo(2);
        assertThat(fx.intColumn(run, "attempt")).as("the first return did not count as a success").isEqualTo(2);
        assertThat(lastError(run)).isEqualTo(TIMEOUT_ERROR);
    }

    @Test
    void handlerThatIgnoresTheInterruptIsHardStoppedAndCanNoLongerActOrCommit() throws Exception {
        double staleBefore = node.metrics().staleCommitRejectedCount();
        double fencedBefore = node.metrics().leaseLostCount(LeaseLostReason.FENCED_WRITE_REJECTED);
        fx.tenant("mrt_stuck", 1);
        UUID job = fx.job("mrt_stuck").handler(HandlerType.PICKUP_ALERT).maxRunTime(MAX_RUN_TIME).maxAttempts(1)
                .insert();
        long run = fx.dueRuns(job, 1).get(0);
        long t0 = System.nanoTime();

        // recorded by the engine while the handler is still spinning: max run time + one renew interval
        awaitState(run, "DEAD_LETTERED");
        long deadAfterMs = (System.nanoTime() - t0) / 1_000_000;
        assertThat(stuckRunning.get()).as("the handler is still running").isTrue();
        assertThat(deadAfterMs).as("max run time + one renew interval, plus scheduling").isLessThan(5_000);
        assertThat(lastError(run)).isEqualTo(TIMEOUT_ERROR);
        assertThat(fx.intColumn(run, "lease_lost_count")).isZero();
        assertThat(fx.count("SELECT count(*) FROM dead_letter WHERE run_id = ? AND error_code = 'TIMEOUT'", run))
                .isEqualTo(1);
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE id = ? AND slot_no IS NULL AND lease_until IS NOT NULL",
                run)).as("slot freed").isEqualTo(1);

        // the slot (cap 1) is usable again while the abandoned thread still runs
        UUID next = fx.job("mrt_stuck").handler(HandlerType.PICKUP_ALERT).maxRunTime(Duration.ofSeconds(30)).insert();
        long nextRun = fx.dueRuns(next, 1).get(0);
        awaitState(nextRun, "SUCCEEDED");
        assertThat(stuckRunning.get()).isTrue();

        // released, the abandoned thread tries an effect: refused before it reaches the sink
        releaseStuck.countDown();
        await().atMost(Duration.ofSeconds(5)).until(() -> stuckEffect.get() != null);
        assertThat(stuckEffect.get()).isInstanceOf(RetryableJobException.class).hasMessageContaining("max run time");
        assertThat(sink.deliveriesFor(run)).isEmpty();
        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                .until(() -> "DEAD_LETTERED".equals(fx.state(run)));
        assertThat(node.metrics().staleCommitRejectedCount()).isEqualTo(staleBefore);
        assertThat(node.metrics().leaseLostCount(LeaseLostReason.FENCED_WRITE_REJECTED)).isEqualTo(fencedBefore);
    }

    // ------------------------------------------------------------------ helpers

    private static void sleepUntilInterrupted(long runId) throws InterruptedException {
        long start = System.nanoTime();
        try {
            Thread.sleep(Duration.ofMinutes(1));
        } catch (InterruptedException e) {
            stoppedAfterMs.computeIfAbsent(runId, id -> new CopyOnWriteArrayList<>())
                    .add((System.nanoTime() - start) / 1_000_000);
            throw e;
        }
        throw new AssertionError("not interrupted at the max run time");
    }

    private static String lastError(long run) {
        return db.jdbc().sql("SELECT last_error FROM job_run WHERE id = ?").param(run).query(String.class).single();
    }

    private static void awaitState(long run, String state) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20))
                .until(() -> state.equals(fx.state(run)));
    }
}
