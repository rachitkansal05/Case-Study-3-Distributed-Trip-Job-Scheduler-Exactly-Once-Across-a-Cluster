package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.app.engine.support.TestCluster;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.app.repository.deadletter.DeadLetterRepository;
import com.moveinsync.scheduler.app.repository.job.JobRepository;
import com.moveinsync.scheduler.app.repository.job.JobRunRepository;
import com.moveinsync.scheduler.app.security.Caller;
import com.moveinsync.scheduler.app.security.Role;
import com.moveinsync.scheduler.app.security.TenantScope;
import com.moveinsync.scheduler.app.service.DeadLetterService;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import com.moveinsync.scheduler.core.policy.PermanentJobException;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Plan section 09: an operator replay (and a manual trigger) is an explicit request to run, so the
 * claim never applies the misfire policy to it. Runs of {@code SKIP} and {@code COALESCE} jobs are
 * dead-lettered, then replayed an hour later (the test moves {@code scheduled_at} back by an hour,
 * threshold 60 s): each executes exactly once, none becomes {@code SKIPPED}, and the replay does
 * not report the dead-letter age as fire lag. Ordinary misfired runs of the same job, claimed in the
 * same transaction, are still skipped. One node with the fast timings of {@link TestCluster#fast}.
 *
 * <p>The ROSTER_GEN handler fails permanently on a run's first execution (straight to the dead
 * letter) and succeeds on every later one, except for the siblings, which always succeed;
 * NO_SHOW_CANCEL always succeeds.
 */
class ReplayMisfireIT {

    private static final Caller OPERATOR = new Caller("it-operator", "platform", Set.of(Role.OPERATOR));

    private static EngineTestDb db;
    private static JdbcClient jdbc;
    private static Fixtures fx;
    private static TestCluster cluster;
    private static SchedulerNode node;
    private static DeadLetterService deadLetters;

    /** Runs (ordinary misfired siblings) whose first execution succeeds. */
    private static final Set<Long> neverFail = ConcurrentHashMap.newKeySet();

    private static final ItScriptedHandler failFirst = new ItScriptedHandler(HandlerType.ROSTER_GEN,
            (ctx, execution) -> {
                if (execution == 1 && !neverFail.contains(ctx.run().runId())) {
                    throw new PermanentJobException("IT_FIRST_FAILS", "injected permanent failure");
                }
            });

    private static final ItScriptedHandler succeeds = new ItScriptedHandler(HandlerType.NO_SHOW_CANCEL,
            (ctx, execution) -> { });

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("replay_misfire_it");
        jdbc = db.jdbc();
        fx = new Fixtures(jdbc);
        deadLetters = new DeadLetterService(new DeadLetterRepository(jdbc));
        cluster = new TestCluster(db, new FakeEffectSink(), new HandlerRegistry(List.of(failFirst, succeeds)));
        node = cluster.start(TestCluster.fast("replay1").build());
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
    void skipJobReplayedAnHourLateRunsOnceWhileItsMisfiredSiblingIsSkipped() {
        String tenant = "rp_skip";
        fx.tenant(tenant, 4);
        UUID job = fx.job(tenant).misfire(MisfirePolicyType.SKIP).misfireThreshold(Duration.ofSeconds(60)).insert();
        double lagBefore = fireLag().count();
        long run = deadLetteredRun(job);
        backdate(run, Duration.ofHours(1));
        // the poller records the lag of the first start after its claim commits, maybe after the run died
        await().atMost(Duration.ofSeconds(5)).until(() -> fireLag().count() == lagBefore + 1);

        // both due in one claim: the replayed run and an ordinary occurrence 30 min late
        pause(tenant, true);
        long sibling = fx.run(job, dbNow().minus(Duration.ofMinutes(30)));
        deadLetters.replay(OPERATOR, run);
        pause(tenant, false);

        awaitState(run, "SUCCEEDED");
        awaitState(sibling, "SKIPPED");
        assertThat(failFirst.executionsOf(run)).as("dead-letter execution + replay").isEqualTo(2);
        assertThat(failFirst.executionsOf(sibling)).isZero();
        assertThat(fx.intColumn(run, "attempt")).isEqualTo(1);
        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                .until(() -> fireLag().count() == lagBefore + 1); // a replay is not a first start: no new lag

    }

    @Test
    void coalesceJobReplayingTwoOccurrencesLateRunsEachOnce() {
        String tenant = "rp_coalesce";
        fx.tenant(tenant, 4);
        UUID job = fx.job(tenant).misfire(MisfirePolicyType.COALESCE).misfireThreshold(Duration.ofSeconds(60))
                .insert();
        long older = deadLetteredRun(job);
        long newer = deadLetteredRun(job);
        backdate(older, Duration.ofHours(2));
        backdate(newer, Duration.ofHours(1));

        // without the exemption COALESCE would fire only the newer one and skip the older
        pause(tenant, true);
        long sibling1 = fx.run(job, dbNow().minus(Duration.ofMinutes(40)));
        long sibling2 = fx.run(job, dbNow().minus(Duration.ofMinutes(20)));
        neverFail.add(sibling2);
        deadLetters.replay(OPERATOR, older);
        deadLetters.replay(OPERATOR, newer);
        pause(tenant, false);

        awaitState(older, "SUCCEEDED");
        awaitState(newer, "SUCCEEDED");
        awaitState(sibling2, "SUCCEEDED");
        assertThat(fx.state(sibling1)).isEqualTo("SKIPPED");
        assertThat(failFirst.executionsOf(older)).isEqualTo(2);
        assertThat(failFirst.executionsOf(newer)).isEqualTo(2);
        assertThat(fx.intColumn(older, "coalesced_count")).isEqualTo(1);
        assertThat(fx.intColumn(newer, "coalesced_count")).isEqualTo(1);
        // the ordinary misfired runs coalesce among themselves only
        assertThat(fx.intColumn(sibling2, "coalesced_count")).isEqualTo(2);
    }

    @Test
    void manualTriggerOfASkipJobDelayedPastTheThresholdStillRuns() {
        String tenant = "rp_manual";
        fx.tenant(tenant, 4);
        UUID job = fx.job(tenant).handler(HandlerType.NO_SHOW_CANCEL).misfire(MisfirePolicyType.SKIP)
                .misfireThreshold(Duration.ofSeconds(60)).insert();

        // the tenant is paused for an hour after the trigger (the test moves scheduled_at back)
        pause(tenant, true);
        TenantScope scope = TenantScope.of(tenant);
        var jobRow = new JobRepository(jdbc).find(scope, job).orElseThrow();
        long run = new JobRunRepository(jdbc).insertManualRun(scope, jobRow, job + ":manual:it-1").orElseThrow();
        backdate(run, Duration.ofHours(1));
        pause(tenant, false);

        awaitState(run, "SUCCEEDED");
        assertThat(succeeds.executionsOf(run)).isEqualTo(1);
    }

    // ------------------------------------------------------------------ helpers

    /** A run of {@code job} that died on its first (permanently failing) execution. */
    private static long deadLetteredRun(UUID job) {
        long run = fx.dueRuns(job, 1).get(0);
        awaitState(run, "DEAD_LETTERED");
        return run;
    }

    /** As if the occurrence (and its whole history) were {@code by} older. */
    private static void backdate(long run, Duration by) {
        jdbc.sql("""
                        UPDATE job_run SET scheduled_at = scheduled_at - ? * interval '1 millisecond',
                               due_at = due_at - ? * interval '1 millisecond',
                               started_at = started_at - ? * interval '1 millisecond',
                               finished_at = finished_at - ? * interval '1 millisecond'
                         WHERE id = ?""")
                .params(by.toMillis(), by.toMillis(), by.toMillis(), by.toMillis(), run).update();
        jdbc.sql("UPDATE dead_letter SET dead_at = dead_at - ? * interval '1 millisecond' WHERE run_id = ?")
                .params(by.toMillis(), run).update();
    }

    private static void pause(String tenant, boolean paused) {
        jdbc.sql("UPDATE tenant SET paused = ? WHERE id = ?").params(paused, tenant).update();
    }

    private static Instant dbNow() {
        return jdbc.sql("SELECT clock_timestamp()").query(OffsetDateTime.class).single().toInstant();
    }

    private static Timer fireLag() {
        return node.metrics().registry().find("scheduler.fire.lag").tag("handler", "ROSTER_GEN").timer();
    }

    private static void awaitState(long run, String state) {
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(50))
                .until(() -> state.equals(fx.state(run)));
    }
}
