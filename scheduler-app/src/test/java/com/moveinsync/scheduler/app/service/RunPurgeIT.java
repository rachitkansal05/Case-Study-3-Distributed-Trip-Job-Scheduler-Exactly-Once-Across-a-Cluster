package com.moveinsync.scheduler.app.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * {@link RunPurge} (plan section 17): deletes only finished runs past the retention, never a run
 * that is still due, running or dead-lettered, takes a replayed dead letter's row with its run,
 * works in bounded batches, and is safe on several nodes at once.
 */
class RunPurgeIT {

    private static final Duration RETENTION = Duration.ofDays(7);
    private static final Duration OLD = Duration.ofDays(8);

    private EngineTestDb db;
    private JdbcClient jdbc;
    private UUID job;
    /** Keeps scheduled_at distinct across inserts (UNIQUE (job_id, scheduled_at)). */
    private long nextOffsetMs;

    @BeforeEach
    void freshDatabase() {
        db = EngineTestDb.create("run_purge_it");
        jdbc = db.jdbc();
        Fixtures fx = new Fixtures(jdbc);
        fx.tenant("purge", 2);
        job = fx.job("purge").insert();
    }

    @AfterEach
    void close() {
        db.close();
    }

    private RunPurge purge(int batchSize, int maxBatches) {
        return new RunPurge(db.admin(), RETENTION, Duration.ofMinutes(10), batchSize, maxBatches);
    }

    @Test
    void deletesOnlyFinishedRunsPastTheRetentionInBoundedBatches() {
        insert("succeeded", "SUCCEEDED", 2_480, OLD);
        insert("skipped", "SKIPPED", 10, OLD);
        insert("cancelled", "CANCELLED", 10, OLD);
        insert("edge", "SUCCEEDED", 10, RETENTION.minusMinutes(1)); // still kept
        insert("fresh", "SUCCEEDED", 10, Duration.ZERO);
        insert("scheduled", "SCHEDULED", 5, OLD);
        insert("retrying", "RETRY_WAIT", 5, OLD);
        insert("paused", "PAUSED", 5, OLD);
        long running = insertRunning("running", OLD);
        insert("dead", "DEAD_LETTERED", 5, OLD);
        deadLetters("dead", false);
        RunPurge purge = purge(1_000, 100);

        assertThat(purge.purgeBatch()).isEqualTo(1_000); // one statement never deletes more than a batch
        assertThat(purge.purgeOnce()).isEqualTo(1_500);
        assertThat(count("succeeded") + count("skipped") + count("cancelled")).isZero();
        assertThat(count("edge")).isEqualTo(10);
        assertThat(count("fresh")).isEqualTo(10);
        assertThat(count("scheduled") + count("retrying") + count("paused")).isEqualTo(15);
        assertThat(jdbc.sql("SELECT state FROM job_run WHERE id = ?").param(running).query(String.class).single())
                .isEqualTo("RUNNING");
        // dead letters stay replayable
        assertThat(count("dead")).isEqualTo(5);
        assertThat(deadLetterCount("dead")).isEqualTo(5);
        assertThat(purge.purgeOnce()).isZero();
    }

    @Test
    void replayedDeadLetterGoesWithItsFinishedRunAndAnUnreplayedOneKeepsItsRun() {
        insert("replayed", "SUCCEEDED", 3, OLD);
        deadLetters("replayed", true);
        // cannot happen through the engine (only a replay leaves DEAD_LETTERED); kept to be safe
        insert("odd", "SUCCEEDED", 2, OLD);
        deadLetters("odd", false);
        insert("replayed_fresh", "SUCCEEDED", 2, Duration.ofDays(1));
        deadLetters("replayed_fresh", true);

        assertThat(purge(1_000, 10).purgeOnce()).isEqualTo(3);
        assertThat(count("replayed")).isZero();
        assertThat(count("odd")).isEqualTo(2);
        assertThat(deadLetterCount("odd")).isEqualTo(2);
        assertThat(count("replayed_fresh")).isEqualTo(2);
        assertThat(deadLetterCount("replayed_fresh")).isEqualTo(2);
        assertThat(allDeadLetters()).as("the replayed rows were deleted, not left behind").isEqualTo(4);
    }

    @Test
    void maxBatchesBoundsOnePass() {
        insert("old", "SUCCEEDED", 350, OLD);
        RunPurge purge = purge(100, 2);
        assertThat(purge.purgeOnce()).isEqualTo(200);
        assertThat(purge.purgeOnce()).isEqualTo(150);
    }

    @Test
    void threeNodesPurgingAtOnceDeleteEachRunOnceWithoutErrors() throws Exception {
        insert("old", "SUCCEEDED", 5_000, OLD);
        insert("replayed", "CANCELLED", 300, OLD);
        deadLetters("replayed", true);
        insert("fresh", "SUCCEEDED", 100, Duration.ofHours(1));
        int nodes = 3;
        CyclicBarrier start = new CyclicBarrier(nodes);
        ExecutorService pool = Executors.newFixedThreadPool(nodes);
        try {
            List<Future<Long>> results = new ArrayList<>();
            for (int i = 0; i < nodes; i++) {
                RunPurge purge = purge(200, 1_000);
                results.add(pool.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    return purge.purgeOnce();
                }));
            }
            long deleted = 0;
            for (Future<Long> f : results) {
                deleted += f.get(60, TimeUnit.SECONDS);
            }
            assertThat(deleted).isEqualTo(5_300);
            assertThat(count("old") + count("replayed")).isZero();
            assertThat(allDeadLetters()).isZero();
            assertThat(count("fresh")).isEqualTo(100);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void scheduledLifecycleStartsAndStops() {
        RunPurge purge = new RunPurge(db.admin(), RETENTION, Duration.ofMillis(50), 100, 10);
        insert("old", "SKIPPED", 10, OLD);
        purge.start();
        try {
            assertThat(purge.isRunning()).isTrue();
            await().atMost(Duration.ofSeconds(10)).until(() -> count("old") == 0);
        } finally {
            purge.stop();
        }
        assertThat(purge.isRunning()).isFalse();
    }

    // ------------------------------------------------------------------ helpers

    /** {@code n} runs in {@code state}, finished (terminal states only) {@code age} ago. */
    private void insert(String prefix, String state, int n, Duration age) {
        jdbc.sql("""
                INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                     scheduled_at, due_at, idem_key, state, attempt, started_at, finished_at)
                SELECT j.id, j.tenant_id, j.partition_id, j.priority, j.version, j.max_run_time,
                       t.at, t.at, ? || '-' || g, ?, 1, t.at,
                       CASE WHEN ? IN ('SUCCEEDED', 'SKIPPED', 'CANCELLED', 'DEAD_LETTERED') THEN t.at END
                  FROM job j
                 CROSS JOIN generate_series(1, ?) g
                 CROSS JOIN LATERAL (SELECT clock_timestamp() - ? * interval '1 millisecond'
                                            - (? + g) * interval '1 millisecond' AS at) t
                 WHERE j.id = ?""")
                .params(prefix, state, state, n, age.toMillis(), nextOffsetMs, job).update();
        nextOffsetMs += n;
    }

    /** One RUNNING run in slot 0 with a live lease (and, oddly, an old finished_at). */
    private long insertRunning(String prefix, Duration age) {
        insert(prefix, "SCHEDULED", 1, age);
        return jdbc.sql("""
                UPDATE job_run SET state = 'RUNNING', slot_no = 0, fencing_epoch = 0, fencing_seq = nextval('fence_seq'),
                       owner_node = 'n', lease_until = clock_timestamp() + interval '1 minute',
                       finished_at = clock_timestamp() - ? * interval '1 millisecond'
                 WHERE idem_key LIKE ? || '-%' RETURNING id""")
                .params(age.toMillis(), prefix).query(Long.class).single();
    }

    private void deadLetters(String prefix, boolean replayed) {
        jdbc.sql("""
                INSERT INTO dead_letter (run_id, tenant_id, error_code, attempts, dead_at, replayed_at)
                SELECT id, tenant_id, 'IT_FAILED', 1, started_at, CASE WHEN ? THEN finished_at END
                  FROM job_run WHERE idem_key LIKE ? || '-%'""")
                .params(replayed, prefix).update();
    }

    private long count(String prefix) {
        return jdbc.sql("SELECT count(*) FROM job_run WHERE idem_key LIKE ? || '-%'").param(prefix)
                .query(Long.class).single();
    }

    private long deadLetterCount(String prefix) {
        return jdbc.sql("""
                SELECT count(*) FROM dead_letter d JOIN job_run r ON r.id = d.run_id
                 WHERE r.idem_key LIKE ? || '-%'""")
                .param(prefix).query(Long.class).single();
    }

    private long allDeadLetters() {
        return jdbc.sql("SELECT count(*) FROM dead_letter").query(Long.class).single();
    }
}
