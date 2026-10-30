package com.moveinsync.scheduler.app.service;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * Run retention (plan section 17): deletes finished {@code job_run} rows ({@code SUCCEEDED},
 * {@code SKIPPED}, {@code CANCELLED}) whose {@code finished_at} is older than the retention (7 days
 * by default, {@code scheduler.run-purge.retention}), so the table does not grow without bound.
 * Built like {@link IdempotencyPurge}.
 *
 * <ul>
 *   <li><b>Never deleted:</b> runs that are not finished ({@code SCHEDULED}, {@code PAUSED},
 *       {@code RUNNING}, {@code RETRY_WAIT}), and {@code DEAD_LETTERED} runs, which stay
 *       replayable for as long as nobody replays them (each has an un-replayed {@code dead_letter}
 *       row; a run with such a row is skipped whatever its state). A replayed dead letter's row is
 *       deleted together with its run once the replayed run finished longer ago than the retention
 *       ({@code dead_letter.run_id} references the run). Terminal runs hold no slot, so the
 *       {@code tenant_slot} foreign key is not involved.
 *   <li>Every 10 minutes by default ({@code interval}), first run after a random delay of up to one
 *       interval so nodes do not all start at once.
 *   <li>Bounded batches ({@code batch-size}, default 1,000 runs per statement and transaction,
 *       oldest first through the partial index {@code run_finished}); a pass stops after {@code
 *       max-batches}.
 *   <li>Safe on every node at once: each batch locks its runs with {@code FOR UPDATE SKIP LOCKED},
 *       so concurrent purges delete disjoint rows and never wait on each other or on the engine. A
 *       locked terminal run cannot change state (replay needs {@code DEAD_LETTERED}), so the
 *       batch's dead-letter and run deletes see the rows they selected.
 * </ul>
 *
 * Effects on the API: a purged run no longer appears in the run history, and a manual trigger
 * repeated with the same {@code Idempotency-Key} after the retention creates a new run (API keys
 * are kept 24 h, far less than the retention).
 */
@Component
public class RunPurge implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(RunPurge.class);

    /* One statement, one transaction. old: the oldest finished runs past the retention, locked
       without waiting; runs with an un-replayed dead letter are left alone. replayed: their (replayed)
       dead_letter rows, deleted in the same statement so the foreign key holds at its end. */
    private static final String DELETE_BATCH = """
            WITH old AS (
              SELECT r.id FROM job_run r
               WHERE r.state IN ('SUCCEEDED', 'SKIPPED', 'CANCELLED')
                 AND r.finished_at < clock_timestamp() - :retentionMs * interval '1 millisecond'
                 AND NOT EXISTS (SELECT 1 FROM dead_letter d WHERE d.run_id = r.id AND d.replayed_at IS NULL)
               ORDER BY r.finished_at
               LIMIT :batch
                 FOR UPDATE OF r SKIP LOCKED),
            replayed AS (
              DELETE FROM dead_letter d USING old WHERE d.run_id = old.id AND d.replayed_at IS NOT NULL)
            DELETE FROM job_run r USING old WHERE r.id = old.id
            """;

    private final JdbcClient jdbc;
    private final Duration retention;
    private final Duration interval;
    private final int batchSize;
    private final int maxBatches;
    private volatile ScheduledExecutorService executor;

    @Autowired
    public RunPurge(DataSource dataSource,
                    @Value("${scheduler.run-purge.retention:7d}") Duration retention,
                    @Value("${scheduler.run-purge.interval:10m}") Duration interval,
                    @Value("${scheduler.run-purge.batch-size:1000}") int batchSize,
                    @Value("${scheduler.run-purge.max-batches:100}") int maxBatches) {
        this.jdbc = JdbcClient.create(Objects.requireNonNull(dataSource, "dataSource"));
        this.retention = Objects.requireNonNull(retention, "retention");
        this.interval = Objects.requireNonNull(interval, "interval");
        if (batchSize < 1 || maxBatches < 1 || interval.isNegative() || interval.isZero()
                || retention.isNegative() || retention.isZero()) {
            throw new IllegalArgumentException("retention, batch size, max batches and interval must be positive");
        }
        this.batchSize = batchSize;
        this.maxBatches = maxBatches;
    }

    /** One batch: deletes at most {@code batchSize} expired runs; returns how many. */
    public int purgeBatch() {
        return jdbc.sql(DELETE_BATCH)
                .param("retentionMs", retention.toMillis())
                .param("batch", batchSize)
                .update();
    }

    /** One pass: batches until a batch comes back short, or {@code maxBatches}; returns runs deleted. */
    public long purgeOnce() {
        long total = 0;
        for (int i = 0; i < maxBatches; i++) {
            int n = purgeBatch();
            total += n;
            if (n < batchSize) {
                break;
            }
        }
        return total;
    }

    private void scheduledPass() {
        try {
            long n = purgeOnce();
            if (n > 0) {
                log.info("run purge deleted {} finished runs older than {}", n, retention);
            }
        } catch (RuntimeException e) {
            log.warn("run purge failed, next try in {}: {}", interval, e.toString());
        }
    }

    @Override
    public synchronized void start() {
        if (executor != null) {
            return;
        }
        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("run-purge").factory());
        long periodMs = interval.toMillis();
        ses.scheduleWithFixedDelay(this::scheduledPass, ThreadLocalRandom.current().nextLong(periodMs + 1),
                periodMs, TimeUnit.MILLISECONDS);
        executor = ses;
    }

    @Override
    public synchronized void stop() {
        ScheduledExecutorService ses = executor;
        executor = null;
        if (ses != null) {
            ses.shutdownNow();
        }
    }

    @Override
    public boolean isRunning() {
        return executor != null;
    }
}
