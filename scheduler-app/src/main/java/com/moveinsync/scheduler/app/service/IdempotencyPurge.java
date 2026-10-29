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
 * Deletes {@code api_idempotency} records older than {@link IdempotencyService#RETENTION} (24 h).
 * They are no longer honoured ({@code find} ignores them), so without this the table only grows.
 *
 * <ul>
 *   <li>Every 10 minutes by default ({@code scheduler.api.idempotency-purge.interval}), first run
 *       after a random delay of up to one interval so nodes do not all start at once.
 *   <li>Bounded batches ({@code batch-size}, default 1,000 rows per statement and transaction), so
 *       no long transaction and no large lock set; a pass stops after {@code max-batches}.
 *   <li>Safe on every node at once: each batch picks its rows with {@code FOR UPDATE SKIP LOCKED},
 *       so concurrent purges delete disjoint rows and never wait on each other, and the outer
 *       {@code DELETE} re-checks the age, so a record a client re-saved (fresh {@code created_at})
 *       in the meantime is never deleted.
 * </ul>
 */
@Component
public class IdempotencyPurge implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyPurge.class);

    /* Uses the api_idempotency_created index; the age is re-checked on the locked row version. */
    private static final String DELETE_BATCH = """
            DELETE FROM api_idempotency a
             USING (SELECT tenant_id, idem_key FROM api_idempotency
                     WHERE created_at < clock_timestamp() - :retentionMs * interval '1 millisecond'
                     ORDER BY created_at
                     LIMIT :batch
                       FOR UPDATE SKIP LOCKED) old
             WHERE a.tenant_id = old.tenant_id AND a.idem_key = old.idem_key
               AND a.created_at < clock_timestamp() - :retentionMs * interval '1 millisecond'
            """;

    private final JdbcClient jdbc;
    private final Duration retention;
    private final Duration interval;
    private final int batchSize;
    private final int maxBatches;
    private volatile ScheduledExecutorService executor;

    @Autowired
    public IdempotencyPurge(DataSource dataSource,
                            @Value("${scheduler.api.idempotency-purge.interval:10m}") Duration interval,
                            @Value("${scheduler.api.idempotency-purge.batch-size:1000}") int batchSize,
                            @Value("${scheduler.api.idempotency-purge.max-batches:100}") int maxBatches) {
        this(dataSource, IdempotencyService.RETENTION, interval, batchSize, maxBatches);
    }

    public IdempotencyPurge(DataSource dataSource, Duration retention, Duration interval, int batchSize,
                            int maxBatches) {
        this.jdbc = JdbcClient.create(Objects.requireNonNull(dataSource, "dataSource"));
        this.retention = Objects.requireNonNull(retention, "retention");
        this.interval = Objects.requireNonNull(interval, "interval");
        if (batchSize < 1 || maxBatches < 1 || interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("batch size, max batches and interval must be positive");
        }
        this.batchSize = batchSize;
        this.maxBatches = maxBatches;
    }

    /** One batch: deletes at most {@code batchSize} expired records; returns how many. */
    public int purgeBatch() {
        return jdbc.sql(DELETE_BATCH)
                .param("retentionMs", retention.toMillis())
                .param("batch", batchSize)
                .update();
    }

    /** One pass: batches until a batch comes back short, or {@code maxBatches}; returns rows deleted. */
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
                log.info("idempotency purge deleted {} records older than {}", n, retention);
            }
        } catch (RuntimeException e) {
            log.warn("idempotency purge failed, next try in {}: {}", interval, e.toString());
        }
    }

    @Override
    public synchronized void start() {
        if (executor != null) {
            return;
        }
        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("idempotency-purge").factory());
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
