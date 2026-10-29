package com.moveinsync.scheduler.app.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.support.EmbeddedPostgresSupport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

/** {@link IdempotencyPurge}: bounded batches, keeps fresh records, safe on several nodes at once. */
class IdempotencyPurgeIT {

    private DataSource ds;
    private JdbcClient jdbc;

    @BeforeEach
    void freshDatabase() {
        ds = EmbeddedPostgresSupport.createDatabase("idempotency_purge_it");
        Flyway.configure().dataSource(ds).locations("classpath:db/migration").load().migrate();
        jdbc = JdbcClient.create(ds);
    }

    @Test
    void deletesOnlyExpiredRecordsInBoundedBatches() {
        insert("old", 2_500, Duration.ofHours(25));
        insert("edge", 10, Duration.ofHours(23).plusMinutes(59)); // still honoured
        insert("new", 40, Duration.ZERO);
        IdempotencyPurge purge = new IdempotencyPurge(ds, Duration.ofHours(24), Duration.ofMinutes(10), 1_000, 100);

        assertThat(purge.purgeBatch()).isEqualTo(1_000); // one statement never deletes more than a batch
        assertThat(count("old")).isEqualTo(1_500);
        assertThat(purge.purgeOnce()).isEqualTo(1_500);
        assertThat(count("old")).isZero();
        assertThat(count("edge")).isEqualTo(10);
        assertThat(count("new")).isEqualTo(40);
        assertThat(purge.purgeOnce()).isZero();
    }

    @Test
    void maxBatchesBoundsOnePass() {
        insert("old", 350, Duration.ofDays(2));
        IdempotencyPurge purge = new IdempotencyPurge(ds, Duration.ofHours(24), Duration.ofMinutes(10), 100, 2);
        assertThat(purge.purgeOnce()).isEqualTo(200);
        assertThat(purge.purgeOnce()).isEqualTo(150);
    }

    @Test
    void threeNodesPurgingAtOnceDeleteEachRecordOnceWithoutErrors() throws Exception {
        insert("old", 6_000, Duration.ofHours(30));
        insert("new", 100, Duration.ofMinutes(5));
        int nodes = 3;
        CyclicBarrier start = new CyclicBarrier(nodes);
        ExecutorService pool = Executors.newFixedThreadPool(nodes);
        try {
            List<Future<Long>> results = new ArrayList<>();
            for (int i = 0; i < nodes; i++) {
                IdempotencyPurge purge = new IdempotencyPurge(ds, Duration.ofHours(24), Duration.ofMinutes(10), 200,
                        1_000);
                results.add(pool.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    return purge.purgeOnce();
                }));
            }
            long deleted = 0;
            for (Future<Long> f : results) {
                deleted += f.get(60, TimeUnit.SECONDS);
            }
            assertThat(deleted).isEqualTo(6_000);
            assertThat(count("old")).isZero();
            assertThat(count("new")).isEqualTo(100);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void scheduledLifecycleStartsAndStops() {
        IdempotencyPurge purge = new IdempotencyPurge(ds, Duration.ofHours(24), Duration.ofMillis(50), 100, 10);
        insert("old", 10, Duration.ofHours(25));
        purge.start();
        try {
            assertThat(purge.isRunning()).isTrue();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> count("old") == 0);
        } finally {
            purge.stop();
        }
        assertThat(purge.isRunning()).isFalse();
    }

    private void insert(String prefix, int n, Duration age) {
        jdbc.sql("""
                INSERT INTO api_idempotency (tenant_id, idem_key, request_hash, response, created_at)
                SELECT 'tenant-purge', ? || '-' || g, 'h', '{"status":201}'::jsonb,
                       clock_timestamp() - ? * interval '1 millisecond'
                  FROM generate_series(1, ?) g""").params(prefix, age.toMillis(), n).update();
    }

    private long count(String prefix) {
        return jdbc.sql("SELECT count(*) FROM api_idempotency WHERE idem_key LIKE ? || '-%'").param(prefix)
                .query(Long.class).single();
    }
}
