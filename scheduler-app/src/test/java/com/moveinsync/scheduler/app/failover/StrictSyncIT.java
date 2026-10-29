package com.moveinsync.scheduler.app.failover;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.SchedulerNode;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.app.engine.support.TestCluster;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.TenantId;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * R4 failure injection, plan section 12, F2: with no synchronous standby connected, the primary
 * does not fall back to asynchronous replication; commits block, so no scheduler claim (and so no
 * fencing token) can be acknowledged.
 *
 * <p>This reproduces F2 with plain Postgres {@code synchronous_standby_names = 'FIRST 1
 * (standby1)'}: when that standby is gone, every commit waits. Production gets the same behaviour
 * from Patroni's {@code synchronous_mode: true} + {@code synchronous_mode_strict: true} (see
 * {@code deploy/patroni/}), which keeps the setting in place even when no standby is left.
 *
 * <p>What is asserted is the client side: the commit is not acknowledged. In Postgres a commit
 * waiting for the synchronous standby is already written locally and can be visible to other
 * sessions, so the test does not assert invisibility.
 *
 * <p>Docker-gated: skipped when no Docker daemon is available.
 */
@Testcontainers(disabledWithoutDocker = true)
class StrictSyncIT {

    private static final String TENANT = "strict_t";
    private static final long BLOCKED_FOR_MS = 5_000;

    @Test
    void withoutSynchronousStandbyCommitsAndClaimsAreNotAcknowledged() throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(2);
        try (PgReplicationPair pair = PgReplicationPair.start();
             HikariDataSource pool = startPair(pair)) {
            Fixtures fx = new Fixtures(JdbcClient.create(pool));
            fx.tenant(TENANT, 2);
            UUID job = fx.job(TENANT).insert();
            assertThat(fx.dueRuns(job, 1)).hasSize(1); // due, slot free: claimable

            // a node's claim protocol, not started (no loops): we call claimDue ourselves
            SchedulerNode node = new SchedulerNode(TestCluster.fast("strict1").build(), pool, new FakeEffectSink(),
                    HandlerRegistry.defaults(new ObjectMapper()), new SimpleMeterRegistry());

            // ---- the synchronous standby disappears --------------------------------------------
            pair.stopStandby();
            await("standby disconnected").atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                    .until(() -> pair.connectedStandbys() == 0);

            // F2: a plain write is not acknowledged
            Future<Integer> write = exec.submit(() -> {
                try (Connection c = pair.primaryConnection(); Statement st = c.createStatement()) {
                    return st.executeUpdate(
                            "INSERT INTO tenant (id, time_zone, configured_cap) VALUES ('strict_t2', 'UTC', 1)");
                }
            });
            assertThatThrownBy(() -> write.get(BLOCKED_FOR_MS, TimeUnit.MILLISECONDS))
                    .as("commit must block without a synchronous standby")
                    .isInstanceOf(TimeoutException.class);

            // F2 for the scheduler: a claim on a due run cannot be acknowledged, so no token is handed out
            Future<List<ClaimedRun>> claim = exec.submit(() ->
                    node.claimProtocol().claimDue("strict1", TenantId.of(TENANT), Fixtures.ALL_PARTITIONS, 1));
            assertThatThrownBy(() -> claim.get(BLOCKED_FOR_MS, TimeUnit.MILLISECONDS))
                    .as("claim commit must block without a synchronous standby")
                    .isInstanceOf(TimeoutException.class);

            // both backends are waiting for the synchronous standby
            await("backends wait in SyncRep").atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(200))
                    .until(() -> syncRepWaiters(pair) >= 2);

            // cleanup: end the waiting backends; their clients get an error (or, at worst, return)
            try (Connection c = pair.primaryConnection(); Statement st = c.createStatement()) {
                st.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE wait_event = 'SyncRep'");
            }
            awaitDone(write);
            awaitDone(claim);
        } finally {
            exec.shutdownNow();
            exec.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    /** Migrates the primary and returns a pool on it (closed by the caller). */
    private static HikariDataSource startPair(PgReplicationPair pair) throws Exception {
        assertThat(pair.syncState()).isEqualTo("sync");
        pair.migratePrimary();
        return pair.pool("strict", pair.primaryJdbcUrl(), 4);
    }

    private static long syncRepWaiters(PgReplicationPair pair) throws Exception {
        try (Connection c = pair.primaryConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'SyncRep'")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static void awaitDone(Future<?> f) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100)).until(f::isDone);
    }
}
