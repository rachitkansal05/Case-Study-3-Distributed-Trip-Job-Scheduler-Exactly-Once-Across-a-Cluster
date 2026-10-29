package com.moveinsync.scheduler.app.repository.claim;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.TenantId;
import com.moveinsync.scheduler.core.policy.ExponentialFullJitterBackoff;
import com.moveinsync.scheduler.core.policy.FailureClassifier;
import com.moveinsync.scheduler.core.policy.RetryDecider;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * R4 / plan section 21 issue 3: an expired lease can never be renewed, checkpointed or committed,
 * even when nobody has taken the run over yet; the lease-lost limit dead-letters with
 * FENCING_CONFLICT.
 */
class LeaseResurrectionIT {

    private static EngineTestDb db;
    private static Fixtures fx;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("lease_resurrection_it");
        fx = new Fixtures(db.jdbc());
    }

    @AfterAll
    static void close() {
        db.close();
    }

    private static JdbcClaimProtocol protocol(int leaseLostLimit, Duration lease) {
        return protocol(leaseLostLimit, lease, ClaimObserver.NONE);
    }

    private static JdbcClaimProtocol protocol(int leaseLostLimit, Duration lease, ClaimObserver observer) {
        return new JdbcClaimProtocol(db.admin(), lease,
                new RetryDecider(ExponentialFullJitterBackoff.withDefaults(), new FailureClassifier(), leaseLostLimit),
                observer);
    }

    @Test
    void expiredUntakenLeaseCannotBeResurrected() {
        JdbcClaimProtocol protocol = protocol(20, Duration.ofSeconds(10));
        fx.tenant("res-a", 1);
        UUID job = fx.job("res-a").insert();
        long runId = fx.dueRuns(job, 1).get(0);
        ClaimedRun run = protocol.claimDue("n1", TenantId.of("res-a"), Fixtures.ALL_PARTITIONS, 1).get(0);

        fx.expireLease(runId);
        assertThat(protocol.renew(runId, run.token())).isFalse();
        assertThat(protocol.checkpoint(runId, run.token(), "{\"nextItem\":50}")).isFalse();
        assertThat(protocol.commitSuccess(runId, run.token())).isFalse();
        // nobody took it over: still RUNNING, still expired, checkpoint untouched
        assertThat(fx.state(runId)).isEqualTo("RUNNING");
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE id = ? AND lease_until < clock_timestamp() "
                + "AND checkpoint IS NULL", runId)).isEqualTo(1);
    }

    @Test
    void naturallyExpiredLeaseCannotBeRenewed() throws Exception {
        JdbcClaimProtocol protocol = protocol(20, Duration.ofMillis(300));
        fx.tenant("res-n", 1);
        UUID job = fx.job("res-n").insert();
        long runId = fx.dueRuns(job, 1).get(0);
        ClaimedRun run = protocol.claimDue("n1", TenantId.of("res-n"), Fixtures.ALL_PARTITIONS, 1).get(0);
        assertThat(protocol.renew(runId, run.token())).isTrue();
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() ->
                fx.count("SELECT count(*) FROM job_run WHERE id = ? AND lease_until < clock_timestamp()", runId) == 1);
        assertThat(protocol.renew(runId, run.token())).isFalse();
    }

    @Test
    void validLeaseIsNotReapedBySweep() {
        JdbcClaimProtocol protocol = protocol(20, Duration.ofSeconds(10));
        fx.tenant("res-v", 1);
        UUID job = fx.job("res-v").insert();
        long runId = fx.dueRuns(job, 1).get(0);
        ClaimedRun run = protocol.claimDue("n1", TenantId.of("res-v"), Fixtures.ALL_PARTITIONS, 1).get(0);
        assertThat(protocol.sweepOrphans("n2", 100)).noneMatch(r -> r.runId() == runId);
        assertThat(protocol.renew(runId, run.token())).isTrue();
    }

    @Test
    void leaseLostLimitDeadLettersWithFencingConflict() {
        com.moveinsync.scheduler.app.engine.EngineMetrics metrics = new com.moveinsync.scheduler.app.engine.EngineMetrics(
                new io.micrometer.core.instrument.simple.SimpleMeterRegistry(), "res-l-node");
        JdbcClaimProtocol protocol = protocol(3, Duration.ofSeconds(10), metrics);
        assertThat(metrics.deadLetterCount("FENCING_CONFLICT")).as("exported before the first event").isZero();
        fx.tenant("res-l", 1);
        UUID job = fx.job("res-l").insert();
        long runId = fx.dueRuns(job, 1).get(0);
        protocol.claimDue("n1", TenantId.of("res-l"), Fixtures.ALL_PARTITIONS, 1);
        for (int loss = 1; loss <= 2; loss++) {
            fx.expireLease(runId);
            List<ClaimedRun> taken = protocol.sweepOrphans("n" + (loss + 1), 100).stream()
                    .filter(r -> r.runId() == runId).toList();
            assertThat(taken).hasSize(1);
            assertThat(taken.get(0).attempt()).isEqualTo(1);
            assertThat(fx.intColumn(runId, "lease_lost_count")).isEqualTo(loss);
        }
        fx.expireLease(runId);
        assertThat(protocol.sweepOrphans("n9", 100)).noneMatch(r -> r.runId() == runId);
        assertThat(fx.state(runId)).isEqualTo("DEAD_LETTERED");
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE id = ? AND slot_no IS NULL "
                + "AND last_error LIKE 'FENCING_CONFLICT: %'", runId)).isEqualTo(1);
        assertThat(fx.count("SELECT count(*) FROM dead_letter WHERE run_id = ? AND error_code = 'FENCING_CONFLICT'",
                runId)).isEqualTo(1);
        assertThat(metrics.deadLetterCount("FENCING_CONFLICT")).isEqualTo(1.0);
    }
}
