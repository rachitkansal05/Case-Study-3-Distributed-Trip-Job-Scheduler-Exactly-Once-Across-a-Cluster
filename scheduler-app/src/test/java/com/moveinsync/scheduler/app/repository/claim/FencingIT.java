package com.moveinsync.scheduler.app.repository.claim;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.RunFailure;
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
 * R4: a claim gets token t; after the lease expires another node takes over with a higher token;
 * the stale holder's renew, checkpoint and commit update 0 rows, and the sink rejects its token.
 */
class FencingIT {

    private static EngineTestDb db;
    private static Fixtures fx;
    private static JdbcClaimProtocol protocol;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("fencing_it");
        fx = new Fixtures(db.jdbc());
        protocol = new JdbcClaimProtocol(db.admin(), Duration.ofSeconds(10),
                new RetryDecider(ExponentialFullJitterBackoff.withDefaults(), new FailureClassifier()),
                ClaimObserver.NONE);
    }

    @AfterAll
    static void close() {
        db.close();
    }

    private ClaimedRun claimOne(String tenant, String node) {
        List<ClaimedRun> runs = protocol.claimDue(node, TenantId.of(tenant), Fixtures.ALL_PARTITIONS, 1);
        assertThat(runs).hasSize(1);
        return runs.get(0);
    }

    @Test
    void staleHolderIsFencedInDbAndSink() {
        fx.tenant("fence-a", 2);
        UUID job = fx.job("fence-a").insert();
        long runId = fx.dueRuns(job, 1).get(0);

        ClaimedRun first = claimOne("fence-a", "node-a");
        assertThat(first.runId()).isEqualTo(runId);
        assertThat(first.attempt()).isEqualTo(1);
        assertThat(first.token().epoch()).isEqualTo(1L); // generation 0, timeline 1
        FakeEffectSink sink = new FakeEffectSink();
        assertThat(sink.send(runId, first.idemKey() + ":e1", first.token(), "{}")).isEqualTo(DeliveryOutcome.DELIVERED);
        assertThat(protocol.renew(runId, first.token())).isTrue();

        fx.expireLease(runId);
        // the sweep is global: other tests' orphans may be taken too; look at ours
        ClaimedRun second = protocol.sweepOrphans("node-b", 100).stream()
                .filter(r -> r.runId() == runId).findFirst().orElseThrow();
        assertThat(second.recovered()).isTrue();
        assertThat(second.slotNo()).isEqualTo(first.slotNo());
        assertThat(second.token().isNewerThan(first.token())).isTrue();
        assertThat(second.attempt()).isEqualTo(1); // lease loss does not use up an attempt
        assertThat(fx.intColumn(runId, "lease_lost_count")).isEqualTo(1);

        // the zombie: every fenced write updates 0 rows
        assertThat(protocol.renew(runId, first.token())).isFalse();
        assertThat(protocol.checkpoint(runId, first.token(), "{\"nextItem\":1}")).isFalse();
        assertThat(protocol.commitSuccess(runId, first.token())).isFalse();
        assertThat(protocol.commitFailure(runId, first.token(),
                new RunFailure.Retry(Duration.ZERO, "X", "x"))).isFalse();
        assertThat(fx.state(runId)).isEqualTo("RUNNING");

        // the sink: the new token raises the fence, the old one is rejected even for a delivered key
        assertThat(sink.send(runId, first.idemKey() + ":e1", second.token(), "{}"))
                .isEqualTo(DeliveryOutcome.ALREADY_DELIVERED);
        assertThat(sink.send(runId, first.idemKey() + ":e2", first.token(), "{}"))
                .isEqualTo(DeliveryOutcome.STALE_TOKEN);
        assertThat(sink.send(runId, first.idemKey() + ":e1", first.token(), "{}"))
                .isEqualTo(DeliveryOutcome.STALE_TOKEN);

        assertThat(protocol.commitSuccess(runId, second.token())).isTrue();
        assertThat(fx.state(runId)).isEqualTo("SUCCEEDED");
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE id = ? AND slot_no IS NULL", runId)).isEqualTo(1);
    }

    @Test
    void claimReapsExpiredHolderToReuseItsSlot() {
        fx.tenant("fence-b", 1);
        UUID job = fx.job("fence-b").insert();
        long runId = fx.dueRuns(job, 1).get(0);
        ClaimedRun first = claimOne("fence-b", "node-a");
        long waiting = fx.dueRuns(job, 1).get(0);
        assertThat(protocol.claimDue("node-b", TenantId.of("fence-b"), Fixtures.ALL_PARTITIONS, 1)).isEmpty();
        fx.expireLease(runId);
        // the claim reaps the expired holder of the only slot and starts the longest-waiting due run
        ClaimedRun second = claimOne("fence-b", "node-b");
        assertThat(second.runId()).isEqualTo(waiting);
        assertThat(second.slotNo()).isEqualTo(first.slotNo());
        assertThat(fx.state(runId)).isEqualTo("RETRY_WAIT");
        assertThat(fx.intColumn(runId, "lease_lost_count")).isEqualTo(1);
        assertThat(fx.intColumn(runId, "attempt")).isZero();
        assertThat(protocol.commitSuccess(runId, first.token())).isFalse();
        assertThat(protocol.commitSuccess(waiting, second.token())).isTrue();
        // the reaped run is due again and gets a newer token than its first claim
        ClaimedRun third = claimOne("fence-b", "node-c");
        assertThat(third.runId()).isEqualTo(runId);
        assertThat(third.attempt()).isEqualTo(1);
        assertThat(third.token().isNewerThan(first.token())).isTrue();
        assertThat(protocol.commitSuccess(runId, third.token())).isTrue();
    }

    @Test
    void epochIncludesClusterGenerationSoRebuiltClusterTokensWin() {
        fx.tenant("fence-c", 1);
        UUID job = fx.job("fence-c").insert();
        long a = fx.dueRuns(job, 1).get(0);
        ClaimedRun before = claimOne("fence-c", "n");
        assertThat(protocol.commitSuccess(a, before.token())).isTrue();
        db.jdbc().sql("UPDATE cluster_meta SET cluster_generation = 1").update();
        try {
            fx.dueRuns(job, 1);
            ClaimedRun after = claimOne("fence-c", "n");
            assertThat(after.token().epoch()).isEqualTo((1L << 32) + 1);
            assertThat(after.token().isNewerThan(before.token())).isTrue();
            assertThat(protocol.commitSuccess(after.runId(), after.token())).isTrue();
        } finally {
            db.jdbc().sql("UPDATE cluster_meta SET cluster_generation = 0").update();
        }
    }

    @Test
    void epochParsingFailsClosed() {
        assertThat(FencingEpochReader.epochOf(0, "000000070000000000000003")).isEqualTo(7L);
        assertThat(FencingEpochReader.epochOf(2, "0000ABCD0000000000000003")).isEqualTo((2L << 32) + 0xABCD);
        for (String bad : new String[] {null, "", "0000", "00000000AAAA", "zzzzzzzz0000"}) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> FencingEpochReader.epochOf(0, bad))
                    .isInstanceOf(FencingEpochReader.EpochUnavailableException.class);
        }
    }

    @Test
    void pausedTenantIsSkippedInTheClaimItself() {
        fx.tenant("fence-p", 2);
        UUID job = fx.job("fence-p").insert();
        fx.dueRuns(job, 2);
        db.jdbc().sql("UPDATE tenant SET paused = true WHERE id = 'fence-p'").update();
        assertThat(protocol.claimDue("n", TenantId.of("fence-p"), Fixtures.ALL_PARTITIONS, 2)).isEmpty();
        db.jdbc().sql("UPDATE tenant SET paused = false WHERE id = 'fence-p'").update();
        assertThat(protocol.claimDue("n", TenantId.of("fence-p"), Fixtures.ALL_PARTITIONS, 2)).hasSize(2);
    }

    @Test
    void commitFailureRetryAndDeadLetterFreeTheSlot() {
        fx.tenant("fence-f", 1);
        UUID job = fx.job("fence-f").insert();
        long runId = fx.dueRuns(job, 1).get(0);
        ClaimedRun c1 = claimOne("fence-f", "n");
        assertThat(protocol.commitFailure(runId, c1.token(),
                new RunFailure.Retry(Duration.ofHours(1), "SINK_5XX", "sink answered 503"))).isTrue();
        assertThat(fx.state(runId)).isEqualTo("RETRY_WAIT");
        assertThat(db.jdbc().sql("SELECT last_error FROM job_run WHERE id = ?").param(runId).query(String.class)
                .single()).isEqualTo("SINK_5XX: sink answered 503");
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE id = ? AND slot_no IS NULL "
                + "AND due_at > clock_timestamp() + interval '59 minutes'", runId)).isEqualTo(1);
        // not due for an hour: nothing to claim
        assertThat(protocol.claimDue("n", TenantId.of("fence-f"), Fixtures.ALL_PARTITIONS, 1)).isEmpty();

        db.jdbc().sql("UPDATE job_run SET due_at = clock_timestamp() WHERE id = ?").param(runId).update();
        ClaimedRun c2 = claimOne("fence-f", "n");
        assertThat(c2.attempt()).isEqualTo(2);
        assertThat(protocol.commitFailure(runId, c2.token(),
                new RunFailure.DeadLetter("INVALID_PAYLOAD", "payload could not be parsed"))).isTrue();
        assertThat(fx.state(runId)).isEqualTo("DEAD_LETTERED");
        assertThat(fx.count("SELECT count(*) FROM dead_letter WHERE run_id = ? AND error_code = 'INVALID_PAYLOAD' "
                + "AND attempts = 2 AND replayed_at IS NULL", runId)).isEqualTo(1);
    }
}
