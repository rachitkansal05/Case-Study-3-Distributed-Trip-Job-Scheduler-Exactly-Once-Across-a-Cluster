package com.moveinsync.scheduler.app.repository.claim;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import com.moveinsync.scheduler.core.domain.TenantId;
import com.moveinsync.scheduler.core.policy.ExponentialFullJitterBackoff;
import com.moveinsync.scheduler.core.policy.FailureClassifier;
import com.moveinsync.scheduler.core.policy.RetryDecider;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * R6: after 3 h of simulated downtime an hourly job has 3 missed occurrences plus one on time.
 * The claim applies the job's misfire policy: FIRE_NOW starts all 4 in order, SKIP skips the 3
 * missed ones, COALESCE fires the newest missed one with coalesced_count = 3 and skips the others.
 */
class MisfireIT {

    private static EngineTestDb db;
    private static Fixtures fx;
    private static JdbcClaimProtocol protocol;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("misfire_it");
        fx = new Fixtures(db.jdbc());
        protocol = new JdbcClaimProtocol(db.admin(), Duration.ofSeconds(10),
                new RetryDecider(ExponentialFullJitterBackoff.withDefaults(), new FailureClassifier()),
                ClaimObserver.NONE);
    }

    @AfterAll
    static void close() {
        db.close();
    }

    private record Job(String tenant, long missed1, long missed2, long missed3, long onTime) {
    }

    /** Runs at now-3h, -2h, -1h (misfired, threshold 60 s) and now-10s (on time). */
    private Job downtime(String tenant, MisfirePolicyType policy) {
        fx.tenant(tenant, 5);
        UUID job = fx.job(tenant).misfire(policy).misfireThreshold(Duration.ofSeconds(60)).insert();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        return new Job(tenant,
                fx.run(job, now.minus(Duration.ofHours(3))),
                fx.run(job, now.minus(Duration.ofHours(2))),
                fx.run(job, now.minus(Duration.ofHours(1))),
                fx.run(job, now.minusSeconds(10)));
    }

    private List<ClaimedRun> claim(String tenant) {
        return protocol.claimDue("n", TenantId.of(tenant), Fixtures.ALL_PARTITIONS, 5);
    }

    private Map<Long, String> states(Job j) {
        return db.jdbc().sql("SELECT id, state FROM job_run WHERE tenant_id = ?").param(j.tenant())
                .query((rs, i) -> Map.entry(rs.getLong(1), rs.getString(2))).list().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Test
    void fireNowFiresEveryMissedOccurrenceInOrder() {
        Job j = downtime("mf-fire", MisfirePolicyType.FIRE_NOW);
        List<ClaimedRun> started = claim(j.tenant());
        assertThat(started).extracting(ClaimedRun::runId)
                .containsExactly(j.missed1(), j.missed2(), j.missed3(), j.onTime());
        assertThat(started).allMatch(r -> r.coalescedCount() == 1);
        assertThat(states(j).values()).containsOnly("RUNNING");
    }

    @Test
    void skipSkipsMissedAndFiresOnTime() {
        Job j = downtime("mf-skip", MisfirePolicyType.SKIP);
        List<ClaimedRun> started = claim(j.tenant());
        assertThat(started).extracting(ClaimedRun::runId).containsExactly(j.onTime());
        Map<Long, String> s = states(j);
        assertThat(s.get(j.missed1())).isEqualTo("SKIPPED");
        assertThat(s.get(j.missed2())).isEqualTo("SKIPPED");
        assertThat(s.get(j.missed3())).isEqualTo("SKIPPED");
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE tenant_id = ? AND state = 'SKIPPED' "
                + "AND finished_at IS NOT NULL AND slot_no IS NULL AND fencing_seq IS NULL", j.tenant())).isEqualTo(3);
    }

    @Test
    void coalesceFiresNewestMissedWithCount() {
        Job j = downtime("mf-coal", MisfirePolicyType.COALESCE);
        List<ClaimedRun> started = claim(j.tenant());
        assertThat(started).extracting(ClaimedRun::runId).containsExactly(j.missed3(), j.onTime());
        assertThat(started.get(0).coalescedCount()).isEqualTo(3);
        assertThat(started.get(1).coalescedCount()).isEqualTo(1);
        Map<Long, String> s = states(j);
        assertThat(s.get(j.missed1())).isEqualTo("SKIPPED");
        assertThat(s.get(j.missed2())).isEqualTo("SKIPPED");
        assertThat(fx.intColumn(j.missed3(), "coalesced_count")).isEqualTo(3);
    }

    @Test
    void skipWithOnlyMissedRunsStartsNothingAndFreesNothing() {
        fx.tenant("mf-only", 2);
        UUID job = fx.job("mf-only").misfire(MisfirePolicyType.SKIP).insert();
        Instant now = Instant.now();
        fx.run(job, now.minus(Duration.ofHours(2)));
        fx.run(job, now.minus(Duration.ofHours(1)));
        assertThat(claim("mf-only")).isEmpty();
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE tenant_id = 'mf-only' AND state = 'SKIPPED'"))
                .isEqualTo(2);
    }

    @Test
    void retriesAreNeverTreatedAsMisfires() {
        fx.tenant("mf-retry", 1);
        UUID job = fx.job("mf-retry").misfire(MisfirePolicyType.SKIP).insert();
        long run = fx.run(job, Instant.now().minus(Duration.ofHours(2)));
        db.jdbc().sql("UPDATE job_run SET state = 'RETRY_WAIT', attempt = 1, due_at = clock_timestamp() WHERE id = ?")
                .param(run).update();
        assertThat(claim("mf-retry")).extracting(ClaimedRun::runId).containsExactly(run);
    }

    // ---------------------------------------------------------------- V204: misfire = late before claimable

    /** A live node whose unbroken heartbeats started {@code aliveFor} ago (DB clock). */
    private void liveNode(String id, Duration aliveFor) {
        db.jdbc().sql("""
                INSERT INTO scheduler_node (node_id, heartbeat_at, started_at, alive_since)
                VALUES (?, clock_timestamp(), clock_timestamp() - ? * interval '1 millisecond',
                        clock_timestamp() - ? * interval '1 millisecond')""")
                .params(id, aliveFor.toMillis(), aliveFor.toMillis()).update();
    }

    private void clearNodes() {
        db.jdbc().sql("DELETE FROM scheduler_node").update();
    }

    @Test
    void runQueuedWhileTheClusterWasUpIsNotAMisfire() {
        fx.tenant("mf-queued", 5);
        UUID job = fx.job("mf-queued").misfire(MisfirePolicyType.SKIP).misfireThreshold(Duration.ofSeconds(60)).insert();
        long queued = fx.run(job, Instant.now().minus(Duration.ofMinutes(5))); // waited behind the cap
        try {
            liveNode("up-1h", Duration.ofHours(1));
            assertThat(claim("mf-queued")).extracting(ClaimedRun::runId).containsExactly(queued);
        } finally {
            clearNodes();
        }
    }

    @Test
    void runDueBeforeAClusterOutageEndedIsAMisfire() {
        fx.tenant("mf-outage", 5);
        UUID job = fx.job("mf-outage").misfire(MisfirePolicyType.SKIP).misfireThreshold(Duration.ofSeconds(60)).insert();
        long missed = fx.run(job, Instant.now().minus(Duration.ofMinutes(5)));
        long dueAfterRecovery = fx.run(job, Instant.now().minus(Duration.ofSeconds(5)));
        try {
            liveNode("back-30s", Duration.ofSeconds(30)); // the cluster came back 30 s ago
            assertThat(claim("mf-outage")).extracting(ClaimedRun::runId).containsExactly(dueAfterRecovery);
            assertThat(fx.state(missed)).isEqualTo("SKIPPED");
        } finally {
            clearNodes();
        }
    }

    @Test
    void runDueWhileTheTenantWasPausedIsAMisfire() {
        fx.tenant("mf-paused", 5);
        UUID job = fx.job("mf-paused").misfire(MisfirePolicyType.SKIP).misfireThreshold(Duration.ofSeconds(60)).insert();
        long missed = fx.run(job, Instant.now().minus(Duration.ofMinutes(5)));
        db.jdbc().sql("UPDATE tenant SET resumed_at = clock_timestamp() - interval '10 seconds' WHERE id = 'mf-paused'")
                .update();
        try {
            liveNode("up-1h", Duration.ofHours(1));
            assertThat(claim("mf-paused")).isEmpty();
            assertThat(fx.state(missed)).isEqualTo("SKIPPED");
        } finally {
            clearNodes();
        }
    }

    @Test
    void coalesceAfterAnOutageCountsOnlyRunsDueBeforeRecovery() {
        fx.tenant("mf-coal2", 5);
        UUID job = fx.job("mf-coal2").misfire(MisfirePolicyType.COALESCE).misfireThreshold(Duration.ofSeconds(60))
                .insert();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        long m1 = fx.run(job, now.minus(Duration.ofMinutes(30)));
        long m2 = fx.run(job, now.minus(Duration.ofMinutes(20)));
        long queued = fx.run(job, now.minus(Duration.ofMinutes(2))); // due after recovery, then queued
        try {
            liveNode("back-5m", Duration.ofMinutes(5));
            List<ClaimedRun> started = claim("mf-coal2");
            assertThat(started).extracting(ClaimedRun::runId).containsExactly(m2, queued);
            assertThat(started.get(0).coalescedCount()).isEqualTo(2);
            assertThat(started.get(1).coalescedCount()).isEqualTo(1);
            assertThat(fx.state(m1)).isEqualTo("SKIPPED");
        } finally {
            clearNodes();
        }
    }

    @Test
    void staleHeartbeatsCountAsDownSoTheRuleFallsBackToDbNow() {
        fx.tenant("mf-stale", 5);
        UUID job = fx.job("mf-stale").misfire(MisfirePolicyType.SKIP).misfireThreshold(Duration.ofSeconds(60)).insert();
        long missed = fx.run(job, Instant.now().minus(Duration.ofMinutes(5)));
        db.jdbc().sql("""
                INSERT INTO scheduler_node (node_id, heartbeat_at, started_at, alive_since)
                VALUES ('dead', clock_timestamp() - interval '1 minute', clock_timestamp() - interval '2 hours',
                        clock_timestamp() - interval '2 hours')""").update();
        try {
            assertThat(claim("mf-stale")).isEmpty(); // e.g. the DB just came back, no heartbeat yet
            assertThat(fx.state(missed)).isEqualTo("SKIPPED");
        } finally {
            clearNodes();
        }
    }
}
