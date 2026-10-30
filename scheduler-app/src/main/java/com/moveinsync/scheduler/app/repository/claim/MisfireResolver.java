package com.moveinsync.scheduler.app.repository.claim;

import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import com.moveinsync.scheduler.core.domain.RunState;
import com.moveinsync.scheduler.core.policy.MisfireDecision;
import com.moveinsync.scheduler.core.policy.MisfirePolicy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Misfire handling at claim time (plan section 09), inside the claim transaction of {@link
 * JdbcClaimProtocol}. When a candidate is a {@code SCHEDULED} run that has misfired, the other due
 * {@code SCHEDULED} runs of the same job are locked and the job's {@link MisfirePolicy} decides:
 * {@code SKIP} marks the misfired ones {@code SKIPPED}; {@code COALESCE} fires the newest misfired
 * one with {@code coalesced_count = n} and skips the rest; {@code FIRE_NOW} fires all in order.
 *
 * <p><b>Misfired means late before it could be claimed</b> (V204), not merely late: a run queued
 * behind its own tenant's cap is late but was never missed. The policy is evaluated at {@code
 * claimable_since}, the moment the tenant last became claimable: the later of the tenant's
 * unpause ({@code tenant.resumed_at}) and the start of the live nodes' unbroken heartbeats ({@code
 * min(scheduler_node.alive_since)}; a full-cluster or DB outage restarts it), or DB now when no
 * node is live (protocol-level use without membership). A run misfired iff {@code claimable_since -
 * scheduled_at > threshold}; runs that fell due after {@code claimable_since} are always on time.
 * Approximation: a rolling restart of every node also moves {@code claimable_since}, so a run that
 * was already queued for more than the threshold at that point counts as misfired.
 *
 * <p>Retries ({@code RETRY_WAIT}) are never treated as misfires: the occurrence already started
 * once, and {@code RETRY_WAIT -> SKIPPED} is not a legal transition. Runs with {@code
 * misfire_exempt} (operator replays of dead letters, manual triggers) are explicit requests to run:
 * they never trigger a decision, are never part of one (so never skipped or coalesced), and start
 * like on-time runs.
 *
 * <p>Locks are run rows only, taken after the claim's slot locks (I2) and with {@code SKIP
 * LOCKED}; a run of the job that another claimer holds at that instant is simply not part of the
 * decision (it is being started by that claimer).
 */
final class MisfireResolver {

    /** A locked candidate row of the claim. */
    record Candidate(long runId, UUID jobId, String state, boolean misfireExempt, Instant scheduledAt,
                     String misfire, Duration threshold, Instant dbNow) {
    }

    /** A run to start, with the coalesced count to store ({@code null} keeps the column). */
    record Start(long runId, Integer coalescedCount) {
    }

    /* I1, I2: the job's due SCHEDULED occurrences, locked after the slots (we already hold the
       candidate's own lock; SKIP LOCKED never makes us wait for another claimer). scheduled_at <=
       the DB time the candidates were read at, as MisfirePolicy requires. Exempt runs (replays,
       manual triggers) are not occurrences the policy may skip or coalesce. */
    private static final String JOB_DUE = """
            SELECT id, scheduled_at FROM job_run
             WHERE job_id = ? AND state = 'SCHEDULED' AND NOT misfire_exempt
               AND due_at <= clock_timestamp() AND scheduled_at <= ?
             ORDER BY scheduled_at
               FOR UPDATE SKIP LOCKED
            """;

    /* Only rows we locked above and that are still SCHEDULED move to SKIPPED (legal transition). */
    private static final String SKIP = """
            UPDATE job_run SET state = 'SKIPPED', finished_at = clock_timestamp()
             WHERE id = ANY (?) AND state = 'SCHEDULED'
            """;

    /* I1: when the tenant last became claimable (see class comment); DB clock, no locks. */
    private static final String CLAIMABLE_SINCE = """
            SELECT greatest(t.resumed_at, coalesce(
                     (SELECT min(n.alive_since) FROM scheduler_node n
                       WHERE n.heartbeat_at > clock_timestamp() - ? * interval '1 millisecond'),
                     clock_timestamp())) AS claimable_since
              FROM tenant t
             WHERE t.id = ?
            """;

    private final long deadAfterMs;

    /** @param deadAfter {@code membership.dead-after}: nodes with an older heartbeat are not live */
    MisfireResolver(Duration deadAfter) {
        this.deadAfterMs = deadAfter.toMillis();
    }

    Session session(Connection connection, String tenantId) {
        return new Session(connection, tenantId);
    }

    /** Per-claim-transaction state: each job is resolved at most once. */
    final class Session {
        private final Connection c;
        private final String tenantId;
        private final Map<UUID, Resolution> resolved = new HashMap<>();
        private final Set<Long> chosen = new HashSet<>();
        private Instant claimableSince; // read once per claim, on the first late candidate

        private Session(Connection c, String tenantId) {
            this.c = c;
            this.tenantId = tenantId;
        }

        /** {@code claimable_since}, never after the candidates' DB time {@code dbNow}. */
        private Instant claimableSince(Instant dbNow) throws SQLException {
            if (claimableSince == null) {
                try (PreparedStatement ps = c.prepareStatement(CLAIMABLE_SINCE)) {
                    ps.setLong(1, deadAfterMs);
                    ps.setString(2, tenantId);
                    try (ResultSet rs = ps.executeQuery()) {
                        claimableSince = rs.next() ? JdbcClaimProtocol.instant(rs, "claimable_since") : dbNow;
                    }
                }
            }
            return claimableSince.isAfter(dbNow) ? dbNow : claimableSince;
        }

        /**
         * Appends runs to start to {@code starts} (at most {@code capacity} in total), in candidate
         * order, applying misfire policies on the way.
         *
         * @return true if a candidate was absorbed (skipped), so a further candidate round may help
         */
        boolean resolve(List<Candidate> candidates, int capacity, List<Start> starts) throws SQLException {
            boolean absorbed = false;
            for (Candidate cand : candidates) {
                if (starts.size() >= capacity) {
                    break;
                }
                Resolution res = resolved.get(cand.jobId());
                // late at DB now is necessary (claimable_since <= now), so on-time claims skip the read
                if (res == null && RunState.SCHEDULED.name().equals(cand.state()) && !cand.misfireExempt()
                        && MisfirePolicy.isMisfire(cand.scheduledAt(), cand.dbNow(), cand.threshold())) {
                    Instant since = claimableSince(cand.dbNow());
                    if (MisfirePolicy.isMisfire(cand.scheduledAt(), since, cand.threshold())) {
                        res = resolveJob(cand, since);
                        resolved.put(cand.jobId(), res);
                    }
                }
                if (res != null && res.skipped().contains(cand.runId())) {
                    absorbed = true;
                    continue;
                }
                if (res != null && res.fire().containsKey(cand.runId())) {
                    for (Map.Entry<Long, Integer> f : res.fire().entrySet()) {
                        if (starts.size() >= capacity) {
                            break;
                        }
                        add(starts, f.getKey(), f.getValue());
                    }
                    continue;
                }
                add(starts, cand.runId(), null);
            }
            return absorbed;
        }

        private void add(List<Start> starts, long runId, Integer coalesced) {
            if (chosen.add(runId)) {
                starts.add(new Start(runId, coalesced));
            }
        }

        /**
         * Decides one job at {@code since}: occurrences due by then go to the policy; later ones
         * fell due while the tenant was claimable, so they are on time and fire individually.
         */
        private Resolution resolveJob(Candidate cand, Instant since) throws SQLException {
            List<Instant> times = new ArrayList<>();
            List<Instant> later = new ArrayList<>();
            Map<Instant, Long> ids = new HashMap<>();
            try (PreparedStatement ps = c.prepareStatement(JOB_DUE)) {
                ps.setObject(1, cand.jobId());
                ps.setObject(2, java.time.OffsetDateTime.ofInstant(cand.dbNow(), java.time.ZoneOffset.UTC));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Instant at = JdbcClaimProtocol.instant(rs, "scheduled_at");
                        (at.isAfter(since) ? later : times).add(at);
                        ids.put(at, rs.getLong("id"));
                    }
                }
            }
            MisfirePolicy policy = MisfirePolicy.of(MisfirePolicyType.valueOf(cand.misfire()));
            MisfireDecision decision = policy.decide(times, since, cand.threshold());
            Set<Long> skipped = new HashSet<>();
            for (Instant at : decision.toSkip()) {
                skipped.add(ids.get(at));
            }
            if (!skipped.isEmpty()) {
                try (PreparedStatement ps = c.prepareStatement(SKIP)) {
                    ps.setArray(1, c.createArrayOf("int8", skipped.toArray()));
                    ps.executeUpdate();
                }
            }
            Map<Long, Integer> fire = new LinkedHashMap<>();
            for (Instant at : decision.toFire()) {
                int count = decision.coalescedCountFor(at);
                fire.put(ids.get(at), count > 1 ? count : null);
            }
            for (Instant at : later) {
                fire.put(ids.get(at), null);
            }
            return new Resolution(fire, skipped);
        }
    }

    /** Fired runs in order (with coalesced count or null) and skipped runs of one job. */
    private record Resolution(Map<Long, Integer> fire, Set<Long> skipped) {
    }
}
