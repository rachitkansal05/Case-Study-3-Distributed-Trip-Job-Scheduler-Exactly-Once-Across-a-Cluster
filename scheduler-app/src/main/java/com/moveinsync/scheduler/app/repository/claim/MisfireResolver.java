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
 * JdbcClaimProtocol}. When a candidate is a {@code SCHEDULED} run whose lateness exceeds its job's
 * threshold ({@code now - scheduled_at > threshold}, DB time), the other due {@code SCHEDULED}
 * runs of the same job are locked and the job's {@link MisfirePolicy} decides: {@code SKIP} marks
 * the misfired ones {@code SKIPPED}; {@code COALESCE} fires the newest misfired one with {@code
 * coalesced_count = n} and skips the rest; {@code FIRE_NOW} fires all in order.
 *
 * <p>Retries ({@code RETRY_WAIT}) are never treated as misfires: the occurrence already started
 * once, and {@code RETRY_WAIT -> SKIPPED} is not a legal transition.
 *
 * <p>Locks are run rows only, taken after the claim's slot locks (I2) and with {@code SKIP
 * LOCKED}; a run of the job that another claimer holds at that instant is simply not part of the
 * decision (it is being started by that claimer).
 */
final class MisfireResolver {

    /** A locked candidate row of the claim. */
    record Candidate(long runId, UUID jobId, String state, Instant scheduledAt, String misfire,
                     Duration threshold, Instant dbNow) {
    }

    /** A run to start, with the coalesced count to store ({@code null} keeps the column). */
    record Start(long runId, Integer coalescedCount) {
    }

    /* I1, I2: the job's due SCHEDULED occurrences, locked after the slots (we already hold the
       candidate's own lock; SKIP LOCKED never makes us wait for another claimer). scheduled_at <=
       the DB time the candidates were read at, as MisfirePolicy requires. */
    private static final String JOB_DUE = """
            SELECT id, scheduled_at FROM job_run
             WHERE job_id = ? AND state = 'SCHEDULED'
               AND due_at <= clock_timestamp() AND scheduled_at <= ?
             ORDER BY scheduled_at
               FOR UPDATE SKIP LOCKED
            """;

    /* Only rows we locked above and that are still SCHEDULED move to SKIPPED (legal transition). */
    private static final String SKIP = """
            UPDATE job_run SET state = 'SKIPPED', finished_at = clock_timestamp()
             WHERE id = ANY (?) AND state = 'SCHEDULED'
            """;

    Session session(Connection connection) {
        return new Session(connection);
    }

    /** Per-claim-transaction state: each job is resolved at most once. */
    final class Session {
        private final Connection c;
        private final Map<UUID, Resolution> resolved = new HashMap<>();
        private final Set<Long> chosen = new HashSet<>();

        private Session(Connection c) {
            this.c = c;
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
                if (res == null && RunState.SCHEDULED.name().equals(cand.state())
                        && MisfirePolicy.isMisfire(cand.scheduledAt(), cand.dbNow(), cand.threshold())) {
                    res = resolveJob(cand);
                    resolved.put(cand.jobId(), res);
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

        private Resolution resolveJob(Candidate cand) throws SQLException {
            List<Instant> times = new ArrayList<>();
            Map<Instant, Long> ids = new HashMap<>();
            try (PreparedStatement ps = c.prepareStatement(JOB_DUE)) {
                ps.setObject(1, cand.jobId());
                ps.setObject(2, java.time.OffsetDateTime.ofInstant(cand.dbNow(), java.time.ZoneOffset.UTC));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Instant at = JdbcClaimProtocol.instant(rs, "scheduled_at");
                        times.add(at);
                        ids.put(at, rs.getLong("id"));
                    }
                }
            }
            MisfirePolicy policy = MisfirePolicy.of(MisfirePolicyType.valueOf(cand.misfire()));
            MisfireDecision decision = policy.decide(times, cand.dbNow(), cand.threshold());
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
            return new Resolution(fire, skipped);
        }
    }

    /** Fired runs in order (with coalesced count or null) and skipped runs of one job. */
    private record Resolution(Map<Long, Integer> fire, Set<Long> skipped) {
    }
}
