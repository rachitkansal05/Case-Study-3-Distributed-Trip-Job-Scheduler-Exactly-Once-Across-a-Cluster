package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.core.time.CronSchedule;
import com.moveinsync.scheduler.core.time.NextFireCalculator;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns job definitions into {@code job_run} rows ahead of time (plan sections 05, 08, 09).
 *
 * <p>Recurring jobs: {@code job.next_fire_at} is the cursor, the next occurrence not yet
 * materialised. Each pass inserts the occurrences from the cursor (inclusive) up to {@code DB now +
 * horizon} (inclusive), at most {@code maxOccurrencesPerJob} per job, and advances the cursor in
 * the same transaction. Cron is evaluated in the tenant zone by {@link NextFireCalculator} (DST
 * rules of {@code WallClockResolver}). One-off jobs get exactly one run at {@code run_at}.
 *
 * <p>Safe to run concurrently on many nodes: inserts are {@code ON CONFLICT DO NOTHING} on the
 * occurrence/idempotency uniques, the cursor update is optimistic, and job rows are only ever
 * locked with {@code FOR UPDATE SKIP LOCKED} (never waits, so it cannot deadlock). Only the DB clock
 * is used. Never logs payloads. Holds no mutable state; thread-safe.
 */
public final class Materializer {

    private static final Logger log = LoggerFactory.getLogger(Materializer.class);

    /** Postgres SQLSTATEs that just mean "try again next pass". */
    private static final String DEADLOCK = "40P01";
    private static final String SERIALIZATION = "40001";

    private static final NextFireCalculator CALCULATOR = new NextFireCalculator();

    /**
     * Counts of one pass.
     *
     * @param jobsExamined jobs (recurring and one-off) this pass looked at
     * @param runsInserted runs actually inserted (conflicts not counted)
     * @param cursorsAdvanced recurring cursors this node moved (including initialisation)
     */
    public record Result(int jobsExamined, int runsInserted, int cursorsAdvanced) {}

    /** Occurrences to insert for one job and the cursor that follows them (null: never fires again). */
    record Plan(List<Instant> occurrences, Instant nextCursor) {}

    // DB clock: the pass reads clock_timestamp() once; every horizon and cursor derives from it.
    private static final String SQL_DB_NOW = "SELECT clock_timestamp()";

    // Candidate scan (no locks): enabled recurring jobs of owned partitions whose cursor is unset or
    // within the horizon. Served by job_next_fire (partition_id, next_fire_at) WHERE enabled AND RECURRING.
    private static final String SQL_RECURRING_CANDIDATES = """
            SELECT j.id
              FROM job j
             WHERE j.enabled
               AND j.kind = 'RECURRING'
               AND j.partition_id = ANY (?)
               AND (j.next_fire_at IS NULL OR j.next_fire_at <= ?)
             ORDER BY j.next_fire_at
             LIMIT ?""";

    // Per-job lock, non-blocking: SKIP LOCKED means a job another node is materialising right now is
    // skipped instead of waited on (no lock waits on job, so no deadlock with the API or other nodes).
    // Re-checks every candidate predicate on the fresh row, so a stale scan never acts.
    private static final String SQL_LOCK_RECURRING = """
            SELECT j.cron, j.next_fire_at, j.version, t.time_zone
              FROM job j
              JOIN tenant t ON t.id = j.tenant_id
             WHERE j.id = ?
               AND j.enabled
               AND j.kind = 'RECURRING'
               AND j.partition_id = ANY (?)
               AND (j.next_fire_at IS NULL OR j.next_fire_at <= ?)
               FOR UPDATE OF j SKIP LOCKED""";

    // Idempotent materialisation: ON CONFLICT DO NOTHING (no target) absorbs both
    // UNIQUE (job_id, scheduled_at) and UNIQUE (idem_key), so a repeated or concurrent insert is a no-op.
    // priority / job_version / max_run_time / partition_id are copied from the job row itself, and
    // only while it still has the version the occurrences were computed from. ORDER BY ord inserts in
    // ascending time, a fixed order for every node.
    private static final String SQL_INSERT_RECURRING_RUNS = """
            INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                 scheduled_at, due_at, idem_key, state)
            SELECT j.id, j.tenant_id, j.partition_id, j.priority, j.version, j.max_run_time,
                   s.at::timestamptz, s.at::timestamptz, s.idem_key, 'SCHEDULED'
              FROM job j
             CROSS JOIN unnest(?::text[], ?::text[]) WITH ORDINALITY AS s (at, idem_key, ord)
             WHERE j.id = ? AND j.version = ?
             ORDER BY s.ord
            ON CONFLICT DO NOTHING""";

    // Optimistic cursor: moves only from the value this pass read and only for the same definition
    // version. 0 rows = another node advanced it or the job was edited; the transaction is rolled back.
    // 'infinity' marks a schedule that can never fire again, so it stops being a candidate.
    private static final String SQL_ADVANCE_CURSOR = """
            UPDATE job
               SET next_fire_at = COALESCE(?::timestamptz, 'infinity'::timestamptz)
             WHERE id = ?
               AND next_fire_at IS NOT DISTINCT FROM ?::timestamptz
               AND version = ?""";

    // One-off candidates, locked without waiting (SKIP LOCKED): enabled one-offs of owned partitions
    // whose cursor is still set (the API creates them with next_fire_at = run_at; we clear it after the
    // insert, served by index job_one_off_pending from V200) and that have no run at (job_id, run_at)
    // yet; the NOT EXISTS probe uses the occurrence unique index and keeps a re-set cursor idempotent.
    // Horizon does not apply: the run is created now and the claim only picks it once due.
    private static final String SQL_ONE_OFF_CANDIDATES = """
            SELECT j.id, j.run_at
              FROM job j
             WHERE j.enabled
               AND j.kind = 'ONE_OFF'
               AND j.partition_id = ANY (?)
               AND j.next_fire_at IS NOT NULL
               AND NOT EXISTS (SELECT 1 FROM job_run r WHERE r.job_id = j.id AND r.scheduled_at = j.run_at)
             ORDER BY j.id
             LIMIT ?
               FOR UPDATE OF j SKIP LOCKED""";

    // Idempotent materialisation of one-offs: exactly one run per job at run_at; ON CONFLICT DO NOTHING
    // on both uniques. The run_at re-check keeps idem_key and scheduled_at consistent; ORDER BY ord
    // gives every node the same insertion order (sorted by job id).
    private static final String SQL_INSERT_ONE_OFF_RUNS = """
            INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                 scheduled_at, due_at, idem_key, state)
            SELECT j.id, j.tenant_id, j.partition_id, j.priority, j.version, j.max_run_time,
                   j.run_at, j.run_at, s.idem_key, 'SCHEDULED'
              FROM unnest(?::uuid[], ?::text[], ?::text[]) WITH ORDINALITY AS s (job_id, at, idem_key, ord)
              JOIN job j ON j.id = s.job_id AND j.run_at = s.at::timestamptz
             ORDER BY s.ord
            ON CONFLICT DO NOTHING""";

    // A one-off has no cursor: clear any stray value (only rows that have one, to avoid useless writes).
    private static final String SQL_CLEAR_ONE_OFF_CURSOR = """
            UPDATE job SET next_fire_at = NULL
             WHERE id = ANY (?::uuid[]) AND kind = 'ONE_OFF' AND next_fire_at IS NOT NULL""";

    private final DataSource dataSource;
    private final Supplier<Set<Integer>> ownedPartitions;
    private final Duration horizon;
    private final int maxOccurrencesPerJob;
    private final int jobBatchSize;

    /**
     * @param dataSource the scheduler database
     * @param ownedPartitions partitions this node currently owns; read once per pass
     * @param horizon how far past DB now to materialise, &gt;= 0
     * @param maxOccurrencesPerJob cap of runs per recurring job per pass (bounded catch-up), &gt;= 1
     * @param jobBatchSize cap of jobs per kind per pass, &gt;= 1
     */
    public Materializer(DataSource dataSource, Supplier<Set<Integer>> ownedPartitions,
                        Duration horizon, int maxOccurrencesPerJob, int jobBatchSize) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.ownedPartitions = Objects.requireNonNull(ownedPartitions, "ownedPartitions");
        this.horizon = Objects.requireNonNull(horizon, "horizon");
        if (horizon.isNegative()) {
            throw new IllegalArgumentException("horizon must be >= 0: " + horizon);
        }
        if (maxOccurrencesPerJob < 1 || maxOccurrencesPerJob > NextFireCalculator.MAX_COUNT) {
            throw new IllegalArgumentException("maxOccurrencesPerJob must be 1-"
                    + NextFireCalculator.MAX_COUNT + ": " + maxOccurrencesPerJob);
        }
        if (jobBatchSize < 1) {
            throw new IllegalArgumentException("jobBatchSize must be >= 1: " + jobBatchSize);
        }
        this.maxOccurrencesPerJob = maxOccurrencesPerJob;
        this.jobBatchSize = jobBatchSize;
    }

    /** One pass over jobs in the owned partitions; safe to call concurrently from many nodes. */
    public Result materializeOnce() {
        Set<Integer> owned = ownedPartitions.get();
        if (owned == null || owned.isEmpty()) {
            return new Result(0, 0, 0);
        }
        Integer[] partitions = owned.toArray(Integer[]::new);
        int examined = 0;
        int inserted = 0;
        int advanced = 0;
        try (Connection c = dataSource.getConnection()) {
            Instant dbNow = dbNow(c);
            Instant limit = dbNow.plus(horizon);

            List<UUID> candidates = recurringCandidates(c, partitions, limit);
            for (UUID jobId : candidates) {
                examined++;
                int[] r = materializeRecurring(c, jobId, partitions, dbNow, limit);
                inserted += r[0];
                advanced += r[1];
            }

            int[] o = materializeOneOffs(c, partitions);
            examined += o[0];
            inserted += o[1];
        } catch (SQLException e) {
            throw new IllegalStateException("materializer pass failed", e);
        }
        if (inserted > 0 || advanced > 0) {
            log.debug("materializer pass: jobsExamined={} runsInserted={} cursorsAdvanced={}",
                    examined, inserted, advanced);
        }
        return new Result(examined, inserted, advanced);
    }

    /**
     * Occurrences starting at {@code cursor} (inclusive) up to {@code limit} (inclusive), at most
     * {@code max}; the next cursor is the first occurrence after the last one returned (or {@code
     * cursor} itself if it is beyond {@code limit}), null if the schedule never fires again.
     */
    static Plan plan(CronSchedule cron, ZoneId zone, Instant cursor, Instant limit, int max) {
        List<Instant> out = new ArrayList<>();
        Instant next = cursor;
        while (next != null && out.size() < max && !next.isAfter(limit)) {
            out.add(next);
            next = CALCULATOR.nextAfter(cron, zone, next).map(NextFireCalculator.FireTime::instant).orElse(null);
        }
        return new Plan(List.copyOf(out), next);
    }

    /** {@code jobId:scheduledAtUTC}, e.g. {@code ...:2026-11-01T05:30:00Z}. */
    static String idemKey(UUID jobId, Instant scheduledAt) {
        return jobId + ":" + scheduledAt;
    }

    private static Instant dbNow(Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(SQL_DB_NOW); ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getObject(1, OffsetDateTime.class).toInstant();
        }
    }

    private List<UUID> recurringCandidates(Connection c, Integer[] partitions, Instant limit)
            throws SQLException {
        List<UUID> ids = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(SQL_RECURRING_CANDIDATES)) {
            ps.setArray(1, c.createArrayOf("int4", partitions));
            ps.setObject(2, utc(limit));
            ps.setInt(3, jobBatchSize);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getObject(1, UUID.class));
                }
            }
        }
        return ids;
    }

    /** One short transaction for one recurring job; returns {inserted, advanced (0/1)}. */
    private int[] materializeRecurring(Connection c, UUID jobId, Integer[] partitions,
                                       Instant dbNow, Instant limit) throws SQLException {
        c.setAutoCommit(false);
        try {
            String cronText;
            Instant oldCursor;
            long version;
            String zoneText;
            try (PreparedStatement ps = c.prepareStatement(SQL_LOCK_RECURRING)) {
                ps.setObject(1, jobId);
                ps.setArray(2, c.createArrayOf("int4", partitions));
                ps.setObject(3, utc(limit));
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        c.rollback(); // locked by another node, advanced past the horizon, or changed
                        return new int[] {0, 0};
                    }
                    cronText = rs.getString(1);
                    OffsetDateTime cur = rs.getObject(2, OffsetDateTime.class);
                    oldCursor = cur == null ? null : cur.toInstant();
                    version = rs.getLong(3);
                    zoneText = rs.getString(4);
                }
            }

            CronSchedule cron;
            ZoneId zone;
            try {
                cron = CronSchedule.parse(cronText);
                zone = ZoneId.of(zoneText);
            } catch (RuntimeException e) {
                c.rollback();
                log.warn("materializer: job {} has an invalid cron or tenant zone, skipped: {}",
                        jobId, e.getClass().getSimpleName());
                return new int[] {0, 0};
            }

            Instant start = oldCursor != null
                    ? oldCursor
                    : CALCULATOR.nextAfter(cron, zone, dbNow).map(NextFireCalculator.FireTime::instant).orElse(null);
            Plan plan = start == null
                    ? new Plan(List.of(), null)
                    : plan(cron, zone, start, limit, maxOccurrencesPerJob);

            int inserted = 0;
            if (!plan.occurrences().isEmpty()) {
                String[] at = new String[plan.occurrences().size()];
                String[] keys = new String[at.length];
                for (int i = 0; i < at.length; i++) {
                    Instant occ = plan.occurrences().get(i);
                    at[i] = occ.toString();
                    keys[i] = idemKey(jobId, occ);
                }
                try (PreparedStatement ps = c.prepareStatement(SQL_INSERT_RECURRING_RUNS)) {
                    ps.setArray(1, c.createArrayOf("text", at));
                    ps.setArray(2, c.createArrayOf("text", keys));
                    ps.setObject(3, jobId);
                    ps.setLong(4, version);
                    inserted = ps.executeUpdate();
                }
            }

            int updated;
            try (PreparedStatement ps = c.prepareStatement(SQL_ADVANCE_CURSOR)) {
                setTimestamp(ps, 1, plan.nextCursor());
                ps.setObject(2, jobId);
                setTimestamp(ps, 3, oldCursor);
                ps.setLong(4, version);
                updated = ps.executeUpdate();
            }
            if (updated == 0) {
                // Lost the optimistic race (or the job was edited): drop this attempt; any runs it
                // would have added were added by the winner or will be by the next pass.
                c.rollback();
                log.debug("materializer: cursor of job {} moved concurrently, skipped", jobId);
                return new int[] {0, 0};
            }
            c.commit();
            if (plan.nextCursor() == null) {
                log.info("materializer: job {} schedule never fires again; cursor parked", jobId);
            }
            return new int[] {inserted, 1};
        } catch (SQLException e) {
            safeRollback(c);
            if (isRetryable(e)) {
                log.debug("materializer: job {} hit {}, retried next pass", jobId, e.getSQLState());
                return new int[] {0, 0};
            }
            throw e;
        } finally {
            c.setAutoCommit(true);
        }
    }

    /** One short transaction for a batch of one-offs; returns {examined, inserted}. */
    private int[] materializeOneOffs(Connection c, Integer[] partitions) throws SQLException {
        c.setAutoCommit(false);
        try {
            List<UUID> ids = new ArrayList<>();
            List<Instant> runAts = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(SQL_ONE_OFF_CANDIDATES)) {
                ps.setArray(1, c.createArrayOf("int4", partitions));
                ps.setInt(2, jobBatchSize);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ids.add(rs.getObject(1, UUID.class));
                        runAts.add(rs.getObject(2, OffsetDateTime.class).toInstant());
                    }
                }
            }
            if (ids.isEmpty()) {
                c.commit();
                return new int[] {0, 0};
            }
            // Same total order on every node (by job id, the leading column of the unique keys).
            Integer[] order = new Integer[ids.size()];
            for (int i = 0; i < order.length; i++) {
                order[i] = i;
            }
            Arrays.sort(order, Comparator.comparing(i -> ids.get(i).toString()));
            String[] idArr = new String[order.length];
            String[] atArr = new String[order.length];
            String[] keyArr = new String[order.length];
            for (int k = 0; k < order.length; k++) {
                UUID id = ids.get(order[k]);
                Instant at = runAts.get(order[k]);
                idArr[k] = id.toString();
                atArr[k] = at.toString();
                keyArr[k] = idemKey(id, at);
            }
            int inserted;
            try (PreparedStatement ps = c.prepareStatement(SQL_INSERT_ONE_OFF_RUNS)) {
                ps.setArray(1, c.createArrayOf("text", idArr));
                ps.setArray(2, c.createArrayOf("text", atArr));
                ps.setArray(3, c.createArrayOf("text", keyArr));
                inserted = ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement(SQL_CLEAR_ONE_OFF_CURSOR)) {
                ps.setArray(1, c.createArrayOf("text", idArr));
                ps.executeUpdate();
            }
            c.commit();
            return new int[] {ids.size(), inserted};
        } catch (SQLException e) {
            safeRollback(c);
            if (isRetryable(e)) {
                log.debug("materializer: one-off batch hit {}, retried next pass", e.getSQLState());
                return new int[] {0, 0};
            }
            throw e;
        } finally {
            c.setAutoCommit(true);
        }
    }

    private static boolean isRetryable(SQLException e) {
        return DEADLOCK.equals(e.getSQLState()) || SERIALIZATION.equals(e.getSQLState());
    }

    private static void safeRollback(Connection c) {
        try {
            c.rollback();
        } catch (SQLException ignored) {
            // the connection is broken; the pool will discard it and the DB rolls the tx back
        }
    }

    private static void setTimestamp(PreparedStatement ps, int index, Instant value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
        } else {
            ps.setObject(index, utc(value));
        }
    }

    private static OffsetDateTime utc(Instant i) {
        return i.atOffset(ZoneOffset.UTC);
    }
}
