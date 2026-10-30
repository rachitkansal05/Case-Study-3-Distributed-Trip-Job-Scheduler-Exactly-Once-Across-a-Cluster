package com.moveinsync.scheduler.app.repository.claim;

import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.RunFailure;
import com.moveinsync.scheduler.core.domain.TenantId;
import com.moveinsync.scheduler.core.policy.RetryDecider;
import com.moveinsync.scheduler.core.ports.ClaimProtocol;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * The claim protocol of plan section 07 on PostgreSQL: the <b>only</b> engine code that changes
 * {@code job_run.state} or which run holds a tenant slot. Every statement carries a comment naming
 * the invariant it protects. The invariants:
 *
 * <ul>
 *   <li><b>I1 DB clock.</b> Every due and lease comparison uses {@code clock_timestamp()}, never
 *       {@code now()} (transaction start) and never node time.
 *   <li><b>I2 Lock order.</b> Slot rows first, then run rows. Locks that could make us wait for
 *       another claimer use {@code FOR UPDATE SKIP LOCKED}; the only blocking waits are on a single
 *       run row whose holder's transaction never waits for anything (renew, checkpoint, commit).
 *       Commits never lock slot rows. So claims cannot deadlock with each other or with commits.
 *   <li><b>I3 Slot ownership.</b> A run is given a slot only while this transaction holds that slot
 *       row's lock; a slot whose holder is RUNNING is reused only after a conditional reap that
 *       re-checks expiry under the holder's row lock (exactly one of a concurrent renew and reap
 *       wins). Together with the schema (FK, CHECK, {@code one_run_per_slot}) running runs per
 *       tenant never exceed slot rows.
 *   <li><b>I4 Fenced writes.</b> Renew, checkpoint and commit match {@code (fencing_epoch,
 *       fencing_seq) = (:e,:s) AND state = 'RUNNING' AND lease_until >= clock_timestamp()}, so an
 *       expired lease can never be renewed, and a zombie can never free a slot it lost.
 *   <li><b>I5 Tokens.</b> {@code (epoch, nextval('fence_seq'))}, epoch read in the same transaction;
 *       a token leaves this class only after its claim transaction committed.
 *   <li><b>I6 Attempts.</b> A claim increments {@code attempt}; a reap is a lease loss: it gives
 *       the attempt back and increments {@code lease_lost_count}; at the lease-lost limit the run
 *       is dead-lettered with {@code FENCING_CONFLICT}.
 *   <li><b>I7 Caps.</b> A claim only uses slots below {@code tenant.configured_cap} and yields
 *       while a reduction drains (a RUNNING run in a slot at or above the cap), so no new run
 *       starts until the running count is below the new cap (plan section 10).
 * </ul>
 *
 * <p><b>Documented exception to "only writer":</b> the API writes {@code job_run} in four ways
 * that never touch a RUNNING run or an occupied slot: manual-trigger inserts (SCHEDULED), cancel
 * of unstarted runs on job disable (SCHEDULED/RETRY_WAIT/PAUSED to CANCELLED), dead-letter replay
 * (DEAD_LETTERED to SCHEDULED), and cap changes (inserting missing low slots, deleting free high
 * slots under {@code FOR UPDATE SKIP LOCKED}). The materialiser inserts SCHEDULED runs.
 */
public final class JdbcClaimProtocol implements ClaimProtocol {

    // ------------------------------------------------------------------ claim (plan 07, steps 0-3)

    /* Step 0. Paused tenants are skipped by reading tenant.paused in the claim itself (never
       cached); the configured cap bounds which slots a claim may use (I7). */
    private static final String TENANT = """
            SELECT paused, configured_cap FROM tenant WHERE id = ?
            """;

    /* Hint only, no locks: how many due runs could start, so we never lock more slots than we can
       fill (other nodes polling the same tenant keep the rest). Due = clock_timestamp() (I1). */
    private static final String PEEK_DUE = """
            SELECT count(*) FROM (
              SELECT 1 FROM job_run r JOIN job j ON j.id = r.job_id
               WHERE r.partition_id = ANY (?) AND r.tenant_id = ?
                 AND r.state IN ('SCHEDULED', 'RETRY_WAIT') AND r.due_at <= clock_timestamp()
                 AND j.enabled
               LIMIT ?) d
            """;

    /* I7: while a cap reduction drains (a RUNNING run sits in a slot >= configured cap), claims
       yield so the compactor can move high runs into every freed low slot first. */
    private static final String DRAINING = """
            SELECT EXISTS (SELECT 1 FROM job_run
                            WHERE tenant_id = ? AND state = 'RUNNING' AND slot_no >= ?)
            """;

    /* Step 1 (I2, I3): lock up to k slot rows of the tenant that look free, slot rows first.
       SKIP LOCKED: never wait for another claimer. The "looks free" join is only a hint. */
    private static final String LOCK_SLOTS = """
            SELECT s.slot_no
              FROM tenant_slot s
              LEFT JOIN job_run r
                ON r.tenant_id = s.tenant_id AND r.slot_no = s.slot_no AND r.state = 'RUNNING'
             WHERE s.tenant_id = ? AND s.slot_no < ?
               AND (r.id IS NULL OR r.lease_until < clock_timestamp())
             ORDER BY s.slot_no
             LIMIT ?
               FOR UPDATE OF s SKIP LOCKED
            """;

    /* Step 1b (I3): re-read the holders of the slots we now hold, in a fresh statement snapshot.
       The step-1 join used a snapshot taken before our locks, so a run another claimer started in
       one of these slots just before we locked it would be invisible there. Every slot assignment
       needs the slot lock, which we hold now, so this read is exact for "who holds the slot";
       only lease expiry can still change, which the conditional reap re-checks. */
    private static final String HOLDERS = """
            SELECT slot_no, id, lease_until < clock_timestamp() AS expired
              FROM job_run
             WHERE tenant_id = ? AND state = 'RUNNING' AND slot_no = ANY (?)
            """;

    /* Step 2 (I3, I6): conditional reap. Waits on the holder's row lock and re-evaluates the WHERE
       on the newest row version, so it cannot race a renewal: exactly one of renew/reap updates
       the row. Pinned to (tenant, slot) so a run that moved slots is never reaped through a stale
       slot. A lease loss: the attempt is given back and lease_lost_count rises. lease_until is not
       changed here, so it returns the expired lease (the takeover time is measured from it). */
    private static final String REAP = """
            UPDATE job_run
               SET state = 'RETRY_WAIT', slot_no = NULL, due_at = clock_timestamp(),
                   lease_lost_count = lease_lost_count + 1, attempt = greatest(attempt - 1, 0)
             WHERE id = ? AND tenant_id = ? AND slot_no = ?
               AND state = 'RUNNING' AND lease_until < clock_timestamp()
            RETURNING lease_lost_count, lease_until
            """;

    /* I6: past the lease-lost limit a reaped run is dead-lettered (FENCING_CONFLICT) so a fencing
       bug cannot loop forever. Same transaction as the reap, row already locked by us. */
    private static final String DEAD_LETTER_REAPED = """
            UPDATE job_run SET state = 'DEAD_LETTERED', finished_at = clock_timestamp(), last_error = ?
             WHERE id = ? AND state = 'RETRY_WAIT'
            RETURNING tenant_id, attempt
            """;

    /* Step 3 candidates (I1, I2): due runs of the tenant in the polled partitions, by priority then
       due time; run rows are locked after the slot rows, SKIP LOCKED so two nodes polling the same
       partition during a handover take different runs. Disabled jobs never start. */
    static final String CANDIDATES = """
            SELECT r.id, r.job_id, r.state, r.misfire_exempt, r.scheduled_at, j.misfire,
                   (extract(epoch FROM j.misfire_threshold) * 1000)::bigint AS threshold_ms,
                   clock_timestamp() AS db_now
              FROM job_run r JOIN job j ON j.id = r.job_id
             WHERE r.partition_id = ANY (?) AND r.tenant_id = ?
               AND r.state IN ('SCHEDULED', 'RETRY_WAIT') AND r.due_at <= clock_timestamp()
               AND j.enabled AND r.id <> ALL (?)
             ORDER BY r.priority, r.due_at, r.id
             LIMIT ?
               FOR UPDATE OF r SKIP LOCKED
            """;

    /* Step 3 start (I3, I5, I6): only for a run whose row we locked and a slot whose row we locked.
       New token = (epoch read in this txn, nextval('fence_seq')); lease on the DB clock (I1);
       started_at keeps the first start (fire lag); attempt + 1. first_start reads started_at before
       this update (the row is already locked by us, so the sub-select sees the version we update):
       only a run that never started reports fire lag, not a retry and not a replayed dead letter. */
    private static final String START = """
            UPDATE job_run r
               SET state = 'RUNNING', slot_no = ?, owner_node = ?,
                   fencing_epoch = ?, fencing_seq = nextval('fence_seq'),
                   lease_until = clock_timestamp() + ? * interval '1 millisecond',
                   started_at = coalesce(r.started_at, clock_timestamp()), finished_at = NULL,
                   attempt = r.attempt + 1, coalesced_count = coalesce(?, r.coalesced_count)
              FROM (SELECT id, started_at IS NULL AS first_start FROM job_run WHERE id = ?) prev
             WHERE r.id = prev.id AND r.state IN ('SCHEDULED', 'RETRY_WAIT')
            RETURNING r.id, r.job_id, r.tenant_id, r.partition_id, r.job_version, r.priority, r.slot_no,
                      r.fencing_epoch, r.fencing_seq, r.idem_key, r.scheduled_at, r.attempt, prev.first_start,
                      (extract(epoch FROM r.max_run_time) * 1000)::bigint AS max_run_time_ms,
                      clock_timestamp() AS claimed_at, r.lease_until, r.checkpoint::text AS checkpoint,
                      r.coalesced_count,
                      (extract(epoch FROM (r.started_at - r.scheduled_at)) * 1000)::bigint AS lag_ms,
                      (SELECT j.handler FROM job j WHERE j.id = r.job_id) AS handler
            """;

    // ------------------------------------------------------------------ orphan sweep (plan 06.6)

    /* Hint only (I1): expired RUNNING runs in ANY partition, oldest expiry first (index run_expired). */
    private static final String EXPIRED = """
            SELECT id, tenant_id, slot_no
              FROM job_run
             WHERE state = 'RUNNING' AND lease_until < clock_timestamp()
             ORDER BY lease_until
             LIMIT ?
            """;

    /* I2, I3: lock the orphan's slot row first; SKIP LOCKED so two sweepers never wait on each
       other: whoever holds the slot owns the takeover. */
    private static final String LOCK_ONE_SLOT = """
            SELECT 1 FROM tenant_slot WHERE tenant_id = ? AND slot_no = ? FOR UPDATE SKIP LOCKED
            """;

    private static final String TENANT_PAUSED = "SELECT paused FROM tenant WHERE id = ?";

    // ------------------------------------------------------------------ fenced holder writes (I4)

    /* I4: renew only a lease that is still valid; an expired lease can never be renewed, even if
       nobody has taken the run over yet (this is what makes slot reuse race-free). */
    static final String RENEW = """
            UPDATE job_run SET lease_until = clock_timestamp() + ? * interval '1 millisecond'
             WHERE id = ? AND (fencing_epoch, fencing_seq) = (?, ?)
               AND state = 'RUNNING' AND lease_until >= clock_timestamp()
            """;

    /* I4: a stale node can never move a checkpoint backward or overwrite the new owner's. */
    private static final String CHECKPOINT = """
            UPDATE job_run SET checkpoint = ?::jsonb
             WHERE id = ? AND (fencing_epoch, fencing_seq) = (?, ?)
               AND state = 'RUNNING' AND lease_until >= clock_timestamp()
            """;

    /* I3, I4: success frees the slot (slot_no = NULL) in the same fenced statement, so a late commit
       from a zombie can never free a slot that now belongs to another run. No slot lock taken. */
    private static final String COMMIT_SUCCESS = """
            UPDATE job_run SET state = 'SUCCEEDED', finished_at = clock_timestamp(), slot_no = NULL
             WHERE id = ? AND (fencing_epoch, fencing_seq) = (?, ?)
               AND state = 'RUNNING' AND lease_until >= clock_timestamp()
            """;

    /* I1, I3, I4: retry waits in the same due index (due_at on the DB clock); slot freed in the
       fenced statement. */
    private static final String COMMIT_RETRY = """
            UPDATE job_run
               SET state = 'RETRY_WAIT', due_at = clock_timestamp() + ? * interval '1 millisecond',
                   slot_no = NULL, last_error = ?
             WHERE id = ? AND (fencing_epoch, fencing_seq) = (?, ?)
               AND state = 'RUNNING' AND lease_until >= clock_timestamp()
            """;

    /* I3, I4: dead letter frees the slot in the fenced statement; the dead_letter row is written in
       the same transaction. */
    private static final String COMMIT_DEAD = """
            UPDATE job_run
               SET state = 'DEAD_LETTERED', finished_at = clock_timestamp(), slot_no = NULL, last_error = ?
             WHERE id = ? AND (fencing_epoch, fencing_seq) = (?, ?)
               AND state = 'RUNNING' AND lease_until >= clock_timestamp()
            RETURNING tenant_id, attempt
            """;

    /* Upsert: a replayed run that dead-letters again resets its dead-letter row (API contract). */
    private static final String UPSERT_DEAD_LETTER = """
            INSERT INTO dead_letter (run_id, tenant_id, error_code, attempts, dead_at, replayed_at)
            VALUES (?, ?, ?, ?, clock_timestamp(), NULL)
            ON CONFLICT (run_id) DO UPDATE
               SET tenant_id = EXCLUDED.tenant_id, error_code = EXCLUDED.error_code,
                   attempts = EXCLUDED.attempts, dead_at = EXCLUDED.dead_at, replayed_at = NULL
            """;

    private static final int MAX_LAST_ERROR = 500;
    private static final int MAX_CANDIDATE_ROUNDS = 4;
    private static final Duration DEFAULT_DEAD_AFTER = Duration.ofSeconds(6);

    private final DataSource dataSource;
    private final long runLeaseMs;
    private final RetryDecider retryDecider;
    private final ClaimObserver observer;
    private final FencingEpochReader epochReader = new FencingEpochReader();
    private final MisfireResolver misfire;
    private final ClaimHooks hooks;

    /**
     * @param dataSource the run store
     * @param runLease lease granted by each claim and renewal (default 10 s)
     * @param retryDecider supplies the lease-lost limit decision ({@code FENCING_CONFLICT})
     * @param observer metrics callbacks
     */
    public JdbcClaimProtocol(DataSource dataSource, Duration runLease, RetryDecider retryDecider,
                             ClaimObserver observer) {
        this(dataSource, runLease, retryDecider, observer, DEFAULT_DEAD_AFTER, ClaimHooks.NONE);
    }

    /**
     * @param deadAfter {@code membership.dead-after}: which nodes count as live for the misfire
     *     rule ({@link MisfireResolver})
     */
    public JdbcClaimProtocol(DataSource dataSource, Duration runLease, RetryDecider retryDecider,
                             ClaimObserver observer, Duration deadAfter) {
        this(dataSource, runLease, retryDecider, observer, deadAfter, ClaimHooks.NONE);
    }

    JdbcClaimProtocol(DataSource dataSource, Duration runLease, RetryDecider retryDecider,
                      ClaimObserver observer, ClaimHooks hooks) {
        this(dataSource, runLease, retryDecider, observer, DEFAULT_DEAD_AFTER, hooks);
    }

    private JdbcClaimProtocol(DataSource dataSource, Duration runLease, RetryDecider retryDecider,
                              ClaimObserver observer, Duration deadAfter, ClaimHooks hooks) {
        this.misfire = new MisfireResolver(Objects.requireNonNull(deadAfter, "deadAfter"));
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.runLeaseMs = Objects.requireNonNull(runLease, "runLease").toMillis();
        this.retryDecider = Objects.requireNonNull(retryDecider, "retryDecider");
        this.observer = Objects.requireNonNull(observer, "observer");
        this.hooks = Objects.requireNonNull(hooks, "hooks");
        if (runLeaseMs <= 0) {
            throw new IllegalArgumentException("runLease must be > 0");
        }
    }

    // ================================================================== claimDue

    @Override
    public List<ClaimedRun> claimDue(String nodeId, TenantId tenant, Set<Integer> ownedPartitions, int maxRuns) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(ownedPartitions, "ownedPartitions");
        if (maxRuns <= 0 || ownedPartitions.isEmpty()) {
            return List.of();
        }
        long t0 = System.nanoTime();
        List<Started> started = List.of();
        boolean capRejected = false;
        List<Long> reaped = new ArrayList<>();
        List<Long> reapedDead = new ArrayList<>();
        List<TakenOver> takenOver = new ArrayList<>();
        try (Connection c = transaction()) {
            try {
                ClaimOutcome out = claimInTransaction(c, nodeId, tenant, ownedPartitions, maxRuns, reaped, reapedDead,
                        takenOver);
                c.commit(); // I5: tokens are handed out only after this commit is acknowledged
                started = out.started();
                capRejected = out.capRejected();
            } catch (SQLException | RuntimeException e) {
                rollbackQuietly(c);
                throw e;
            }
        } catch (SQLException e) {
            throw new ClaimStoreException("claim failed", e);
        } finally {
            observer.onClaimBatch(tenant, Duration.ofNanos(System.nanoTime() - t0), started.size());
        }
        List<ClaimedRun> runs = new ArrayList<>(started.size());
        reaped.forEach(id -> observer.onReaped(id, false));
        reapedDead.forEach(id -> {
            observer.onReaped(id, true);
            observer.onDeadLettered(id, RetryDecider.FENCING_CONFLICT); // the only reason a reap dead-letters
        });
        if (capRejected) {
            observer.onCapRejected(tenant);
        }
        for (TakenOver t : takenOver) {
            runs.add(t.run());
            observer.onOrphanTakeover(t.run(), t.sinceLastRenewal());
        }
        for (Started s : started) {
            runs.add(s.run());
            if (s.firstStartLag() != null) {
                observer.onFirstStart(s.run(), s.firstStartLag());
            }
        }
        return runs;
    }

    private record ClaimOutcome(List<Started> started, boolean capRejected) {
    }

    /** A started run and, for its first start ever, the fire lag (started_at - scheduled_at). */
    private record Started(ClaimedRun run, Duration firstStartLag) {
    }

    private ClaimOutcome claimInTransaction(Connection c, String nodeId, TenantId tenant, Set<Integer> owned,
                                            int maxRuns, List<Long> reaped, List<Long> reapedDead,
                                            List<TakenOver> takenOver)
            throws SQLException {
        int cap;
        try (PreparedStatement ps = c.prepareStatement(TENANT)) {
            ps.setString(1, tenant.value());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next() || rs.getBoolean("paused")) {
                    return new ClaimOutcome(List.of(), false);
                }
                cap = rs.getInt("configured_cap");
            }
        }
        long epoch = epochReader.read(c); // I5, fail closed
        Array partitions = c.createArrayOf("int4", owned.toArray());

        int due;
        try (PreparedStatement ps = c.prepareStatement(PEEK_DUE)) {
            ps.setArray(1, partitions);
            ps.setString(2, tenant.value());
            ps.setInt(3, maxRuns);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                due = rs.getInt(1);
            }
        }
        if (due == 0) {
            return new ClaimOutcome(List.of(), false);
        }
        if (draining(c, tenant, cap)) {
            return new ClaimOutcome(List.of(), true);
        }

        List<Integer> locked = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(LOCK_SLOTS)) {
            ps.setString(1, tenant.value());
            ps.setInt(2, cap);
            ps.setInt(3, Math.min(maxRuns, due));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    locked.add(rs.getInt(1));
                }
            }
        }
        hooks.afterSlotsLocked(tenant, List.copyOf(locked));
        if (locked.isEmpty()) {
            return new ClaimOutcome(List.of(), true);
        }

        Map<Integer, long[]> holders = new HashMap<>(); // slot -> {runId, expired}
        try (PreparedStatement ps = c.prepareStatement(HOLDERS)) {
            ps.setString(1, tenant.value());
            ps.setArray(2, c.createArrayOf("int4", locked.toArray()));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    holders.put(rs.getInt("slot_no"), new long[] {rs.getLong("id"), rs.getBoolean("expired") ? 1 : 0});
                }
            }
        }
        List<Integer> free = new ArrayList<>();
        for (int slot : locked) {
            long[] holder = holders.get(slot);
            if (holder == null) {
                free.add(slot);
            } else if (holder[1] == 1) {
                hooks.beforeReap(holder[0]);
                Reaped reapedRun = reap(c, holder[0], tenant.value(), slot);
                ReapResult r = reapedRun.result();
                hooks.afterReap(holder[0], r != ReapResult.STILL_VALID);
                if (r == ReapResult.DEAD_LETTERED) {
                    reapedDead.add(holder[0]);
                    free.add(slot);
                } else if (r == ReapResult.FREED) {
                    reaped.add(holder[0]);
                    // Restart the orphan in the slot we just freed, as the sweep does: the reap moved
                    // it to RETRY_WAIT with due_at = now, so leaving it to the candidate scan would
                    // queue it behind the tenant's backlog (or strand it in a partition we do not own).
                    Instant lastRenewal = reapedRun.expiredLease().minusMillis(runLeaseMs);
                    Optional<ClaimedRun> restarted = start(c, holder[0], slot, nodeId, epoch, null, true)
                            .map(Started::run);
                    if (restarted.isPresent()) {
                        takenOver.add(new TakenOver(restarted.get(),
                                nonNegative(Duration.between(lastRenewal, restarted.get().claimedAt()))));
                    } else {
                        free.add(slot);
                    }
                }
            }
            // a holder with a valid lease keeps its slot; we only hold the row lock until commit
        }
        if (free.isEmpty()) {
            return new ClaimOutcome(List.of(), takenOver.isEmpty());
        }
        return new ClaimOutcome(startDue(c, nodeId, tenant, partitions, epoch, free), false);
    }

    private boolean draining(Connection c, TenantId tenant, int cap) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(DRAINING)) {
            ps.setString(1, tenant.value());
            ps.setInt(2, cap);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    /** Picks due runs (applying misfire policies) and starts one per free slot. */
    private List<Started> startDue(Connection c, String nodeId, TenantId tenant, Array partitions, long epoch,
                                      List<Integer> freeSlots) throws SQLException {
        List<MisfireResolver.Start> starts = new ArrayList<>();
        List<Long> seen = new ArrayList<>();
        MisfireResolver.Session session = misfire.session(c, tenant.value());
        for (int round = 0; round < MAX_CANDIDATE_ROUNDS && starts.size() < freeSlots.size(); round++) {
            List<MisfireResolver.Candidate> candidates = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(CANDIDATES)) {
                ps.setArray(1, partitions);
                ps.setString(2, tenant.value());
                ps.setArray(3, c.createArrayOf("int8", seen.toArray()));
                ps.setInt(4, freeSlots.size() - starts.size());
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        candidates.add(new MisfireResolver.Candidate(
                                rs.getLong("id"), rs.getObject("job_id", UUID.class), rs.getString("state"),
                                rs.getBoolean("misfire_exempt"), instant(rs, "scheduled_at"), rs.getString("misfire"),
                                Duration.ofMillis(rs.getLong("threshold_ms")), instant(rs, "db_now")));
                    }
                }
            }
            if (candidates.isEmpty()) {
                break;
            }
            candidates.forEach(cand -> seen.add(cand.runId()));
            boolean absorbed = session.resolve(candidates, freeSlots.size(), starts);
            if (!absorbed) {
                break; // nothing was skipped: no reason to expect more candidates this transaction
            }
        }
        List<Started> started = new ArrayList<>(starts.size());
        for (int i = 0; i < starts.size(); i++) {
            start(c, starts.get(i).runId(), freeSlots.get(i), nodeId, epoch, starts.get(i).coalescedCount(), false)
                    .ifPresent(started::add);
        }
        return started;
    }

    // ================================================================== orphan sweep

    @Override
    public List<ClaimedRun> sweepOrphans(String nodeId, int maxRuns) {
        Objects.requireNonNull(nodeId, "nodeId");
        if (maxRuns <= 0) {
            return List.of();
        }
        record Orphan(long runId, String tenantId, int slot) {
        }
        List<Orphan> orphans = new ArrayList<>();
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(EXPIRED)) {
            ps.setInt(1, maxRuns);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    orphans.add(new Orphan(rs.getLong("id"), rs.getString("tenant_id"), rs.getInt("slot_no")));
                }
            }
        } catch (SQLException e) {
            throw new ClaimStoreException("orphan scan failed", e);
        }
        List<ClaimedRun> taken = new ArrayList<>();
        for (Orphan o : orphans) {
            takeOver(nodeId, o.runId(), o.tenantId(), o.slot()).ifPresent(t -> {
                taken.add(t.run());
                observer.onOrphanTakeover(t.run(), t.sinceLastRenewal());
            });
        }
        return taken;
    }

    /**
     * A restarted orphan and the time from its dead owner's last successful renewal (or claim) to
     * this restart, both on the DB clock: the expired {@code lease_until} minus the run lease is
     * the last moment the owner was proven alive. Bounded by lease + sweep + claim (10.7 s by
     * default); assumes every node grants the same run lease.
     */
    private record TakenOver(ClaimedRun run, Duration sinceLastRenewal) {
    }

    /**
     * One orphan, one transaction: lock its slot, reap it, restart it in the same slot with a new
     * (higher) token from its checkpoint. Empty if another node got there first, the holder renewed
     * in time, the run hit the lease-lost limit, or the tenant is paused (then it is only reaped).
     */
    private Optional<TakenOver> takeOver(String nodeId, long runId, String tenantId, int slot) {
        boolean reapedOnly = false;
        boolean dead = false;
        Optional<TakenOver> result = Optional.empty();
        try (Connection c = transaction()) {
            try {
                long epoch = epochReader.read(c); // I5, fail closed before touching anything
                boolean locked;
                try (PreparedStatement ps = c.prepareStatement(LOCK_ONE_SLOT)) {
                    ps.setString(1, tenantId);
                    ps.setInt(2, slot);
                    try (ResultSet rs = ps.executeQuery()) {
                        locked = rs.next();
                    }
                }
                if (!locked) {
                    c.rollback();
                    return Optional.empty();
                }
                Reaped r = reap(c, runId, tenantId, slot);
                if (r.result() == ReapResult.STILL_VALID) {
                    c.rollback();
                    return Optional.empty();
                }
                dead = r.result() == ReapResult.DEAD_LETTERED;
                if (!dead && !paused(c, tenantId)) {
                    Instant lastRenewal = r.expiredLease().minusMillis(runLeaseMs);
                    result = start(c, runId, slot, nodeId, epoch, null, true).map(Started::run)
                            .map(run -> new TakenOver(run, nonNegative(Duration.between(lastRenewal, run.claimedAt()))));
                }
                reapedOnly = result.isEmpty();
                c.commit(); // I5
            } catch (SQLException | RuntimeException e) {
                rollbackQuietly(c);
                throw e;
            }
        } catch (SQLException e) {
            throw new ClaimStoreException("orphan takeover failed", e);
        }
        observer.onReaped(runId, dead);
        if (dead) {
            observer.onDeadLettered(runId, RetryDecider.FENCING_CONFLICT);
        }
        if (reapedOnly) {
            return Optional.empty();
        }
        return result;
    }

    private boolean paused(Connection c, String tenantId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(TENANT_PAUSED)) {
            ps.setString(1, tenantId);
            try (ResultSet rs = ps.executeQuery()) {
                return !rs.next() || rs.getBoolean(1);
            }
        }
    }

    // ================================================================== shared steps

    private enum ReapResult { FREED, DEAD_LETTERED, STILL_VALID }

    /** Outcome of a reap and the expired {@code lease_until} it found (null if still valid). */
    private record Reaped(ReapResult result, Instant expiredLease) {
    }

    private static Duration nonNegative(Duration d) {
        return d.isNegative() ? Duration.ZERO : d;
    }

    /** Conditional reap of {@code runId} in {@code (tenant, slot)}; caller holds the slot lock. */
    private Reaped reap(Connection c, long runId, String tenantId, int slot) throws SQLException {
        int leaseLost;
        Instant expiredLease;
        try (PreparedStatement ps = c.prepareStatement(REAP)) {
            ps.setLong(1, runId);
            ps.setString(2, tenantId);
            ps.setInt(3, slot);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return new Reaped(ReapResult.STILL_VALID, null); // holder renewed (or committed) in time
                }
                leaseLost = rs.getInt("lease_lost_count");
                expiredLease = instant(rs, "lease_until");
            }
        }
        Optional<RunFailure.DeadLetter> limit = retryDecider.onLeaseLost(leaseLost);
        if (limit.isEmpty()) {
            return new Reaped(ReapResult.FREED, expiredLease);
        }
        try (PreparedStatement ps = c.prepareStatement(DEAD_LETTER_REAPED)) {
            ps.setString(1, lastError(limit.get()));
            ps.setLong(2, runId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    upsertDeadLetter(c, runId, rs.getString("tenant_id"), limit.get().errorCode(), rs.getInt("attempt"));
                }
            }
        }
        return new Reaped(ReapResult.DEAD_LETTERED, expiredLease);
    }

    private Optional<Started> start(Connection c, long runId, int slot, String nodeId, long epoch,
                                       Integer coalesced, boolean recovered) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(START)) {
            ps.setInt(1, slot);
            ps.setString(2, nodeId);
            ps.setLong(3, epoch);
            ps.setLong(4, runLeaseMs);
            if (coalesced == null) {
                ps.setNull(5, Types.INTEGER);
            } else {
                ps.setInt(5, coalesced);
            }
            ps.setLong(6, runId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                ClaimedRun run = toClaimedRun(rs, recovered);
                boolean first = !recovered && rs.getBoolean("first_start");
                return Optional.of(new Started(run,
                        first ? Duration.ofMillis(Math.max(0, rs.getLong("lag_ms"))) : null));
            }
        }
    }

    private static ClaimedRun toClaimedRun(ResultSet rs, boolean recovered) throws SQLException {
        return new ClaimedRun(
                rs.getLong("id"),
                rs.getObject("job_id", UUID.class),
                TenantId.of(rs.getString("tenant_id")),
                rs.getInt("partition_id"),
                rs.getLong("job_version"),
                HandlerType.valueOf(rs.getString("handler")),
                rs.getInt("priority"),
                rs.getInt("slot_no"),
                new FencingToken(rs.getLong("fencing_epoch"), rs.getLong("fencing_seq")),
                rs.getString("idem_key"),
                instant(rs, "scheduled_at"),
                rs.getInt("attempt"),
                Duration.ofMillis(rs.getLong("max_run_time_ms")),
                instant(rs, "claimed_at"),
                instant(rs, "lease_until"),
                rs.getString("checkpoint"),
                rs.getInt("coalesced_count"),
                recovered);
    }

    // ================================================================== fenced holder writes

    @Override
    public boolean renew(long runId, FencingToken token) {
        return fencedUpdate(RENEW, "renew", ps -> {
            ps.setLong(1, runLeaseMs);
            ps.setLong(2, runId);
            ps.setLong(3, token.epoch());
            ps.setLong(4, token.seq());
        });
    }

    @Override
    public boolean checkpoint(long runId, FencingToken token, String checkpointJson) {
        Objects.requireNonNull(checkpointJson, "checkpointJson");
        return fencedUpdate(CHECKPOINT, "checkpoint", ps -> {
            ps.setString(1, checkpointJson);
            ps.setLong(2, runId);
            ps.setLong(3, token.epoch());
            ps.setLong(4, token.seq());
        });
    }

    @Override
    public boolean commitSuccess(long runId, FencingToken token) {
        return fencedUpdate(COMMIT_SUCCESS, "commit", ps -> {
            ps.setLong(1, runId);
            ps.setLong(2, token.epoch());
            ps.setLong(3, token.seq());
        });
    }

    @Override
    public boolean commitFailure(long runId, FencingToken token, RunFailure failure) {
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(failure, "failure");
        if (failure instanceof RunFailure.Retry retry) {
            return fencedUpdate(COMMIT_RETRY, "commit retry", ps -> {
                ps.setLong(1, retry.delay().toMillis());
                ps.setString(2, lastError(retry));
                ps.setLong(3, runId);
                ps.setLong(4, token.epoch());
                ps.setLong(5, token.seq());
            });
        }
        RunFailure.DeadLetter dead = (RunFailure.DeadLetter) failure;
        try (Connection c = transaction()) {
            try {
                boolean applied = false;
                try (PreparedStatement ps = c.prepareStatement(COMMIT_DEAD)) {
                    ps.setString(1, lastError(dead));
                    ps.setLong(2, runId);
                    ps.setLong(3, token.epoch());
                    ps.setLong(4, token.seq());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            upsertDeadLetter(c, runId, rs.getString("tenant_id"), dead.errorCode(), rs.getInt("attempt"));
                            applied = true;
                        }
                    }
                }
                c.commit();
                if (applied) {
                    observer.onDeadLettered(runId, dead.errorCode());
                }
                return applied;
            } catch (SQLException | RuntimeException e) {
                rollbackQuietly(c);
                throw e;
            }
        } catch (SQLException e) {
            throw new ClaimStoreException("commit dead letter failed", e);
        }
    }

    @FunctionalInterface
    private interface Binder {
        void bind(PreparedStatement ps) throws SQLException;
    }

    /** One fenced statement in autocommit: 1 row = applied, 0 rows = lease lost. */
    private boolean fencedUpdate(String sql, String what, Binder binder) {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(true);
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                binder.bind(ps);
                return ps.executeUpdate() == 1;
            }
        } catch (SQLException e) {
            throw new ClaimStoreException(what + " failed", e);
        }
    }

    private static void upsertDeadLetter(Connection c, long runId, String tenantId, String code, int attempts)
            throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(UPSERT_DEAD_LETTER)) {
            ps.setLong(1, runId);
            ps.setString(2, tenantId);
            ps.setString(3, code);
            ps.setInt(4, attempts);
            ps.executeUpdate();
        }
    }

    /** {@code "CODE: sanitised message"} (the API parses the leading code). Never payload data. */
    static String lastError(RunFailure failure) {
        String message = failure.safeMessage() == null ? "" : failure.safeMessage();
        message = message.replaceAll("[\\p{Cntrl}]", " ").strip();
        String text = message.isEmpty() ? failure.errorCode() : failure.errorCode() + ": " + message;
        return text.length() <= MAX_LAST_ERROR ? text : text.substring(0, MAX_LAST_ERROR);
    }

    private Connection transaction() throws SQLException {
        Connection c = dataSource.getConnection();
        try {
            c.setAutoCommit(false);
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        } catch (SQLException e) {
            c.close();
            throw e;
        }
        return c;
    }

    private static void rollbackQuietly(Connection c) {
        try {
            c.rollback();
        } catch (SQLException ignored) {
            // the connection is broken; closing it discards the transaction anyway
        }
    }

    static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime t = rs.getObject(column, OffsetDateTime.class);
        return t == null ? null : t.toInstant();
    }
}
