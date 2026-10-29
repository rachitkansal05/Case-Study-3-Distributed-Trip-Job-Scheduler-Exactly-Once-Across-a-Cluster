package com.moveinsync.scheduler.app.repository.deadletter;

import com.moveinsync.scheduler.app.security.TenantScope;
import com.moveinsync.scheduler.core.domain.RunState;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * {@code dead_letter} rows with their run's current state. Every method is restricted to the
 * caller's {@link TenantScope}. Only error codes are exposed, never payloads or messages.
 */
@Repository
public class DeadLetterRepository {

    private static final String COLUMNS = """
            d.run_id, r.job_id, d.tenant_id, d.error_code, d.attempts, d.dead_at, d.replayed_at, r.state,
            r.idem_key, r.due_at, j.enabled AS job_enabled""";
    private static final String FROM = """
             FROM dead_letter d
             JOIN job_run r ON r.id = d.run_id
             JOIN job j ON j.id = r.job_id
            """;

    private final JdbcClient jdbc;

    public DeadLetterRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Newest first; fetches {@code limit} rows from {@code offset}. */
    public List<DeadLetterRow> page(TenantScope scope, int offset, int limit) {
        return scope.bind(jdbc.sql("SELECT " + COLUMNS + FROM + " WHERE " + scope.predicate("d.tenant_id")
                        + " ORDER BY d.dead_at DESC, d.run_id DESC OFFSET :offset LIMIT :limit"))
                .param("offset", offset)
                .param("limit", limit)
                .query(DeadLetterRepository::map)
                .list();
    }

    /** Reads a dead letter and locks it and its run ({@code FOR UPDATE}) for a replay. */
    public Optional<DeadLetterRow> lockForReplay(TenantScope scope, long runId) {
        return scope.bind(jdbc.sql("SELECT " + COLUMNS + FROM + " WHERE d.run_id = :runId AND "
                        + scope.predicate("d.tenant_id") + " FOR UPDATE OF d, r"))
                .param("runId", runId)
                .query(DeadLetterRepository::map)
                .optional();
    }

    /**
     * Puts the same run row back to {@code SCHEDULED}, due now, with {@code attempt} and
     * {@code lease_lost_count} reset and no lease or slot. {@code idem_key} and the checkpoint are
     * kept, so items delivered before the run died are not sent again. Returns 0 unless the run
     * is {@code DEAD_LETTERED}.
     */
    public int rescheduleRun(TenantScope scope, long runId) {
        return scope.bind(jdbc.sql("""
                        UPDATE job_run
                           SET state = 'SCHEDULED', due_at = clock_timestamp(), attempt = 0, lease_lost_count = 0,
                               slot_no = NULL, owner_node = NULL, lease_until = NULL, finished_at = NULL
                         WHERE id = :runId AND state = 'DEAD_LETTERED' AND """ + scope.predicate("tenant_id")))
                .param("runId", runId)
                .update();
    }

    public void markReplayed(TenantScope scope, long runId) {
        scope.bind(jdbc.sql("UPDATE dead_letter SET replayed_at = clock_timestamp() WHERE run_id = :runId AND "
                        + scope.predicate("tenant_id")))
                .param("runId", runId)
                .update();
    }

    private static DeadLetterRow map(ResultSet rs, int row) throws SQLException {
        return new DeadLetterRow(
                rs.getLong("run_id"),
                rs.getObject("job_id", UUID.class),
                rs.getString("tenant_id"),
                rs.getString("error_code"),
                rs.getInt("attempts"),
                instant(rs.getTimestamp("dead_at")),
                instant(rs.getTimestamp("replayed_at")),
                RunState.valueOf(rs.getString("state")),
                rs.getString("idem_key"),
                instant(rs.getTimestamp("due_at")),
                rs.getBoolean("job_enabled"));
    }

    private static Instant instant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }

    /**
     * @param runState current state of the dead-lettered run ({@code SCHEDULED} etc. after a replay)
     */
    public record DeadLetterRow(long runId, UUID jobId, String tenantId, String errorCode, int attempts,
                                Instant deadAt, Instant replayedAt, RunState runState, String idemKey,
                                Instant dueAt, boolean jobEnabled) {
    }
}
