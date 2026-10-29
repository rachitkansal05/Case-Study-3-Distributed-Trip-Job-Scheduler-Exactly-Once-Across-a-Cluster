package com.moveinsync.scheduler.app.repository.job;

import com.moveinsync.scheduler.app.security.TenantScope;
import com.moveinsync.scheduler.core.domain.RunState;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * The API's writes to {@code job_run} (manual trigger, cancel on delete) and the run history.
 * Every method is restricted to the caller's {@link TenantScope}.
 *
 * <p>The API only ever writes runs that are not running: it inserts {@code SCHEDULED} runs and
 * moves unstarted ones to {@code CANCELLED}. Claims, commits, retries and slots belong to the
 * Engine's claim protocol.
 */
@Repository
public class JobRunRepository {

    /** Error code = leading upper-case token of last_error ("CODE: sanitised message"). */
    static final String RUN_COLUMNS = """
            r.id, r.job_id, r.tenant_id, r.state, r.attempt, r.lease_lost_count, r.scheduled_at, r.due_at,
            r.started_at, r.finished_at,
            CASE WHEN r.started_at IS NULL THEN NULL
                 ELSE round(extract(epoch FROM (r.started_at - r.scheduled_at)) * 1000)::bigint END AS lag_ms,
            substring(r.last_error FROM '^[A-Z][A-Z0-9_]*') AS error_code,
            r.coalesced_count, position(':manual:' IN r.idem_key) > 0 AS manual""";

    private final JdbcClient jdbc;

    public JobRunRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts a manual run due now ({@code scheduled_at = due_at = clock_timestamp()}), copying
     * partition, priority, version and max run time from the job. Returns empty when a run with
     * this {@code idemKey} already exists (the {@code UNIQUE (idem_key)} constraint deduplicates).
     */
    public Optional<Long> insertManualRun(TenantScope scope, JobRow job, String idemKey) {
        if (!scope.includes(job.tenantId())) {
            return Optional.empty();
        }
        return jdbc.sql("""
                        INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                             scheduled_at, due_at, idem_key, state)
                        SELECT j.id, j.tenant_id, j.partition_id, j.priority, j.version, j.max_run_time,
                               clock_timestamp(), clock_timestamp(), :idemKey, 'SCHEDULED'
                          FROM job j
                         WHERE j.id = :jobId AND j.tenant_id = :tenant
                        ON CONFLICT (idem_key) DO NOTHING
                        RETURNING id""")
                .param("idemKey", idemKey)
                .param("jobId", job.id())
                .param("tenant", job.tenantId())
                .query(Long.class)
                .optional();
    }

    public Optional<RunRow> findByIdemKey(TenantScope scope, String idemKey) {
        return scope.bind(jdbc.sql("SELECT " + RUN_COLUMNS + " FROM job_run r WHERE r.idem_key = :key AND "
                        + scope.predicate("r.tenant_id")))
                .param("key", idemKey)
                .query(JobRunRepository::map)
                .optional();
    }

    public Optional<RunRow> find(TenantScope scope, long runId) {
        return scope.bind(jdbc.sql("SELECT " + RUN_COLUMNS + " FROM job_run r WHERE r.id = :id AND "
                        + scope.predicate("r.tenant_id")))
                .param("id", runId)
                .query(JobRunRepository::map)
                .optional();
    }

    /**
     * Cancels the job's runs that have not started ({@code SCHEDULED}, {@code RETRY_WAIT},
     * {@code PAUSED}). A run claimed concurrently is already {@code RUNNING} when this statement
     * re-checks the row, so it is left alone and finishes normally.
     */
    public int cancelUnstarted(TenantScope scope, UUID jobId) {
        return scope.bind(jdbc.sql("""
                        UPDATE job_run SET state = 'CANCELLED', finished_at = clock_timestamp()
                         WHERE job_id = :jobId AND state IN ('SCHEDULED', 'RETRY_WAIT', 'PAUSED') AND """
                        + scope.predicate("tenant_id")))
                .param("jobId", jobId)
                .update();
    }

    /** One page of a job's runs, newest occurrence first; fetches {@code limit} rows. */
    public List<RunRow> page(TenantScope scope, UUID jobId, int offset, int limit) {
        return scope.bind(jdbc.sql("SELECT " + RUN_COLUMNS + " FROM job_run r WHERE r.job_id = :jobId AND "
                        + scope.predicate("r.tenant_id")
                        + " ORDER BY r.scheduled_at DESC, r.id DESC OFFSET :offset LIMIT :limit"))
                .param("jobId", jobId)
                .param("offset", offset)
                .param("limit", limit)
                .query(JobRunRepository::map)
                .list();
    }

    static RunRow map(ResultSet rs, int row) throws SQLException {
        long lag = rs.getLong("lag_ms");
        Long lagMillis = rs.wasNull() ? null : lag;
        return new RunRow(
                rs.getLong("id"),
                rs.getObject("job_id", UUID.class),
                rs.getString("tenant_id"),
                RunState.valueOf(rs.getString("state")),
                rs.getInt("attempt"),
                rs.getInt("lease_lost_count"),
                JobRepository.instant(rs.getTimestamp("scheduled_at")),
                JobRepository.instant(rs.getTimestamp("due_at")),
                JobRepository.instant(rs.getTimestamp("started_at")),
                JobRepository.instant(rs.getTimestamp("finished_at")),
                lagMillis,
                rs.getString("error_code"),
                rs.getInt("coalesced_count"),
                rs.getBoolean("manual"));
    }
}
