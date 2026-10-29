package com.moveinsync.scheduler.app.repository.job;

import com.moveinsync.scheduler.app.security.TenantScope;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.JobKind;
import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** {@code job} definitions. Every method is restricted to the caller's {@link TenantScope}. */
@Repository
public class JobRepository {

    private static final String COLUMNS = """
            id, tenant_id, kind, handler, cron, run_at, payload::text AS payload, priority,
            extract(epoch FROM max_run_time) AS max_run_time_s, misfire,
            extract(epoch FROM misfire_threshold) AS misfire_threshold_s, max_attempts, partition_id,
            next_fire_at, version, enabled""";

    private final JdbcClient jdbc;

    public JobRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Inserts a new job definition. The row's tenant must be inside the scope. */
    public void insert(TenantScope scope, JobRow job) {
        if (!scope.includes(job.tenantId())) {
            throw new IllegalArgumentException("job tenant outside the caller's scope");
        }
        jdbc.sql("""
                        INSERT INTO job (id, tenant_id, kind, handler, cron, run_at, payload, priority, max_run_time,
                                         misfire, misfire_threshold, max_attempts, partition_id, next_fire_at,
                                         version, enabled)
                        VALUES (:id, :tenant, :kind, :handler, :cron, :runAt, CAST(:payload AS jsonb), :priority,
                                :maxRunTimeMs * interval '1 millisecond', :misfire,
                                :misfireThresholdMs * interval '1 millisecond', :maxAttempts, :partition,
                                :nextFireAt, :version, :enabled)""")
                .param("id", job.id())
                .param("tenant", job.tenantId())
                .param("kind", job.kind().name())
                .param("handler", job.handler().name())
                .param("cron", job.cron(), java.sql.Types.VARCHAR)
                .param("runAt", timestamp(job.runAt()), java.sql.Types.TIMESTAMP_WITH_TIMEZONE)
                .param("payload", job.payloadJson())
                .param("priority", job.priority())
                .param("maxRunTimeMs", job.maxRunTime().toMillis())
                .param("misfire", job.misfire().name())
                .param("misfireThresholdMs", job.misfireThreshold().toMillis())
                .param("maxAttempts", job.maxAttempts())
                .param("partition", job.partitionId())
                .param("nextFireAt", timestamp(job.nextFireAt()), java.sql.Types.TIMESTAMP_WITH_TIMEZONE)
                .param("version", job.version())
                .param("enabled", job.enabled())
                .update();
    }

    public Optional<JobRow> find(TenantScope scope, UUID id) {
        return scope.bind(jdbc.sql("SELECT " + COLUMNS + " FROM job WHERE id = :id AND " + scope.predicate("tenant_id")))
                .param("id", id)
                .query(JobRepository::map)
                .optional();
    }

    /**
     * Reads the job and takes a {@code FOR SHARE} lock, so a concurrent disable waits for this
     * transaction (or this one sees {@code enabled = false}). Used before inserting a manual run.
     */
    public Optional<JobRow> findForShare(TenantScope scope, UUID id) {
        return scope.bind(jdbc.sql("SELECT " + COLUMNS + " FROM job WHERE id = :id AND "
                        + scope.predicate("tenant_id") + " FOR SHARE"))
                .param("id", id)
                .query(JobRepository::map)
                .optional();
    }

    /** Sets {@code enabled = false}; false when the job is not in the scope. */
    public boolean disable(TenantScope scope, UUID id) {
        return scope.bind(jdbc.sql("UPDATE job SET enabled = false WHERE id = :id AND " + scope.predicate("tenant_id")))
                .param("id", id)
                .update() == 1;
    }

    private static JobRow map(ResultSet rs, int row) throws SQLException {
        return new JobRow(
                rs.getObject("id", UUID.class),
                rs.getString("tenant_id"),
                JobKind.valueOf(rs.getString("kind")),
                HandlerType.valueOf(rs.getString("handler")),
                rs.getString("cron"),
                instant(rs.getTimestamp("run_at")),
                rs.getString("payload"),
                rs.getInt("priority"),
                seconds(rs.getBigDecimal("max_run_time_s")),
                MisfirePolicyType.valueOf(rs.getString("misfire")),
                seconds(rs.getBigDecimal("misfire_threshold_s")),
                rs.getInt("max_attempts"),
                rs.getInt("partition_id"),
                instant(rs.getTimestamp("next_fire_at")),
                rs.getLong("version"),
                rs.getBoolean("enabled"));
    }

    static Duration seconds(java.math.BigDecimal secs) {
        return secs == null ? null : Duration.ofMillis(secs.movePointRight(3).longValue());
    }

    static Instant instant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }

    /** Binds as timestamptz (UTC). */
    static OffsetDateTime timestamp(Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }
}
