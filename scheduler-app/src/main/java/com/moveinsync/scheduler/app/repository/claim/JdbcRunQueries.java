package com.moveinsync.scheduler.app.repository.claim;

import com.moveinsync.scheduler.core.domain.TenantId;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Read-only queries the engine needs around the claim protocol. None of them takes a lock or
 * decides anything: every decision is re-made by {@link JdbcClaimProtocol} under its own locks.
 */
public final class JdbcRunQueries {

    /** What an executor needs from the job definition. {@code payloadJson} may hold PII. */
    public record JobSpec(String payloadJson, int maxAttempts) {
    }

    /* Hint for the poller: unpaused tenants with due runs in the polled partitions (DB clock).
       One EXISTS probe per tenant on the run_due index, cheap even with 50,000 due runs. */
    private static final String TENANTS_WITH_DUE = """
            SELECT t.id FROM tenant t
             WHERE NOT t.paused
               AND EXISTS (SELECT 1 FROM job_run r
                            WHERE r.tenant_id = t.id AND r.partition_id = ANY (?::int[])
                              AND r.state IN ('SCHEDULED', 'RETRY_WAIT') AND r.due_at <= clock_timestamp())
             ORDER BY t.id
            """;

    private static final String JOB_SPEC = "SELECT payload::text AS payload, max_attempts FROM job WHERE id = ?";

    private static final String RUN_COUNTS = """
            SELECT count(*) FILTER (WHERE state = 'RUNNING')       AS running,
                   count(*) FILTER (WHERE state = 'RETRY_WAIT')    AS retrying,
                   count(*) FILTER (WHERE state = 'DEAD_LETTERED') AS dead,
                   count(*) FILTER (WHERE state IN ('SCHEDULED', 'RETRY_WAIT')
                                      AND due_at <= clock_timestamp()) AS due
              FROM job_run
             WHERE state IN ('RUNNING', 'RETRY_WAIT', 'DEAD_LETTERED', 'SCHEDULED')
            """;

    private static final String TENANT_CAPS = """
            SELECT t.id, t.configured_cap, (SELECT count(*) FROM tenant_slot s WHERE s.tenant_id = t.id) AS effective
              FROM tenant t ORDER BY t.id
            """;

    private final JdbcClient jdbc;

    public JdbcRunQueries(DataSource dataSource) {
        this.jdbc = JdbcClient.create(Objects.requireNonNull(dataSource, "dataSource"));
    }

    /** Unpaused tenants with due runs in {@code partitions}, ordered by id. */
    public List<TenantId> tenantsWithDueRuns(Set<Integer> partitions) {
        if (partitions.isEmpty()) {
            return List.of();
        }
        String array = partitions.stream().map(String::valueOf)
                .collect(java.util.stream.Collectors.joining(",", "{", "}"));
        return jdbc.sql(TENANTS_WITH_DUE).param(array).query(String.class).list()
                .stream().map(TenantId::of).toList();
    }

    /** Payload and max attempts of a job, or empty if it no longer exists. */
    public Optional<JobSpec> jobSpec(UUID jobId) {
        return jdbc.sql(JOB_SPEC).param(jobId)
                .query((rs, i) -> new JobSpec(rs.getString("payload"), rs.getInt("max_attempts")))
                .optional();
    }

    /** {running, retrying, dead_lettered, due-and-not-started}. */
    public long[] runCounts() {
        return jdbc.sql(RUN_COUNTS).query((rs, i) -> new long[] {
            rs.getLong("running"), rs.getLong("retrying"), rs.getLong("dead"), rs.getLong("due")}).single();
    }

    /** tenant -> {configured cap, effective cap = slot rows}. */
    public Map<String, int[]> tenantCaps() {
        Map<String, int[]> caps = new LinkedHashMap<>();
        jdbc.sql(TENANT_CAPS).query((rs, i) -> {
            caps.put(rs.getString(1), new int[] {rs.getInt(2), rs.getInt(3)});
            return null;
        }).list();
        return caps;
    }
}
