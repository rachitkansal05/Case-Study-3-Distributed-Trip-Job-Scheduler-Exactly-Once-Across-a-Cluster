package com.moveinsync.scheduler.app.engine.support;

import com.moveinsync.scheduler.core.cluster.PartitionHasher;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.IntStream;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Test data written straight to the tables (test setup is allowed to bypass the claim protocol;
 * production code is not).
 */
public final class Fixtures {

    /** All 128 partitions. */
    public static final Set<Integer> ALL_PARTITIONS =
            new TreeSet<>(IntStream.range(0, 128).boxed().toList());

    private final JdbcClient jdbc;

    public Fixtures(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Tenant in UTC with slot rows 0..cap-1. */
    public void tenant(String id, int cap) {
        tenant(id, cap, "UTC");
    }

    public void tenant(String id, int cap, String zone) {
        jdbc.sql("INSERT INTO tenant (id, time_zone, configured_cap) VALUES (?, ?, ?)")
                .params(id, zone, cap).update();
        jdbc.sql("INSERT INTO tenant_slot (tenant_id, slot_no) SELECT ?, g FROM generate_series(0, ? - 1) g")
                .params(id, cap).update();
    }

    /** Job definition builder; {@link JobSpec#insert()} writes it. */
    public JobSpec job(String tenant) {
        return new JobSpec(tenant);
    }

    /** Fluent job definition. */
    public final class JobSpec {
        private final String tenant;
        private UUID id = UUID.randomUUID();
        private HandlerType handler = HandlerType.ROSTER_GEN;
        private String payload = "{\"shiftDate\":\"2026-11-01\",\"siteId\":\"s1\"}";
        private int priority = 5;
        private Duration maxRunTime = Duration.ofMinutes(5);
        private MisfirePolicyType misfire = MisfirePolicyType.FIRE_NOW;
        private Duration misfireThreshold = Duration.ofSeconds(60);
        private int maxAttempts = 5;
        private Integer partition;

        private JobSpec(String tenant) {
            this.tenant = tenant;
        }

        public JobSpec id(UUID v) { id = v; return this; }
        public JobSpec handler(HandlerType v) { handler = v; return this; }
        public JobSpec payload(String v) { payload = v; return this; }
        public JobSpec priority(int v) { priority = v; return this; }
        public JobSpec maxRunTime(Duration v) { maxRunTime = v; return this; }
        public JobSpec misfire(MisfirePolicyType v) { misfire = v; return this; }
        public JobSpec misfireThreshold(Duration v) { misfireThreshold = v; return this; }
        public JobSpec maxAttempts(int v) { maxAttempts = v; return this; }
        public JobSpec partition(int v) { partition = v; return this; }

        /** Inserts a one-off job (run_at = now) without runs; returns its id. */
        public UUID insert() {
            int p = partition != null ? partition : PartitionHasher.partitionOf(id);
            jdbc.sql("""
                    INSERT INTO job (id, tenant_id, kind, handler, run_at, payload, priority, max_run_time,
                                     misfire, misfire_threshold, max_attempts, partition_id, version)
                    VALUES (?, ?, 'ONE_OFF', ?, clock_timestamp(), ?::jsonb, ?, ? * interval '1 millisecond',
                            ?, ? * interval '1 millisecond', ?, ?, 0)""")
                    .params(id, tenant, handler.name(), payload, priority, maxRunTime.toMillis(), misfire.name(),
                            misfireThreshold.toMillis(), maxAttempts, p)
                    .update();
            return id;
        }
    }

    /** Inserts one SCHEDULED run of {@code job} at {@code scheduledAt} (due then). */
    public long run(UUID job, Instant scheduledAt) {
        return jdbc.sql("""
                INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                     scheduled_at, due_at, idem_key, state)
                SELECT j.id, j.tenant_id, j.partition_id, j.priority, j.version, j.max_run_time, ?, ?,
                       j.id || ':' || ?, 'SCHEDULED'
                  FROM job j WHERE j.id = ?
                RETURNING id""")
                .params(ts(scheduledAt), ts(scheduledAt), scheduledAt.toString(), job)
                .query(Long.class).single();
    }

    /** Inserts {@code n} SCHEDULED runs of {@code job}, already due, scheduled 1 ms apart. */
    public List<Long> dueRuns(UUID job, int n) {
        return jdbc.sql("""
                INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                     scheduled_at, due_at, idem_key, state)
                SELECT j.id, j.tenant_id, j.partition_id, j.priority, j.version, j.max_run_time,
                       t.at, t.at, j.id || ':' || to_char(t.at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                       'SCHEDULED'
                  FROM job j,
                       LATERAL (SELECT date_trunc('milliseconds', now()) - interval '1 second' - g * interval '1 millisecond' AS at
                                  FROM generate_series(1, ?) g) t
                 WHERE j.id = ?
                RETURNING id""")
                .params(n, job).query(Long.class).list();
    }

    /** Makes a RUNNING run's lease expired by DB time (the "test clock" of plan section 16). */
    public void expireLease(long runId) {
        jdbc.sql("UPDATE job_run SET lease_until = clock_timestamp() - interval '1 millisecond' WHERE id = ?")
                .param(runId).update();
    }

    public String state(long runId) {
        return jdbc.sql("SELECT state FROM job_run WHERE id = ?").param(runId).query(String.class).single();
    }

    public int intColumn(long runId, String column) {
        return jdbc.sql("SELECT " + column + " FROM job_run WHERE id = ?").param(runId).query(Integer.class).single();
    }

    public long count(String sql, Object... params) {
        return jdbc.sql(sql).params(params).query(Long.class).single();
    }

    public static OffsetDateTime ts(Instant i) {
        return OffsetDateTime.ofInstant(i, ZoneOffset.UTC);
    }
}
