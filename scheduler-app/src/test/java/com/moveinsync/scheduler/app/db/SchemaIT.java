package com.moveinsync.scheduler.app.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.app.support.EmbeddedPostgresSupport;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Proves V1 applies on real PostgreSQL 16 and that the database itself enforces the tenant-slot
 * invariants from plan section 10 ("Why the cap holds at every moment"), independent of any
 * application code. Runs Flyway directly against a fresh database (no Spring context).
 */
class SchemaIT {

    private static final String FK_VIOLATION = "23503";
    private static final String UNIQUE_VIOLATION = "23505";
    private static final String CHECK_VIOLATION = "23514";

    private static final AtomicLong SEQ = new AtomicLong();

    private static DataSource dataSource;
    private static MigrateResult migration;
    private JdbcClient jdbc;

    private String tenantA;
    private String tenantB;
    private UUID jobA;

    @BeforeAll
    static void migrate() {
        dataSource = EmbeddedPostgresSupport.createDatabase("schema_it");
        migration = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();
    }

    @BeforeEach
    void seed() {
        jdbc = JdbcClient.create(dataSource);
        long n = SEQ.incrementAndGet();
        tenantA = "tenant-a-" + n;
        tenantB = "tenant-b-" + n;
        createTenant(tenantA, 4);   // slots 0..3
        createTenant(tenantB, 10);  // slots 0..9
        jobA = createJob(tenantA);
    }

    // ------------------------------------------------------------------ positive

    @Test
    void migrationsApplyAndSeedData() {
        assertThat(migration.success).isTrue();
        assertThat(migration.migrationsExecuted).isGreaterThanOrEqualTo(1);

        assertThat(jdbc.sql("SELECT count(*) FROM partition_lease").query(Long.class).single()).isEqualTo(128L);
        assertThat(jdbc.sql("SELECT min(partition_id), max(partition_id) FROM partition_lease")
                .query((rs, i) -> new int[] {rs.getInt(1), rs.getInt(2)}).single())
                .containsExactly(0, 127);
        assertThat(jdbc.sql("SELECT cluster_generation FROM cluster_meta").query(Integer.class).single()).isZero();
        assertThat(jdbc.sql("SELECT count(*) FROM pg_indexes WHERE indexname IN "
                        + "('one_run_per_slot','run_due','run_expired')")
                .query(Long.class).single()).isEqualTo(3L);
    }

    @Test
    void runningRunHoldingOwnSlotIsAccepted() {
        long run = insertRun(jobA, tenantA, "SCHEDULED", null, null);
        int updated = jdbc.sql("""
                UPDATE job_run SET state = 'RUNNING', slot_no = 2, fencing_epoch = 1,
                       fencing_seq = nextval('fence_seq'), lease_until = clock_timestamp() + interval '10 s'
                 WHERE id = ?""").param(run).update();
        assertThat(updated).isEqualTo(1);
    }

    @Test
    void secondClusterMetaRowRejected() {
        assertSqlState(() -> jdbc.sql("INSERT INTO cluster_meta (id, cluster_generation) VALUES (false, 1)").update(),
                CHECK_VIOLATION);
    }

    // ------------------------------------------------------------------ negative (R7 slot constraints)

    @Test
    void runningWithNullSlotRejected() {
        assertSqlState(() -> insertRun(jobA, tenantA, "RUNNING", null, 1L), CHECK_VIOLATION);
    }

    @Test
    void runningWithoutFencingTokenRejected() {
        assertSqlState(() -> insertRun(jobA, tenantA, "RUNNING", (short) 0, null), CHECK_VIOLATION);
    }

    @Test
    void slotOutsideTenantsSetRejected() {
        assertSqlState(() -> insertRun(jobA, tenantA, "RUNNING", (short) 99, 1L), FK_VIOLATION);
    }

    @Test
    void anotherTenantsSlotRejected() {
        // slot 7 exists for tenant B (0..9) but not for tenant A (0..3)
        assertSqlState(() -> insertRun(jobA, tenantA, "RUNNING", (short) 7, 1L), FK_VIOLATION);
    }

    @Test
    void twoRunningRunsInOneSlotRejected() {
        insertRun(jobA, tenantA, "RUNNING", (short) 1, 1L);
        UUID otherJob = createJob(tenantA);
        assertSqlState(() -> insertRun(otherJob, tenantA, "RUNNING", (short) 1, 2L), UNIQUE_VIOLATION);
    }

    @Test
    void deletingSlotInUseRejected() {
        insertRun(jobA, tenantA, "RUNNING", (short) 3, 1L);
        assertSqlState(() -> jdbc.sql("DELETE FROM tenant_slot WHERE tenant_id = ? AND slot_no = 3")
                .param(tenantA).update(), FK_VIOLATION);
    }

    @Test
    void deletingFreeSlotAllowed() {
        int deleted = jdbc.sql("DELETE FROM tenant_slot WHERE tenant_id = ? AND slot_no = 3").param(tenantA).update();
        assertThat(deleted).isEqualTo(1);
    }

    @Test
    void duplicateOccurrenceAndIdemKeyRejected() {
        jdbc.sql("""
                INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                     scheduled_at, due_at, idem_key, state)
                VALUES (?, ?, 0, 5, 0, interval '1 minute', timestamptz '2026-01-01 00:00Z',
                        timestamptz '2026-01-01 00:00Z', ?, 'SCHEDULED')""")
                .params(jobA, tenantA, "k-" + jobA).update();
        assertSqlState(() -> jdbc.sql("""
                INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                     scheduled_at, due_at, idem_key, state)
                VALUES (?, ?, 0, 5, 0, interval '1 minute', timestamptz '2026-01-01 00:00Z',
                        timestamptz '2026-01-01 00:00Z', ?, 'SCHEDULED')""")
                .params(jobA, tenantA, "other-" + jobA).update(), UNIQUE_VIOLATION);
        UUID otherJob = createJob(tenantA);
        assertSqlState(() -> jdbc.sql("""
                INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                     scheduled_at, due_at, idem_key, state)
                VALUES (?, ?, 0, 5, 0, interval '1 minute', clock_timestamp(), clock_timestamp(), ?, 'SCHEDULED')""")
                .params(otherJob, tenantA, "k-" + jobA).update(), UNIQUE_VIOLATION);
    }

    @Test
    void unknownStateRejected() {
        assertSqlState(() -> insertRun(jobA, tenantA, "BOGUS", null, null), CHECK_VIOLATION);
    }

    // ------------------------------------------------------------------ helpers

    private void createTenant(String id, int cap) {
        jdbc.sql("INSERT INTO tenant (id, time_zone, configured_cap) VALUES (?, 'Asia/Kolkata', ?)")
                .params(id, cap).update();
        jdbc.sql("INSERT INTO tenant_slot (tenant_id, slot_no) SELECT ?, s FROM generate_series(0, ? - 1) s")
                .params(id, cap).update();
    }

    private UUID createJob(String tenant) {
        UUID id = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO job (id, tenant_id, kind, handler, run_at, payload, max_run_time, misfire, partition_id)
                VALUES (?, ?, 'ONE_OFF', 'PICKUP_ALERT', clock_timestamp(), '{}'::jsonb, interval '5 minutes',
                        'FIRE_NOW', 0)""")
                .params(id, tenant).update();
        return id;
    }

    /** Inserts a run; {@code seq == null} means no fencing token. Returns the run id. */
    private long insertRun(UUID job, String tenant, String state, Short slot, Long seq) {
        Long epoch = seq == null ? null : 1L;
        return jdbc.sql("""
                INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                     scheduled_at, due_at, idem_key, state, slot_no, fencing_epoch, fencing_seq,
                                     lease_until)
                VALUES (:job, :tenant, 0, 5, 0, interval '5 minutes', clock_timestamp(), clock_timestamp(),
                        :key, :state, :slot, :epoch, :seq, clock_timestamp() + interval '10 s')
                RETURNING id""")
                .param("job", job)
                .param("tenant", tenant)
                .param("key", job + ":" + UUID.randomUUID())
                .param("state", state)
                .param("slot", slot, java.sql.Types.SMALLINT)
                .param("epoch", epoch, java.sql.Types.BIGINT)
                .param("seq", seq, java.sql.Types.BIGINT)
                .query(Long.class)
                .single();
    }

    private static void assertSqlState(ThrowingCallable call, String sqlState) {
        assertThatThrownBy(call)
                .satisfies(t -> assertThat(sqlStateOf(t)).as("SQLState").isEqualTo(sqlState));
    }

    private static String sqlStateOf(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof SQLException e && e.getSQLState() != null) {
                return e.getSQLState();
            }
        }
        return null;
    }
}
