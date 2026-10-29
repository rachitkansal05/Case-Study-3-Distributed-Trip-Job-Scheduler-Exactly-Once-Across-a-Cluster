package com.moveinsync.scheduler.sink.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.sink.support.EmbeddedPostgresSupport;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;

/** Proves the sink migrations apply into their own {@code sink} schema on real PostgreSQL 16. */
class SinkSchemaIT {

    private static final String HASH = "a".repeat(64);

    private static DataSource dataSource;
    private static MigrateResult migration;

    @BeforeAll
    static void migrate() {
        dataSource = EmbeddedPostgresSupport.createDatabase("sink_schema_it");
        migration = Flyway.configure()
                .dataSource(dataSource)
                .schemas("sink")
                .defaultSchema("sink")
                .locations("classpath:db/migration")
                .load()
                .migrate();
    }

    @Test
    void migrationsApplyIntoSinkSchema() {
        assertThat(migration.success).isTrue();
        JdbcClient jdbc = JdbcClient.create(dataSource);
        assertThat(jdbc.sql("""
                SELECT table_name FROM information_schema.tables
                 WHERE table_schema = 'sink' ORDER BY table_name""").query(String.class).list())
                .contains("attempt_log", "delivery", "run_fence", "flyway_schema_history");
        assertThat(jdbc.sql("""
                SELECT count(*) FROM information_schema.tables
                 WHERE table_schema = 'public'""").query(Long.class).single())
                .as("sink must not create tables outside its schema").isZero();
    }

    @Test
    void deliveryIsUniquePerIdemKey() {
        JdbcClient jdbc = JdbcClient.create(dataSource);
        String insert = "INSERT INTO sink.delivery "
                + "(idem_key, run_id, fencing_epoch, fencing_seq, channel, payload_sha256, delivered_at) "
                + "VALUES ('run-1:emp-7', 1, 1, 1, 'SMS', '" + HASH + "', clock_timestamp())";
        jdbc.sql(insert).update();
        assertThatThrownBy(() -> jdbc.sql(insert).update()).isInstanceOf(DuplicateKeyException.class);
    }

    @Test
    void deliveryRejectsUnknownChannelAndNonHashPayload() {
        JdbcClient jdbc = JdbcClient.create(dataSource);
        assertThatThrownBy(() -> jdbc.sql("INSERT INTO sink.delivery "
                + "(idem_key, run_id, fencing_epoch, fencing_seq, channel, payload_sha256, delivered_at) "
                + "VALUES ('k-email', 1, 1, 1, 'EMAIL', '" + HASH + "', clock_timestamp())").update())
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> jdbc.sql("INSERT INTO sink.delivery "
                + "(idem_key, run_id, fencing_epoch, fencing_seq, channel, payload_sha256, delivered_at) "
                + "VALUES ('k-plain', 1, 1, 1, 'SMS', '+919876543210', clock_timestamp())").update())
                .as("only a hex SHA-256 fits the payload column, never the payload itself")
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void attemptLogRequiresRunAndKnownOutcome() {
        JdbcClient jdbc = JdbcClient.create(dataSource);
        jdbc.sql("INSERT INTO sink.attempt_log (run_id, idem_key, fencing_epoch, fencing_seq, outcome, at) "
                + "VALUES (1, 'k', 1, 1, 'STALE_TOKEN', clock_timestamp())").update();
        assertThatThrownBy(() -> jdbc.sql("INSERT INTO sink.attempt_log "
                + "(idem_key, fencing_epoch, fencing_seq, outcome, at) "
                + "VALUES ('k', 1, 1, 'DELIVERED', clock_timestamp())").update())
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThatThrownBy(() -> jdbc.sql("INSERT INTO sink.attempt_log "
                + "(run_id, idem_key, fencing_epoch, fencing_seq, outcome, at) "
                + "VALUES (1, 'k', 1, 1, 'MAYBE', clock_timestamp())").update())
                .isInstanceOf(DataIntegrityViolationException.class);
    }
}
