package com.moveinsync.scheduler.sink.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.sink.support.EmbeddedPostgresSupport;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;

/** Proves the sink migrations apply into their own {@code sink} schema on real PostgreSQL 16. */
class SinkSchemaIT {

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
        String insert = "INSERT INTO sink.delivery (idem_key, run_id, fencing_epoch, fencing_seq, delivered_at) "
                + "VALUES ('run-1:emp-7', 1, 1, 1, clock_timestamp())";
        jdbc.sql(insert).update();
        assertThatThrownBy(() -> jdbc.sql(insert).update()).isInstanceOf(DuplicateKeyException.class);
    }
}
