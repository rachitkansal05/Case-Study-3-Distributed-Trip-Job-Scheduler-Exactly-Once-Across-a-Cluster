package com.moveinsync.scheduler.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.sink.support.AbstractEmbeddedPostgresIT;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;

/** Smoke test: the sink boots against real Postgres, migrates its own schema and reports healthy. */
class SinkApplicationIT extends AbstractEmbeddedPostgresIT {

    @Autowired
    private TestRestTemplate http;

    @Autowired
    private JdbcClient jdbc;

    @Test
    void contextStartsAndMigratesSinkSchema() {
        assertThat(jdbc.sql("SELECT count(*) FROM sink.run_fence").query(Long.class).single()).isZero();
        assertThat(jdbc.sql("SELECT count(*) FROM sink.flyway_schema_history WHERE success")
                .query(Long.class).single()).isPositive();
    }

    @Test
    void healthIsUp() {
        ResponseEntity<String> health = http.getForEntity("/actuator/health", String.class);
        assertThat(health.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(health.getBody()).contains("\"UP\"");
    }
}
