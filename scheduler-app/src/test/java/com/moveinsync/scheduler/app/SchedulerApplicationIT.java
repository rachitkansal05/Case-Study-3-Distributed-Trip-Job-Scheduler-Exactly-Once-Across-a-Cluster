package com.moveinsync.scheduler.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.config.SchedulerProperties;
import com.moveinsync.scheduler.app.support.AbstractEmbeddedPostgresIT;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Smoke test: the node boots against real Postgres, migrates, binds config and exposes actuator.
 * {@code @AutoConfigureObservability} re-enables the Prometheus registry, which Spring Boot tests
 * switch off by default.
 */
@AutoConfigureObservability
class SchedulerApplicationIT extends AbstractEmbeddedPostgresIT {

    @Autowired
    private TestRestTemplate http;

    @Autowired
    private SchedulerProperties props;

    @Autowired
    private JdbcClient jdbc;

    @Test
    void contextStartsAndMigrates() {
        assertThat(jdbc.sql("SELECT count(*) FROM partition_lease").query(Long.class).single()).isEqualTo(128L);
        assertThat(jdbc.sql("SELECT max(version) FROM flyway_schema_history WHERE success")
                .query(String.class).single()).isNotBlank();
    }

    @Test
    void timingKnobsBindWithPlanDefaults() {
        assertThat(props.nodeId()).isEqualTo("test-node");
        assertThat(props.partitions()).isEqualTo(128);
        assertThat(props.runLease().duration()).isEqualTo(Duration.ofSeconds(10));
        assertThat(props.runLease().renewInterval()).isEqualTo(Duration.ofSeconds(3));
        assertThat(props.partitionLease().duration()).isEqualTo(Duration.ofSeconds(6));
        assertThat(props.partitionLease().renewInterval()).isEqualTo(Duration.ofSeconds(2));
        assertThat(props.partitionLease().loopInterval()).isEqualTo(Duration.ofSeconds(1));
        assertThat(props.membership().heartbeatInterval()).isEqualTo(Duration.ofSeconds(2));
        assertThat(props.membership().deadAfter()).isEqualTo(Duration.ofSeconds(6));
        assertThat(props.claim().pollInterval()).isEqualTo(Duration.ofMillis(200));
        assertThat(props.orphanSweep().interval()).isEqualTo(Duration.ofSeconds(1));
        assertThat(props.workers().count()).isEqualTo(64);
        assertThat(props.workers().reservedForRecovery()).isEqualTo(1);
        assertThat(props.selfFenceMargin()).isEqualTo(Duration.ofSeconds(2));
        assertThat(props.tenant().defaultCap()).isEqualTo(32);
        assertThat(props.misfire().defaultThreshold()).isEqualTo(Duration.ofSeconds(60));
        assertThat(props.retry().maxAttempts()).isEqualTo(5);
        assertThat(props.retry().leaseLostLimit()).isEqualTo(20);
        assertThat(props.security().toString()).doesNotContain(props.security().jwtHmacSecret());
    }

    @Test
    void healthAndPrometheusAreExposedWithoutAuth() {
        ResponseEntity<String> health = http.getForEntity("/actuator/health", String.class);
        assertThat(health.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(health.getBody()).contains("\"UP\"");

        ResponseEntity<String> prometheus = http.getForEntity("/actuator/prometheus", String.class);
        assertThat(prometheus.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(prometheus.getBody()).contains("jvm_memory_used_bytes");
    }

    @Test
    void apiRequiresAuthentication() {
        assertThat(http.getForEntity("/api/v1/cluster", String.class).getStatusCode())
                .isEqualTo(HttpStatus.UNAUTHORIZED);
    }
}
