package com.moveinsync.scheduler.sink.api;

import static com.moveinsync.scheduler.sink.api.DeliveryIT.assertProblem;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.moveinsync.scheduler.sink.support.AbstractEmbeddedPostgresIT;
import com.moveinsync.scheduler.sink.support.SinkTestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;

/** {@code /admin/faults}: injected failures happen before any write, and admin routes need the token. */
class FaultInjectionIT extends AbstractEmbeddedPostgresIT {

    @LocalServerPort
    private int port;

    @Autowired
    private JdbcClient jdbc;

    private SinkTestClient sink;

    @BeforeEach
    void client() {
        sink = new SinkTestClient(port);
    }

    @AfterEach
    void clearFaults() {
        assertThat(sink.admin(HttpMethod.DELETE, "/admin/faults", null).getStatusCode())
                .isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    void failureRateOneFailsEveryCallAndWritesNothing() {
        long deliveriesBefore = count("sink.delivery");
        long attemptsBefore = count("sink.attempt_log");
        long fencesBefore = count("sink.run_fence");

        ResponseEntity<JsonNode> set = sink.admin(HttpMethod.PUT, "/admin/faults",
                "{\"latencyMs\":0,\"latencyJitterMs\":0,\"failureRate\":1,\"failureStatus\":503}");
        assertThat(set.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(set.getBody().get("failureRate").asDouble()).isEqualTo(1.0);

        long run = SinkTestClient.newRunId();
        for (int i = 0; i < 25; i++) {
            ResponseEntity<JsonNode> r = sink.deliver(run, SinkTestClient.newKey("f" + i), "5." + i);
            assertProblem(r, 503, "INJECTED_FAULT");
        }

        assertThat(count("sink.delivery")).isEqualTo(deliveriesBefore);
        assertThat(count("sink.attempt_log")).isEqualTo(attemptsBefore);
        assertThat(count("sink.run_fence")).isEqualTo(fencesBefore);
        JsonNode stats = sink.get("/api/v1/deliveries/stats?runId=" + run).getBody();
        assertThat(stats.get("deliveredDistinct").asLong()).isZero();
        assertThat(stats.get("attempts").asLong()).isZero();
    }

    @Test
    void status500CanBeInjectedAndClearingRestoresDelivery() {
        sink.admin(HttpMethod.PUT, "/admin/faults", "{\"failureRate\":1.0,\"failureStatus\":500}");
        long run = SinkTestClient.newRunId();
        String key = SinkTestClient.newKey("retry");
        assertProblem(sink.deliver(run, key, "2.1"), 500, "INJECTED_FAULT");

        assertThat(sink.admin(HttpMethod.DELETE, "/admin/faults", null).getStatusCode())
                .isEqualTo(HttpStatus.NO_CONTENT);
        JsonNode current = sink.admin(HttpMethod.GET, "/admin/faults", null).getBody();
        assertThat(current.get("failureRate").asDouble()).isZero();

        // the retry after the injected failure is the first real delivery
        assertThat(sink.deliver(run, key, "2.1").getStatusCode()).isEqualTo(HttpStatus.CREATED);
    }

    @Test
    void latencyIsInjected() {
        sink.admin(HttpMethod.PUT, "/admin/faults", "{\"latencyMs\":150}");
        long start = System.nanoTime();
        ResponseEntity<JsonNode> r = sink.deliver(SinkTestClient.newRunId(), SinkTestClient.newKey("slow"), "1.1");
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(elapsedMs).isGreaterThanOrEqualTo(150);
    }

    @Test
    void faultsDoNotAffectReadApis() {
        sink.admin(HttpMethod.PUT, "/admin/faults", "{\"failureRate\":1}");
        assertThat(sink.get("/api/v1/deliveries/stats").getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(sink.get("/api/v1/fence/highest-epoch").getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void invalidSettingsAre400AndLeaveFaultsUnchanged() {
        ResponseEntity<JsonNode> r = sink.admin(HttpMethod.PUT, "/admin/faults",
                "{\"latencyMs\":-1,\"latencyJitterMs\":70000,\"failureRate\":1.5,\"failureStatus\":502}");
        assertProblem(r, 400, "VALIDATION_FAILED");
        assertThat(r.getBody().get("errors").findValuesAsText("field"))
                .containsExactlyInAnyOrder("latencyMs", "latencyJitterMs", "failureRate", "failureStatus");
        assertThat(sink.admin(HttpMethod.GET, "/admin/faults", null).getBody().get("failureRate").asDouble()).isZero();
    }

    @Test
    void adminRoutesNeedTheAdminToken() {
        assertProblem(sink.admin(HttpMethod.PUT, "/admin/faults", "{\"failureRate\":1}", null), 401,
                "ADMIN_UNAUTHORIZED");
        assertProblem(sink.admin(HttpMethod.PUT, "/admin/faults", "{\"failureRate\":1}", "wrong"), 401,
                "ADMIN_UNAUTHORIZED");
        assertProblem(sink.admin(HttpMethod.DELETE, "/admin/faults", null, null), 401, "ADMIN_UNAUTHORIZED");
        assertProblem(sink.admin(HttpMethod.GET, "/admin/faults", null, null), 401, "ADMIN_UNAUTHORIZED");
        // nothing was changed by the rejected PUTs
        assertThat(sink.admin(HttpMethod.GET, "/admin/faults", null).getBody().get("failureRate").asDouble()).isZero();
    }

    private long count(String table) {
        return jdbc.sql("SELECT count(*) FROM " + table).query(Long.class).single();
    }
}
