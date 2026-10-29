package com.moveinsync.scheduler.sink.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.moveinsync.scheduler.sink.support.AbstractEmbeddedPostgresIT;
import com.moveinsync.scheduler.sink.support.SinkTestClient;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;

/** {@code POST /api/v1/deliveries}: idempotency, fencing order, and request validation. */
class DeliveryIT extends AbstractEmbeddedPostgresIT {

    @LocalServerPort
    private int port;

    @Autowired
    private JdbcClient jdbc;

    private SinkTestClient sink;

    @BeforeEach
    void client() {
        sink = new SinkTestClient(port);
    }

    @Test
    void firstDeliveryIs201ThenSameKeyIs200AlreadyDelivered() {
        long run = SinkTestClient.newRunId();
        String key = SinkTestClient.newKey(run + ":emp-1");

        ResponseEntity<JsonNode> first = sink.deliver(run, key, "7.41");
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(first.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_JSON);
        assertThat(first.getBody().get("outcome").asText()).isEqualTo("DELIVERED");
        assertThat(first.getBody().get("runId").asLong()).isEqualTo(run);
        assertThat(first.getBody().get("fencingToken").asText()).isEqualTo("7.41");
        assertThat(first.getBody().get("deliveredAt").asText()).isNotBlank();

        ResponseEntity<JsonNode> again = sink.deliver(run, key, "7.41");
        assertThat(again.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(again.getBody().get("outcome").asText()).isEqualTo("ALREADY_DELIVERED");
        assertThat(again.getBody().get("deliveredAt").asText()).isEqualTo(first.getBody().get("deliveredAt").asText());

        assertThat(deliveries(key)).isEqualTo(1);
        assertThat(attemptOutcomes(key)).containsExactly("DELIVERED", "ALREADY_DELIVERED");
    }

    @Test
    void lowerTokenIs409AndDeliversNothing() {
        long run = SinkTestClient.newRunId();
        assertThat(sink.deliver(run, SinkTestClient.newKey("a"), "7.42").getStatusCode()).isEqualTo(HttpStatus.CREATED);

        String staleKey = SinkTestClient.newKey("stale");
        ResponseEntity<JsonNode> stale = sink.deliver(run, staleKey, "7.41");
        assertThat(stale.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(stale.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(stale.getBody().get("code").asText()).isEqualTo("STALE_TOKEN");
        assertThat(stale.getBody().get("outcome").asText()).isEqualTo("STALE_TOKEN");
        assertThat(stale.getBody().get("status").asInt()).isEqualTo(409);

        assertThat(deliveries(staleKey)).isZero();
        assertThat(attemptOutcomes(staleKey)).containsExactly("STALE_TOKEN");
        assertThat(fence(run)).isEqualTo("7.42");
    }

    @Test
    void staleRetryOfAnAlreadyDeliveredKeyIsAlso409() {
        long run = SinkTestClient.newRunId();
        String key = SinkTestClient.newKey("k");
        assertThat(sink.deliver(run, key, "7.41").getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(sink.deliver(run, SinkTestClient.newKey("k2"), "7.42").getStatusCode()).isEqualTo(HttpStatus.CREATED);

        assertThat(sink.deliver(run, key, "7.41").getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(sink.deliver(run, key, "7.42").getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(attemptOutcomes(key)).containsExactly("DELIVERED", "STALE_TOKEN", "ALREADY_DELIVERED");
    }

    @Test
    void higherTokenAfterLowerIsAcceptedAndRaisesFence() {
        long run = SinkTestClient.newRunId();
        assertThat(sink.deliver(run, SinkTestClient.newKey("a"), "7.41").getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(sink.deliver(run, SinkTestClient.newKey("b"), "7.42").getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(fence(run)).isEqualTo("7.42");
        assertThat(sink.deliver(run, SinkTestClient.newKey("c"), "7.41").getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    void equalTokenIsAcceptedForNewKeys() {
        long run = SinkTestClient.newRunId();
        for (int i = 0; i < 3; i++) {
            assertThat(sink.deliver(run, SinkTestClient.newKey("item-" + i), "9.5").getStatusCode())
                    .isEqualTo(HttpStatus.CREATED);
        }
        assertThat(fence(run)).isEqualTo("9.5");
    }

    @Test
    void epochBeatsSeq() {
        long run = SinkTestClient.newRunId();
        assertThat(sink.deliver(run, SinkTestClient.newKey("a"), "7.999999").getStatusCode())
                .isEqualTo(HttpStatus.CREATED);
        assertThat(sink.deliver(run, SinkTestClient.newKey("b"), "8.1").getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(fence(run)).isEqualTo("8.1");
        assertThat(sink.deliver(run, SinkTestClient.newKey("c"), "7.1000000").getStatusCode())
                .isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    void fencesAreIndependentPerRun() {
        long runA = SinkTestClient.newRunId();
        long runB = SinkTestClient.newRunId();
        assertThat(sink.deliver(runA, SinkTestClient.newKey("a"), "9.9").getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(sink.deliver(runB, SinkTestClient.newKey("b"), "1.1").getStatusCode()).isEqualTo(HttpStatus.CREATED);
    }

    @Test
    void missingOrMalformedHeadersAre400AndRecordNothing() {
        long run = SinkTestClient.newRunId();
        String key = SinkTestClient.newKey("hdr");
        String body = "{\"runId\":" + run + ",\"channel\":\"SMS\",\"payload\":{}}";

        assertProblem(sink.post(null, "7.1", body), 400, "MISSING_IDEMPOTENCY_KEY");
        assertProblem(sink.post(key, null, body), 400, "MISSING_FENCING_TOKEN");
        for (String bad : List.of("7", "7.1.1", "-1.2", "a.b", "99999999999999999999.1")) {
            ResponseEntity<JsonNode> r = sink.post(key, bad, body);
            assertProblem(r, 400, "INVALID_FENCING_TOKEN");
            assertThat(r.getBody().toString()).doesNotContain(bad);
        }
        assertProblem(sink.post("bad key", "7.1", body), 400, "INVALID_IDEMPOTENCY_KEY");
        // headers are checked before the body
        assertProblem(sink.post(null, null, "not json"), 400, "MISSING_IDEMPOTENCY_KEY");

        assertThat(attempts(run)).isZero();
        assertThat(jdbc.sql("SELECT count(*) FROM sink.run_fence WHERE run_id = :r").param("r", run)
                .query(Long.class).single()).isZero();
    }

    @Test
    void invalidBodiesAre400WithFieldErrorsAndRecordNothing() {
        long run = SinkTestClient.newRunId();
        String key = SinkTestClient.newKey("body");

        assertProblem(sink.post(key, "7.1", "not json"), 400, "MALFORMED_BODY");
        assertProblem(sink.post(key, "7.1", ""), 400, "MALFORMED_BODY");
        assertProblem(sink.post(key, "7.1", "[1,2]"), 400, "MALFORMED_BODY");

        ResponseEntity<JsonNode> r = sink.post(key, "7.1", "{\"runId\":" + run + ",\"channel\":\"EMAIL\",\"payload\":[]}");
        assertProblem(r, 400, "VALIDATION_FAILED");
        assertThat(r.getBody().get("errors").findValuesAsText("field")).containsExactlyInAnyOrder("channel", "payload");
        assertThat(r.getBody().toString()).doesNotContain("EMAIL");

        r = sink.post(key, "7.1", "{\"channel\":\"SMS\"}");
        assertProblem(r, 400, "VALIDATION_FAILED");
        assertThat(r.getBody().get("errors").findValuesAsText("field")).containsExactlyInAnyOrder("runId", "payload");

        r = sink.post(key, "7.1", "{\"runId\":\"abc\",\"channel\":\"SMS\",\"payload\":{}}");
        assertProblem(r, 400, "VALIDATION_FAILED");
        assertThat(r.getBody().get("errors").findValuesAsText("field")).containsExactly("runId");
        assertThat(r.getBody().toString()).doesNotContain("abc");

        // no scalar coercion
        for (String runId : List.of("\"123\"", "1.5", "true")) {
            r = sink.post(key, "7.1", "{\"runId\":" + runId + ",\"channel\":\"SMS\",\"payload\":{}}");
            assertProblem(r, 400, "VALIDATION_FAILED");
            assertThat(r.getBody().get("errors").findValuesAsText("field")).containsExactly("runId");
        }
        r = sink.post(key, "7.1", "{\"runId\":" + run + ",\"channel\":1,\"payload\":{}}");
        assertProblem(r, 400, "VALIDATION_FAILED");
        assertThat(r.getBody().get("errors").findValuesAsText("field")).containsExactly("channel");

        assertProblem(sink.post(key, "7.1", "{\"runId\":0,\"channel\":\"PUSH\",\"payload\":{}}"), 400, "VALIDATION_FAILED");

        assertThat(attempts(run)).isZero();
        assertThat(deliveries(key)).isZero();
    }

    @Test
    void pushChannelIsAccepted() {
        long run = SinkTestClient.newRunId();
        String key = SinkTestClient.newKey("push");
        assertThat(sink.deliver(run, key, "1.1", "PUSH", "{\"title\":\"Cab arriving\"}").getStatusCode())
                .isEqualTo(HttpStatus.CREATED);
        assertThat(jdbc.sql("SELECT channel FROM sink.delivery WHERE idem_key = :k").param("k", key)
                .query(String.class).single()).isEqualTo("PUSH");
    }

    @Test
    void errorsAreProblemDetailsWithoutStackTraces() {
        ResponseEntity<JsonNode> notFound = sink.get("/api/v1/nope");
        assertProblem(notFound, 404, "NOT_FOUND");
        ResponseEntity<JsonNode> wrongMethod = sink.get("/api/v1/deliveries");
        assertProblem(wrongMethod, 405, "METHOD_NOT_ALLOWED");
        for (ResponseEntity<JsonNode> r : List.of(notFound, wrongMethod)) {
            assertThat(r.getBody().has("trace")).isFalse();
            assertThat(r.getBody().has("exception")).isFalse();
        }
    }

    static void assertProblem(ResponseEntity<JsonNode> r, int status, String code) {
        assertThat(r.getStatusCode().value()).as("status of %s", code).isEqualTo(status);
        assertThat(r.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(r.getBody().get("code").asText()).isEqualTo(code);
        assertThat(r.getBody().get("title").asText()).isNotBlank();
        assertThat(r.getBody().get("detail").asText()).isNotBlank();
    }

    private long deliveries(String key) {
        return jdbc.sql("SELECT count(*) FROM sink.delivery WHERE idem_key = :k").param("k", key)
                .query(Long.class).single();
    }

    private long attempts(long run) {
        return jdbc.sql("SELECT count(*) FROM sink.attempt_log WHERE run_id = :r").param("r", run)
                .query(Long.class).single();
    }

    private List<String> attemptOutcomes(String key) {
        return jdbc.sql("SELECT outcome FROM sink.attempt_log WHERE idem_key = :k ORDER BY id").param("k", key)
                .query(String.class).list();
    }

    private String fence(long run) {
        return jdbc.sql("SELECT max_epoch || '.' || max_seq FROM sink.run_fence WHERE run_id = :r").param("r", run)
                .query(String.class).single();
    }
}
