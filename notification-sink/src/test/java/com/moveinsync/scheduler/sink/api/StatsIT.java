package com.moveinsync.scheduler.sink.api;

import static com.moveinsync.scheduler.sink.api.DeliveryIT.assertProblem;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.sink.support.AbstractEmbeddedPostgresIT;
import com.moveinsync.scheduler.sink.support.SinkTestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/** Read APIs: per-run stats, totals with {@code maxDeliveriesPerKey}, and the highest epoch. */
class StatsIT extends AbstractEmbeddedPostgresIT {

    @LocalServerPort
    private int port;

    private SinkTestClient sink;

    @BeforeEach
    void client() {
        sink = new SinkTestClient(port);
    }

    @Test
    void runStatsCountEveryOutcome() {
        long run = SinkTestClient.newRunId();
        String k1 = SinkTestClient.newKey("k1");
        assertThat(sink.deliver(run, k1, "3.1").getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(sink.deliver(run, k1, "3.1").getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(sink.deliver(run, SinkTestClient.newKey("k2"), "3.2").getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(sink.deliver(run, SinkTestClient.newKey("k3"), "3.1").getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        ResponseEntity<JsonNode> r = sink.get("/api/v1/deliveries/stats?runId=" + run);
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode s = r.getBody();
        assertThat(s.get("runId").asLong()).isEqualTo(run);
        assertThat(s.get("deliveredDistinct").asLong()).isEqualTo(2);
        assertThat(s.get("attempts").asLong()).isEqualTo(4);
        assertThat(s.get("delivered").asLong()).isEqualTo(2);
        assertThat(s.get("alreadyDelivered").asLong()).isEqualTo(1);
        assertThat(s.get("staleRejected").asLong()).isEqualTo(1);
    }

    @Test
    void unknownRunHasZeroStats() {
        JsonNode s = sink.get("/api/v1/deliveries/stats?runId=" + SinkTestClient.newRunId()).getBody();
        assertThat(s.get("deliveredDistinct").asLong()).isZero();
        assertThat(s.get("attempts").asLong()).isZero();
        assertThat(s.get("staleRejected").asLong()).isZero();
    }

    @Test
    void invalidRunIdIs400() {
        assertProblem(sink.get("/api/v1/deliveries/stats?runId=abc"), 400, "VALIDATION_FAILED");
        assertProblem(sink.get("/api/v1/deliveries/stats?runId=0"), 400, "VALIDATION_FAILED");
    }

    @Test
    void totalsMoveByExactlyWhatWasSentAndNoKeyIsDeliveredTwice() {
        JsonNode before = sink.get("/api/v1/deliveries/stats").getBody();
        long run = SinkTestClient.newRunId();
        String key = SinkTestClient.newKey("t");
        sink.deliver(run, key, "4.1");
        sink.deliver(run, key, "4.1");
        sink.deliver(run, key, "4.2");
        sink.deliver(run, SinkTestClient.newKey("t2"), "4.0");

        JsonNode after = sink.get("/api/v1/deliveries/stats").getBody();
        assertThat(delta(before, after, "deliveredDistinct")).isEqualTo(1);
        assertThat(delta(before, after, "attempts")).isEqualTo(4);
        assertThat(delta(before, after, "delivered")).isEqualTo(1);
        assertThat(delta(before, after, "alreadyDelivered")).isEqualTo(2);
        assertThat(delta(before, after, "staleRejected")).isEqualTo(1);
        assertThat(delta(before, after, "runs")).isEqualTo(1);
        assertThat(after.get("maxDeliveriesPerKey").asLong()).isEqualTo(1);
        assertThat(after.get("attempts").asLong()).isEqualTo(after.get("delivered").asLong()
                + after.get("alreadyDelivered").asLong() + after.get("staleRejected").asLong());
        assertThat(after.get("deliveredDistinct").asLong()).isEqualTo(after.get("delivered").asLong());
    }

    @Test
    void highestEpochIsTheHighestAcceptedEpochAndNeverGoesDown() {
        long current = sink.get("/api/v1/fence/highest-epoch").getBody().get("highestEpoch").asLong();
        long higher = Math.max(current, FencingToken.epochOf(3, 7)) + 1;

        assertThat(sink.deliver(SinkTestClient.newRunId(), SinkTestClient.newKey("e"), higher + ".1").getStatusCode())
                .isEqualTo(HttpStatus.CREATED);
        assertThat(highestEpoch()).isEqualTo(higher);

        // A lower epoch accepted on another run leaves the maximum alone.
        assertThat(sink.deliver(SinkTestClient.newRunId(), SinkTestClient.newKey("e"), "1.1").getStatusCode())
                .isEqualTo(HttpStatus.CREATED);
        assertThat(highestEpoch()).isEqualTo(higher);
    }

    private long highestEpoch() {
        ResponseEntity<JsonNode> r = sink.get("/api/v1/fence/highest-epoch");
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.OK);
        return r.getBody().get("highestEpoch").asLong();
    }

    private static long delta(JsonNode before, JsonNode after, String field) {
        return after.get(field).asLong() - before.get(field).asLong();
    }
}
