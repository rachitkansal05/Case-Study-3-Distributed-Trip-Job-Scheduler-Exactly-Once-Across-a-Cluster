package com.moveinsync.scheduler.sink.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.sink.support.AbstractEmbeddedPostgresIT;
import com.moveinsync.scheduler.sink.support.SinkTestClient;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/** {@code sink_deliveries_total{outcome}} and the latency timer, exported on {@code /actuator/prometheus}. */
@AutoConfigureObservability(tracing = false)
class MetricsIT extends AbstractEmbeddedPostgresIT {

    @LocalServerPort
    private int port;

    @Autowired
    private MeterRegistry registry;

    private SinkTestClient sink;

    @BeforeEach
    void client() {
        sink = new SinkTestClient(port);
    }

    @Test
    void countsEveryOutcome() {
        double delivered = count("DELIVERED");
        double already = count("ALREADY_DELIVERED");
        double stale = count("STALE_TOKEN");
        double injected = count("INJECTED_FAULT");

        long run = SinkTestClient.newRunId();
        String key = SinkTestClient.newKey("m");
        sink.deliver(run, key, "6.2");
        sink.deliver(run, key, "6.2");
        sink.deliver(run, SinkTestClient.newKey("m2"), "6.1");
        sink.admin(HttpMethod.PUT, "/admin/faults", "{\"failureRate\":1}");
        try {
            sink.deliver(run, SinkTestClient.newKey("m3"), "6.2");
        } finally {
            sink.admin(HttpMethod.DELETE, "/admin/faults", null);
        }

        assertThat(count("DELIVERED") - delivered).isEqualTo(1);
        assertThat(count("ALREADY_DELIVERED") - already).isEqualTo(1);
        assertThat(count("STALE_TOKEN") - stale).isEqualTo(1);
        assertThat(count("INJECTED_FAULT") - injected).isEqualTo(1);
        assertThat(registry.get("sink.delivery.latency").tag("outcome", "DELIVERED").timer().count()).isPositive();
    }

    @Test
    void prometheusEndpointExportsSinkMeters() {
        sink.deliver(SinkTestClient.newRunId(), SinkTestClient.newKey("p"), "1.1");
        ResponseEntity<String> r = sink.getText("/actuator/prometheus");
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(r.getBody())
                .contains("sink_deliveries_total{")
                .contains("outcome=\"DELIVERED\"")
                .contains("outcome=\"STALE_TOKEN\"")
                .contains("sink_delivery_latency_seconds_bucket{")
                .contains("sink_delivery_latency_seconds_count{");
    }

    private double count(String outcome) {
        return registry.get("sink.deliveries").tag("outcome", outcome).counter().count();
    }
}
