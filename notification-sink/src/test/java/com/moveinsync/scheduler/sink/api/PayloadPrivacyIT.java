package com.moveinsync.scheduler.sink.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.sink.domain.Hashing;
import com.moveinsync.scheduler.sink.support.AbstractEmbeddedPostgresIT;
import com.moveinsync.scheduler.sink.support.SinkTestClient;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * The payload (phone, e-mail, home location) is never stored, logged or echoed in plain text;
 * only its SHA-256 is stored. Logs carry the key hash, never the raw key.
 */
@ExtendWith(OutputCaptureExtension.class)
class PayloadPrivacyIT extends AbstractEmbeddedPostgresIT {

    private static final String PHONE = "+919812345678";
    private static final String EMAIL = "priya.sharma@example.com";
    private static final String HOME = "42 Residency Road, Bengaluru";
    private static final String PAYLOAD = "{\"to\":\"" + PHONE + "\",\"email\":\"" + EMAIL + "\","
            + "\"pickup\":{\"address\":\"" + HOME + "\",\"lat\":12.9716,\"lng\":77.5946},\"text\":\"Cab at 8:05\"}";

    @LocalServerPort
    private int port;

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private ObjectMapper objectMapper;

    private SinkTestClient sink;

    @BeforeEach
    void client() {
        sink = new SinkTestClient(port);
    }

    @Test
    void payloadIsStoredOnlyAsSha256AndNeverLogged(CapturedOutput output) throws Exception {
        long run = SinkTestClient.newRunId();
        String key = SinkTestClient.newKey(run + ":emp-secret-key");

        ResponseEntity<JsonNode> first = sink.deliver(run, key, "7.1", "SMS", PAYLOAD);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(sink.deliver(run, key, "7.1", "SMS", PAYLOAD).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(sink.deliver(run, SinkTestClient.newKey("other"), "7.0", "SMS", PAYLOAD).getStatusCode())
                .isEqualTo(HttpStatus.CONFLICT);

        String expectedHash = Hashing.sha256Hex(objectMapper.writeValueAsBytes(objectMapper.readTree(PAYLOAD)));
        assertThat(jdbc.sql("SELECT payload_sha256 FROM sink.delivery WHERE idem_key = :k").param("k", key)
                .query(String.class).single()).isEqualTo(expectedHash);

        // nothing in any sink table contains the personal data
        for (String table : List.of("sink.delivery", "sink.attempt_log", "sink.run_fence")) {
            List<String> rows = jdbc.sql("SELECT row_to_json(t)::text FROM " + table + " t").query(String.class).list();
            assertThat(rows).as(table).noneMatch(r -> r.contains(PHONE) || r.contains(EMAIL) || r.contains(HOME)
                    || r.contains("12.9716"));
        }

        // responses never echo it
        assertThat(first.getBody().toString()).doesNotContain(PHONE, EMAIL, HOME);

        // logs carry the key hash (so capture works) but never the payload or the raw key
        assertThat(output.getAll()).contains("idemKeyHash=" + Hashing.logHash(key));
        assertThat(output.getAll()).doesNotContain(PHONE, EMAIL, HOME, key);
    }

    @Test
    void rejectedBodiesDoNotEchoPersonalData(CapturedOutput output) {
        long run = SinkTestClient.newRunId();
        String key = SinkTestClient.newKey("bad");

        List<ResponseEntity<JsonNode>> responses = List.of(
                // malformed JSON that contains the phone number
                sink.post(key, "7.1", "{\"runId\":" + run + ",\"channel\":\"SMS\",\"payload\":{\"to\":\"" + PHONE),
                // the phone number in a field of the wrong type
                sink.post(key, "7.1", "{\"runId\":\"" + PHONE + "\",\"channel\":\"SMS\",\"payload\":{}}"),
                // the e-mail as a channel
                sink.post(key, "7.1", "{\"runId\":" + run + ",\"channel\":\"" + EMAIL + "\",\"payload\":"
                        + PAYLOAD + "}"),
                // payload not an object
                sink.post(key, "7.1", "{\"runId\":" + run + ",\"channel\":\"SMS\",\"payload\":\"" + PHONE + "\"}"));

        for (ResponseEntity<JsonNode> r : responses) {
            assertThat(r.getStatusCode().value()).isEqualTo(400);
            assertThat(r.getBody().toString()).doesNotContain(PHONE, EMAIL, HOME);
        }
        assertThat(output.getAll()).doesNotContain(PHONE, EMAIL, HOME);
    }
}
