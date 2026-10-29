package com.moveinsync.scheduler.sink.support;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/**
 * HTTP helper for sink integration tests (JDK HttpClient; never throws on 4xx/5xx), plus unique
 * ids, because the test database is shared by every IT in the JVM.
 */
public final class SinkTestClient {

    /** Dev default of {@code sink.admin.token}. */
    public static final String ADMIN_TOKEN = "dev-only-sink-admin-token";

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final AtomicLong RUN_IDS = new AtomicLong(System.currentTimeMillis() * 1_000L);

    private final RestClient http;

    public SinkTestClient(int port) {
        this.http = RestClient.builder()
                .baseUrl("http://localhost:" + port)
                .requestFactory(new JdkClientHttpRequestFactory())
                .defaultStatusHandler(status -> true, (request, response) -> { })
                .build();
    }

    /** A run id no other test in this JVM uses. */
    public static long newRunId() {
        return RUN_IDS.incrementAndGet();
    }

    /** An idempotency key no other test uses. */
    public static String newKey(String prefix) {
        return prefix + ":" + UUID.randomUUID();
    }

    public static String smsPayload() {
        return "{\"template\":\"PICKUP_ALERT\",\"tripId\":\"T-1\",\"etaMinutes\":5}";
    }

    public ResponseEntity<JsonNode> deliver(long runId, String key, String token) {
        return deliver(runId, key, token, "SMS", smsPayload());
    }

    public ResponseEntity<JsonNode> deliver(long runId, String key, String token, String channel, String payloadJson) {
        String body = "{\"runId\":" + runId + ",\"channel\":\"" + channel + "\",\"payload\":" + payloadJson + "}";
        return post(key, token, body);
    }

    /** Raw POST; {@code key} or {@code token} may be null to omit that header. */
    public ResponseEntity<JsonNode> post(String key, String token, String rawBody) {
        return http.post().uri("/api/v1/deliveries")
                .contentType(MediaType.APPLICATION_JSON)
                .headers(h -> {
                    if (key != null) {
                        h.set("Idempotency-Key", key);
                    }
                    if (token != null) {
                        h.set("Fencing-Token", token);
                    }
                })
                .body(rawBody)
                .retrieve()
                .toEntity(JsonNode.class);
    }

    /** POST with an explicit {@code Content-Type} and optional {@code Accept}; a non-JSON body reads as null. */
    public ResponseEntity<JsonNode> raw(String contentType, String accept, String key, String token, String rawBody) {
        ResponseEntity<String> r = http.post().uri("/api/v1/deliveries")
                .contentType(MediaType.parseMediaType(contentType))
                .headers(h -> {
                    if (accept != null) {
                        h.set("Accept", accept);
                    }
                    h.set("Idempotency-Key", key);
                    h.set("Fencing-Token", token);
                })
                .body(rawBody)
                .retrieve()
                .toEntity(String.class);
        JsonNode body;
        try {
            body = r.getBody() == null ? null : JSON.readTree(r.getBody());
        } catch (JsonProcessingException e) {
            body = null;
        }
        return new ResponseEntity<>(body, r.getHeaders(), r.getStatusCode());
    }

    public ResponseEntity<JsonNode> get(String path) {
        return http.get().uri(path).retrieve().toEntity(JsonNode.class);
    }

    public ResponseEntity<String> getText(String path) {
        return http.get().uri(path).retrieve().toEntity(String.class);
    }

    /** Admin call with the given {@code X-Admin-Token} ({@code null} omits the header). */
    public ResponseEntity<JsonNode> admin(HttpMethod method, String path, String jsonBody, String adminToken) {
        RestClient.RequestBodySpec spec = http.method(method).uri(path)
                .headers(h -> {
                    if (adminToken != null) {
                        h.set("X-Admin-Token", adminToken);
                    }
                });
        if (jsonBody != null) {
            spec = spec.contentType(MediaType.APPLICATION_JSON).body(jsonBody);
        }
        return spec.retrieve().toEntity(JsonNode.class);
    }

    public ResponseEntity<JsonNode> admin(HttpMethod method, String path, String jsonBody) {
        return admin(method, path, jsonBody, ADMIN_TOKEN);
    }
}
