package com.moveinsync.scheduler.app.sink;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.policy.PermanentJobException;
import com.moveinsync.scheduler.core.policy.RetryableJobException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Objects;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP client of {@code notification-sink}, exactly per docs/sink-api.md (§1, §3, §8).
 *
 * <ul>
 *   <li>201 → {@code DELIVERED}, 200 → {@code ALREADY_DELIVERED}, 409 → {@code STALE_TOKEN}.
 *   <li>Other 4xx → {@link PermanentJobException} {@code SINK_REJECTED_<status>}.
 *   <li>5xx → {@link RetryableJobException} {@code SINK_5XX}; timeout → {@code SINK_TIMEOUT}; other
 *       I/O errors → {@code SINK_UNAVAILABLE}; interrupt → {@code INTERRUPTED} (flag restored).
 * </ul>
 *
 * <p>Payloads, raw keys and personal data are never logged; log lines carry only the run id, the
 * token and a key hash. Thread-safe.
 */
public final class HttpEffectSink implements ChannelAwareEffectSink {

    private static final Logger log = LoggerFactory.getLogger(HttpEffectSink.class);

    static final String DELIVERIES_PATH = "/api/v1/deliveries";
    static final String HIGHEST_EPOCH_PATH = "/api/v1/fence/highest-epoch";
    private static final int MAX_KEY_LENGTH = 256;
    private static final Pattern VISIBLE_ASCII = Pattern.compile("[\\x21-\\x7E]+");
    private static final Pattern SAFE_CODE = Pattern.compile("[A-Z][A-Z0-9_]{0,63}");

    private final URI deliveriesUri;
    private final URI highestEpochUri;
    private final Duration requestTimeout;
    private final HttpClient client;
    private final ObjectMapper mapper;
    private final ObjectReader strictReader;

    /**
     * @param baseUrl sink base URL, e.g. {@code http://localhost:8090}
     * @param connectTimeout TCP connect timeout
     * @param requestTimeout per-request timeout; keep it below the lease budget
     * @param mapper shared Jackson mapper
     */
    public HttpEffectSink(URI baseUrl, Duration connectTimeout, Duration requestTimeout, ObjectMapper mapper) {
        this(baseUrl, HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Objects.requireNonNull(connectTimeout, "connectTimeout"))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build(), requestTimeout, mapper);
    }

    /**
     * Same, with a prebuilt client (tests).
     *
     * @param baseUrl sink base URL
     * @param client HTTP client to use
     * @param requestTimeout per-request timeout
     * @param mapper shared Jackson mapper
     */
    public HttpEffectSink(URI baseUrl, HttpClient client, Duration requestTimeout, ObjectMapper mapper) {
        String base = Objects.requireNonNull(baseUrl, "baseUrl").toString();
        base = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        this.deliveriesUri = URI.create(base + DELIVERIES_PATH);
        this.highestEpochUri = URI.create(base + HIGHEST_EPOCH_PATH);
        this.client = Objects.requireNonNull(client, "client");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.strictReader = mapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    }

    /**
     * Not supported: the sink body needs a channel. Use {@link #boundTo} with the run's handler.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public DeliveryOutcome send(long runId, String idemKey, FencingToken token, String payloadJson) {
        throw new UnsupportedOperationException(
                "HttpEffectSink needs a channel: use boundTo(handler) or send(channel, ...)");
    }

    @Override
    public DeliveryOutcome send(
            SinkChannel channel, long runId, String idemKey, FencingToken token, String payloadJson) {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(token, "token");
        requireKey(idemKey);
        String body = "{\"runId\":" + runId + ",\"channel\":\"" + channel.name() + "\",\"payload\":"
                + requireObject(payloadJson) + "}";
        HttpRequest request = HttpRequest.newBuilder(deliveriesUri)
                .timeout(requestTimeout)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Idempotency-Key", idemKey)
                .header(FencingToken.HEADER, token.toHeader())
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (HttpTimeoutException e) {
            log.warn("sink timeout runId={} fencingToken={} idemKeyHash={}", runId, token, keyHash(idemKey));
            throw new RetryableJobException("SINK_TIMEOUT", "sink request timed out", e);
        } catch (IOException e) {
            log.warn("sink unavailable runId={} fencingToken={} idemKeyHash={} error={}",
                    runId, token, keyHash(idemKey), e.getClass().getSimpleName());
            throw new RetryableJobException("SINK_UNAVAILABLE", "sink unreachable", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableJobException("INTERRUPTED", "interrupted while calling the sink", e);
        }
        return toOutcome(response, runId, idemKey, token);
    }

    private DeliveryOutcome toOutcome(
            HttpResponse<String> response, long runId, String idemKey, FencingToken token) {
        int status = response.statusCode();
        if (status == 201) {
            return DeliveryOutcome.DELIVERED;
        }
        if (status == 200) {
            return DeliveryOutcome.ALREADY_DELIVERED;
        }
        if (status == 409) {
            if (!"STALE_TOKEN".equals(errorCode(response.body()))) {
                log.warn("sink 409 without code STALE_TOKEN, treated as stale runId={} fencingToken={} "
                        + "idemKeyHash={}", runId, token, keyHash(idemKey));
            }
            return DeliveryOutcome.STALE_TOKEN;
        }
        String code = errorCode(response.body());
        String detail = "sink answered " + status + (code == null ? "" : " " + code);
        log.warn("sink error status={} code={} runId={} fencingToken={} idemKeyHash={}",
                status, code, runId, token, keyHash(idemKey));
        if (status >= 500) {
            throw new RetryableJobException("SINK_5XX", detail);
        }
        if (status >= 400) {
            throw new PermanentJobException("SINK_REJECTED_" + status, detail);
        }
        throw new RetryableJobException("SINK_UNEXPECTED_STATUS", detail);
    }

    /**
     * {@code GET /api/v1/fence/highest-epoch}. Fails closed: never assumes {@code 0}.
     *
     * @throws SinkUnavailableException on a non-200, a malformed body or an I/O error
     */
    @Override
    public long highestAcceptedEpoch() {
        HttpRequest request = HttpRequest.newBuilder(highestEpochUri)
                .timeout(requestTimeout)
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new SinkUnavailableException(
                    "highest-epoch request failed: " + e.getClass().getSimpleName(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SinkUnavailableException("interrupted while reading the highest epoch", e);
        }
        if (response.statusCode() != 200) {
            throw new SinkUnavailableException("highest-epoch answered " + response.statusCode(), null);
        }
        JsonNode epoch;
        try {
            JsonNode root = mapper.readTree(response.body());
            epoch = root == null ? null : root.get("highestEpoch");
        } catch (IOException | RuntimeException e) {
            throw new SinkUnavailableException("highest-epoch body is not JSON", e);
        }
        if (epoch == null || !epoch.isIntegralNumber() || !epoch.canConvertToLong() || epoch.longValue() < 0) {
            throw new SinkUnavailableException("highest-epoch body lacks a non-negative integer highestEpoch", null);
        }
        return epoch.longValue();
    }

    /** Validates that the payload is exactly one JSON object and returns it for raw embedding. */
    private String requireObject(String payloadJson) {
        if (payloadJson == null) {
            throw new PermanentJobException("INVALID_EFFECT_PAYLOAD", "effect payload is missing");
        }
        JsonNode node;
        try {
            node = strictReader.readTree(payloadJson);
        } catch (IOException e) {
            throw new PermanentJobException("INVALID_EFFECT_PAYLOAD", "effect payload is not valid JSON");
        }
        if (node == null || !node.isObject()) {
            throw new PermanentJobException("INVALID_EFFECT_PAYLOAD", "effect payload must be a JSON object");
        }
        return payloadJson.strip();
    }

    private static void requireKey(String idemKey) {
        if (idemKey == null || idemKey.length() > MAX_KEY_LENGTH || !VISIBLE_ASCII.matcher(idemKey).matches()) {
            throw new PermanentJobException(
                    "INVALID_IDEMPOTENCY_KEY", "idempotency key must be 1-256 visible ASCII characters");
        }
    }

    /** The sink's stable {@code code}, or null when absent or not a plain code. */
    private String errorCode(String body) {
        if (body == null || body.isBlank()) {
            return null;
        }
        try {
            JsonNode root = mapper.readTree(body);
            JsonNode code = root == null ? null : root.get("code");
            return code != null && code.isTextual() && SAFE_CODE.matcher(code.textValue()).matches()
                    ? code.textValue()
                    : null;
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    /** First 16 hex characters of SHA-256 of the key (same as the sink's {@code idemKeyHash}). */
    static String keyHash(String idemKey) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(idemKey.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
