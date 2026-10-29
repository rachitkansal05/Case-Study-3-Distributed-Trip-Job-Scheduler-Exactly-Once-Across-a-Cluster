package com.moveinsync.scheduler.app.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.policy.PermanentJobException;
import com.moveinsync.scheduler.core.policy.RetryableJobException;
import com.moveinsync.scheduler.core.ports.EffectSink;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HttpEffectSinkTest {

    private static final FencingToken TOKEN = new FencingToken(7, 41);

    private record Recorded(String method, String path, Headers headers, String body) {}

    private record Reply(int status, String body, long delayMs) {}

    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicReference<Recorded> last = new AtomicReference<>();
    private final AtomicReference<Reply> reply = new AtomicReference<>(new Reply(201, "{\"outcome\":\"DELIVERED\"}", 0));
    private HttpServer server;
    private HttpEffectSink sink;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            last.set(new Recorded(exchange.getRequestMethod(), exchange.getRequestURI().getPath(),
                    exchange.getRequestHeaders(), body));
            Reply r = reply.get();
            try {
                if (r.delayMs() > 0) {
                    Thread.sleep(r.delayMs());
                }
                byte[] bytes = r.body().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(r.status(), bytes.length == 0 ? -1 : bytes.length);
                if (bytes.length > 0) {
                    try (OutputStream out = exchange.getResponseBody()) {
                        out.write(bytes);
                    }
                }
            } catch (InterruptedException | IOException e) {
                // client gave up
            } finally {
                exchange.close();
            }
        });
        server.start();
        sink = newSink(server.getAddress().getPort(), Duration.ofSeconds(5));
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private HttpEffectSink newSink(int port, Duration requestTimeout) {
        return new HttpEffectSink(URI.create("http://127.0.0.1:" + port + "/"), Duration.ofSeconds(2),
                requestTimeout, mapper);
    }

    private DeliveryOutcome send() {
        return sink.send(SinkChannel.SMS, 77, "77:emp-601", TOKEN, "{\"to\":\"x\",\"n\":1.10}");
    }

    @Test
    void sendsExactHeadersAndRawJsonBody() throws Exception {
        assertThat(send()).isEqualTo(DeliveryOutcome.DELIVERED);

        Recorded r = last.get();
        assertThat(r.method()).isEqualTo("POST");
        assertThat(r.path()).isEqualTo("/api/v1/deliveries");
        assertThat(r.headers().getFirst("Content-Type")).isEqualTo("application/json");
        assertThat(r.headers().getFirst("Idempotency-Key")).isEqualTo("77:emp-601");
        assertThat(r.headers().getFirst("Fencing-Token")).isEqualTo("7.41");
        JsonNode body = mapper.readTree(r.body());
        assertThat(body.get("runId").isIntegralNumber()).isTrue();
        assertThat(body.get("runId").longValue()).isEqualTo(77);
        assertThat(body.get("channel").textValue()).isEqualTo("SMS");
        assertThat(body.get("payload").isObject()).isTrue();
        assertThat(body.get("payload").get("to").textValue()).isEqualTo("x");
        assertThat(r.body()).contains("\"payload\":{\"to\":\"x\",\"n\":1.10}");
    }

    @Test
    void boundViewPicksChannelFromHandler() throws Exception {
        EffectSink push = sink.boundTo(HandlerType.BILLING_CLOSE);
        assertThat(push.send(5, "k", TOKEN, "{}")).isEqualTo(DeliveryOutcome.DELIVERED);
        assertThat(mapper.readTree(last.get().body()).get("channel").textValue()).isEqualTo("PUSH");
        sink.boundTo(HandlerType.PICKUP_ALERT).send(5, "k", TOKEN, "{}");
        assertThat(mapper.readTree(last.get().body()).get("channel").textValue()).isEqualTo("SMS");
    }

    @Test
    void plainPortSendIsUnsupported() {
        assertThatThrownBy(() -> sink.send(77, "k", TOKEN, "{}"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("boundTo");
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"a string\"", "[1,2]", "not json", "{\"a\":1} {\"b\":2}", "", "null"})
    void nonObjectPayloadIsPermanent(String payload) {
        assertThatThrownBy(() -> sink.send(SinkChannel.SMS, 77, "k", TOKEN, payload))
                .isInstanceOfSatisfying(PermanentJobException.class,
                        e -> assertThat(e.errorCode()).isEqualTo("INVALID_EFFECT_PAYLOAD"));
        assertThat(last.get()).isNull();
    }

    @Test
    void maps200ToAlreadyDelivered() {
        reply.set(new Reply(200, "{\"outcome\":\"ALREADY_DELIVERED\"}", 0));
        assertThat(send()).isEqualTo(DeliveryOutcome.ALREADY_DELIVERED);
    }

    @Test
    void maps409ToStaleToken() {
        reply.set(new Reply(409, "{\"code\":\"STALE_TOKEN\",\"outcome\":\"STALE_TOKEN\"}", 0));
        assertThat(send()).isEqualTo(DeliveryOutcome.STALE_TOKEN);
    }

    @Test
    void maps409WithoutCodeToStaleTokenToo() {
        reply.set(new Reply(409, "", 0));
        assertThat(send()).isEqualTo(DeliveryOutcome.STALE_TOKEN);
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 415, 404})
    void maps4xxToPermanent(int status) {
        reply.set(new Reply(status, "{\"code\":\"VALIDATION_FAILED\"}", 0));
        assertThatThrownBy(this::send)
                .isInstanceOfSatisfying(PermanentJobException.class, e -> {
                    assertThat(e.errorCode()).isEqualTo("SINK_REJECTED_" + status).matches("[A-Z][A-Z0-9_]*");
                    assertThat(e.safeMessage()).contains(String.valueOf(status));
                });
    }

    @ParameterizedTest
    @ValueSource(ints = {500, 503})
    void maps5xxToRetryable(int status) {
        reply.set(new Reply(status, "{\"code\":\"INJECTED_FAULT\"}", 0));
        assertThatThrownBy(this::send)
                .isInstanceOfSatisfying(RetryableJobException.class,
                        e -> assertThat(e.errorCode()).isEqualTo("SINK_5XX"));
    }

    @Test
    void timeoutIsRetryable() {
        reply.set(new Reply(201, "{}", 2_000));
        HttpEffectSink slow = newSink(server.getAddress().getPort(), Duration.ofMillis(200));
        assertThatThrownBy(() -> slow.send(SinkChannel.SMS, 77, "k", TOKEN, "{}"))
                .isInstanceOfSatisfying(RetryableJobException.class,
                        e -> assertThat(e.errorCode()).isEqualTo("SINK_TIMEOUT"));
    }

    @Test
    void connectionRefusedIsRetryable() throws IOException {
        int port;
        try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            port = s.getLocalPort();
        }
        HttpEffectSink dead = newSink(port, Duration.ofSeconds(2));
        assertThatThrownBy(() -> dead.send(SinkChannel.SMS, 77, "k", TOKEN, "{}"))
                .isInstanceOfSatisfying(RetryableJobException.class,
                        e -> assertThat(e.errorCode()).isEqualTo("SINK_UNAVAILABLE"));
        assertThatThrownBy(dead::highestAcceptedEpoch).isInstanceOf(SinkUnavailableException.class);
    }

    @Test
    void interruptIsRetryableAndKeepsFlag() {
        reply.set(new Reply(201, "{}", 2_000));
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(this::send)
                    .isInstanceOfSatisfying(RetryableJobException.class,
                            e -> assertThat(e.errorCode()).isEqualTo("INTERRUPTED"));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void invalidKeyIsPermanentAndNotSent() {
        assertThatThrownBy(() -> sink.send(SinkChannel.SMS, 77, "has space", TOKEN, "{}"))
                .isInstanceOf(PermanentJobException.class);
        assertThat(last.get()).isNull();
    }

    @Test
    void highestEpochOk() {
        reply.set(new Reply(200, "{\"highestEpoch\":4294967297}", 0));
        assertThat(sink.highestAcceptedEpoch()).isEqualTo(4_294_967_297L);
        assertThat(last.get().method()).isEqualTo("GET");
        assertThat(last.get().path()).isEqualTo("/api/v1/fence/highest-epoch");
    }

    @Test
    void highestEpochNon200Throws() {
        reply.set(new Reply(503, "{\"highestEpoch\":0}", 0));
        assertThatThrownBy(sink::highestAcceptedEpoch).isInstanceOf(SinkUnavailableException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"highestEpoch\":\"7\"}", "{\"highestEpoch\":7.5}", "{\"highestEpoch\":-1}",
        "{\"highestEpoch\":null}", "not json", "", "[7]", "{\"highestEpoch\":99999999999999999999}"})
    void highestEpochMalformedThrows(String body) {
        reply.set(new Reply(200, body, 0));
        assertThatThrownBy(sink::highestAcceptedEpoch).isInstanceOf(SinkUnavailableException.class);
    }

    @Test
    void keyHashIsFirst16HexOfSha256() {
        // sha256("abc") = ba7816bf8f01cfea...
        assertThat(HttpEffectSink.keyHash("abc")).isEqualTo("ba7816bf8f01cfea");
    }
}
