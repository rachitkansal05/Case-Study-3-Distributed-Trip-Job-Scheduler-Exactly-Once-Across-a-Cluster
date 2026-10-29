package com.moveinsync.scheduler.app.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.OutputStreamAppender;
import ch.qos.logback.core.encoder.Encoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.config.SchedulerProperties;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.handler.BillingCloseHandler;
import com.moveinsync.scheduler.app.handler.EffectContext;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.app.handler.JobHandler;
import com.moveinsync.scheduler.app.handler.NoShowCancelHandler;
import com.moveinsync.scheduler.app.handler.RosterGenerationHandler;
import com.moveinsync.scheduler.app.security.JwtTokenIssuer;
import com.moveinsync.scheduler.app.security.Role;
import com.moveinsync.scheduler.app.support.EmbeddedPostgresSupport;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.ports.EffectSink;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import net.logstash.logback.encoder.LogstashEncoder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.boot.logging.LoggingInitializationContext;
import org.springframework.boot.logging.LoggingSystem;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

/**
 * Profile {@code json} end to end: the application's own {@code logback-spring.xml} JSON encoder
 * (captured into a buffer) writes one JSON object per line; request lines carry {@code traceId}
 * and {@code tenantId}; lines written while a run executes carry {@code nodeId, runId, jobId,
 * tenantId, fencingToken, attempt}; and none of the phone number, email or coordinates in the job
 * payload reaches the output, even though a deliberately careless handler logs the whole payload
 * and throws an exception quoting it. The PII values are obviously fake.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"test", "json"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Import(LoggingIT.Config.class)
@TestPropertySource(properties = {
    "scheduler.engine.enabled=true",
    "scheduler.engine.epoch-guard.enabled=false",
    "scheduler.engine.materializer.interval=300ms",
    "scheduler.node-id=logging-node"})
class LoggingIT {

    private static final String OWN_DATABASE = "observability_logging_it";

    // obviously fake PII
    private static final String PHONE = "+91 99999 00000";
    private static final String EMAIL = "fake.rider@example.com";
    private static final String LAT = "12.345678";
    private static final String LNG = "76.543210";
    private static final String PAIR = "11.111111, 22.222222";

    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private OutputStreamAppender<ILoggingEvent> capture;

    @DynamicPropertySource
    static void ownDatabase(DynamicPropertyRegistry registry) {
        EmbeddedPostgresSupport.createDatabase(OWN_DATABASE);
        registry.add("spring.datasource.url", () -> EmbeddedPostgresSupport.jdbcUrl(OWN_DATABASE));
        registry.add("spring.datasource.username", () -> EmbeddedPostgresSupport.USER);
        registry.add("spring.datasource.password", () -> "");
    }

    @Autowired
    private TestRestTemplate http;

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private SchedulerProperties props;

    /**
     * Loads logback-spring.xml under profile json and tees the root logger into a buffer through the
     * very encoder it configured. The explicit reload is needed because Spring Boot initialises
     * logback only once while any (cached) test context is alive, so this context's profile alone
     * would not switch an already-initialised JVM to JSON.
     */
    @BeforeAll
    @SuppressWarnings("unchecked")
    void captureJsonOutput() {
        loadLogbackConfig("test", "json");
        Logger root = ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        OutputStreamAppender<ILoggingEvent> json = (OutputStreamAppender<ILoggingEvent>) root.getAppender("JSON");
        assertThat(json).as("profile json installs the JSON console appender").isNotNull();
        Encoder<ILoggingEvent> encoder = json.getEncoder();
        assertThat(encoder).isInstanceOf(LogstashEncoder.class);
        assertThat(root.getAppender("CONSOLE")).as("no plain-text appender under json").isNull();
        capture = new OutputStreamAppender<>();
        capture.setContext(root.getLoggerContext());
        capture.setName("logging-it-capture");
        capture.setEncoder(encoder);
        capture.setOutputStream(buffer);
        capture.setImmediateFlush(true);
        capture.start();
        root.addAppender(capture);
    }

    @AfterAll
    void stopCapture() {
        Logger root = ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.detachAppender(capture);
        loadLogbackConfig("test");
    }

    private static void loadLogbackConfig(String... profiles) {
        LoggingSystem system = LoggingSystem.get(LoggingIT.class.getClassLoader());
        system.cleanUp();
        StandardEnvironment env = new StandardEnvironment();
        env.setActiveProfiles(profiles);
        system.beforeInitialize();
        system.initialize(new LoggingInitializationContext(env), "classpath:logback-spring.xml", null);
    }

    @Test
    void jsonLinesCarryMdcAndNoPii() {
        String tenant = "logging-it";
        jdbc.sql("INSERT INTO tenant (id, time_zone, configured_cap) VALUES (?, 'UTC', 2)").param(tenant).update();
        jdbc.sql("INSERT INTO tenant_slot (tenant_id, slot_no) VALUES (?, 0), (?, 1)").params(tenant, tenant).update();
        String payload = """
                {"employeeIds":["e1"],"messageTemplate":"Cab for {employeeId}","phone":"%s","email":"%s",
                 "lat": %s, "lng": %s, "pickup":"%s"}""".formatted(PHONE, EMAIL, LAT, LNG, PAIR);
        String body = """
                {"kind":"ONE_OFF","handler":"PICKUP_ALERT","runAt":"%s","payload":%s,"maxRunTime":"PT1M"}"""
                .formatted(Instant.now(), payload);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(new JwtTokenIssuer(props.security().jwtHmacSecret())
                .issue("logging-it-admin", tenant, Role.ADMIN, Instant.now(), Duration.ofMinutes(10)));
        ResponseEntity<JsonNode> created = http.postForEntity("/api/v1/jobs", new HttpEntity<>(body, headers),
                JsonNode.class);
        assertThat(created.getStatusCode().value()).as(String.valueOf(created.getBody())).isEqualTo(201);
        String jobId = created.getBody().get("id").asText();
        String traceId = created.getHeaders().getFirst("X-Trace-Id");
        assertThat(traceId).isNotBlank();

        await().atMost(Duration.ofSeconds(30)).until(() -> jdbc.sql(
                "SELECT count(*) FROM job_run WHERE job_id = ? AND state = 'SUCCEEDED'")
                .param(UUID.fromString(jobId)).query(Long.class).single() == 1);
        long runId = jdbc.sql("SELECT id FROM job_run WHERE job_id = ?").param(UUID.fromString(jobId))
                .query(Long.class).single();

        List<JsonNode> lines = await().atMost(Duration.ofSeconds(5)).until(this::lines,
                l -> l.stream().anyMatch(message(m -> m.startsWith("leaky handler"))));

        // per request: traceId and tenantId
        JsonNode createdLine = only(lines, message(m -> m.startsWith("job " + jobId + " created")));
        assertThat(createdLine.path("traceId").asText()).isEqualTo(traceId);
        assertThat(createdLine.path("tenantId").asText()).isEqualTo(tenant);

        // while the run executes: the run MDC
        JsonNode runLine = only(lines, message(m -> m.startsWith("leaky handler payload")));
        assertThat(runLine.path("nodeId").asText()).isEqualTo("logging-node");
        assertThat(runLine.path("runId").asText()).isEqualTo(String.valueOf(runId));
        assertThat(runLine.path("jobId").asText()).isEqualTo(jobId);
        assertThat(runLine.path("tenantId").asText()).isEqualTo(tenant);
        assertThat(runLine.path("fencingToken").asText()).matches("\\d+\\.\\d+");
        assertThat(runLine.path("attempt").asText()).isEqualTo("1");
        JsonNode errorLine = only(lines, message(m -> m.startsWith("leaky handler failure")));
        assertThat(errorLine.path("stack_trace").asText()).contains("IllegalStateException").contains("[phone]");

        // no PII anywhere, and it was masked (not just absent)
        String all = buffer.toString(StandardCharsets.UTF_8);
        assertThat(all).doesNotContain("99999", "00000", "fake.rider", "example.com", LAT, LNG, "11.111111",
                "22.222222");
        assertThat(runLine.path("message").asText()).contains("[phone]", "[email]", "[geo]");
        assertThat(all).contains(String.valueOf(runId), tenant, runLine.path("fencingToken").asText());
    }

    /** Every captured line is exactly one JSON object. */
    private List<JsonNode> lines() {
        ObjectMapper mapper = new ObjectMapper();
        List<JsonNode> parsed = new ArrayList<>();
        for (String line : buffer.toString(StandardCharsets.UTF_8).split("\\R")) {
            if (line.isEmpty()) {
                continue;
            }
            try {
                JsonNode node = mapper.readTree(line);
                assertThat(node.isObject()).as("JSON object per line: %s", line).isTrue();
                assertThat(node.has("@timestamp") && node.has("level") && node.has("logger_name")).as(line).isTrue();
                parsed.add(node);
            } catch (IOException e) {
                throw new UncheckedIOException("not one JSON object per line: " + line, e);
            }
        }
        return parsed;
    }

    private static Predicate<JsonNode> message(Predicate<String> test) {
        return n -> test.test(n.path("message").asText());
    }

    private static JsonNode only(List<JsonNode> lines, Predicate<JsonNode> filter) {
        List<JsonNode> match = lines.stream().filter(filter).toList();
        assertThat(match).as("exactly one matching line").hasSize(1);
        return match.get(0);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Config {

        @Bean
        @Primary
        EffectSink fakeEffectSink() {
            return new FakeEffectSink();
        }

        @Bean
        @Primary
        HandlerRegistry leakyHandlers(ObjectMapper mapper) {
            return new HandlerRegistry(List.of(new LeakyHandler(mapper), new RosterGenerationHandler(mapper),
                    new NoShowCancelHandler(mapper), new BillingCloseHandler(mapper)));
        }
    }

    /** A PICKUP_ALERT handler that breaks the "never log payloads" rule, to prove masking is the last line of defence. */
    static final class LeakyHandler extends JobHandler<String> {

        private static final org.slf4j.Logger log = LoggerFactory.getLogger(LeakyHandler.class);

        LeakyHandler(ObjectMapper mapper) {
            super(mapper);
        }

        @Override
        public HandlerType type() {
            return HandlerType.PICKUP_ALERT;
        }

        @Override
        protected String parsePayload(String payloadJson) {
            return payloadJson;
        }

        @Override
        protected void execute(EffectContext ctx, String payload) {
            log.info("leaky handler payload {}", payload);
            log.warn("leaky handler failure", new IllegalStateException("could not notify " + payload));
            ctx.send(ctx.run().itemKey("e1"), "{\"employeeId\":\"e1\",\"text\":\"cab\"}");
        }
    }
}
