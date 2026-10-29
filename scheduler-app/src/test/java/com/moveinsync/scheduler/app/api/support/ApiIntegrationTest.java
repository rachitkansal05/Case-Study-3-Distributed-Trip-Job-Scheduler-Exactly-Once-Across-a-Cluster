package com.moveinsync.scheduler.app.api.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.config.SchedulerProperties;
import com.moveinsync.scheduler.app.security.JwtTokenIssuer;
import com.moveinsync.scheduler.app.security.Role;
import com.moveinsync.scheduler.app.support.AbstractEmbeddedPostgresIT;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Base class for API integration tests: the full application on a random port against embedded
 * PostgreSQL, with {@link FakeFireTimeService} as the cron engine. Requests go over real HTTP
 * (JDK {@link HttpClient}), so the security filter chain, advice and filters all run.
 *
 * <p>The database is shared across test classes: always create fresh tenants with
 * {@link #newTenant(int)} instead of assuming empty tables.
 */
@Import(ApiTestConfig.class)
public abstract class ApiIntegrationTest extends AbstractEmbeddedPostgresIT {

    private static final AtomicLong SEQ = new AtomicLong(System.nanoTime() % 1_000_000);

    protected static final String ONE_OFF_BODY_TEMPLATE = """
            {"kind":"ONE_OFF","handler":"PICKUP_ALERT","runAt":"%s",
             "payload":{"tripId":42,"phone":"+91 98450 12345"},"maxRunTime":"PT5M"}""";

    @LocalServerPort
    protected int port;
    @Autowired
    protected JdbcClient jdbc;
    @Autowired
    protected SchedulerProperties props;
    @Autowired
    protected ObjectMapper mapper;
    @Autowired
    protected FakeFireTimeService fireTimes;

    protected final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    // ------------------------------------------------------------------ tokens and tenants

    protected String token(String tenant, Role role) {
        return new JwtTokenIssuer(props.security().jwtHmacSecret())
                .issue("test-" + role.name().toLowerCase(), tenant, role, Instant.now(), Duration.ofMinutes(30));
    }

    /** Operator token; its tenant claim only namespaces idempotency keys. */
    protected String operatorToken() {
        return token("platform", Role.OPERATOR);
    }

    /** A unique tenant id, e.g. {@code t-<prefix>-123}. */
    protected static String uniqueTenantId(String prefix) {
        return "t-" + prefix + "-" + SEQ.incrementAndGet();
    }

    /** Creates a tenant in Asia/Kolkata with {@code cap} slot rows directly in the database. */
    protected String newTenant(int cap) {
        String id = uniqueTenantId(getClass().getSimpleName().toLowerCase());
        jdbc.sql("INSERT INTO tenant (id, time_zone, configured_cap) VALUES (?, 'Asia/Kolkata', ?)")
                .params(id, cap).update();
        if (cap > 0) {
            jdbc.sql("INSERT INTO tenant_slot (tenant_id, slot_no) SELECT ?, s FROM generate_series(0, ? - 1) s")
                    .params(id, cap).update();
        }
        return id;
    }

    /** Creates a one-off job (run in one hour) through the API as the tenant's ADMIN. */
    protected UUID createOneOffJob(String tenant) {
        Resp r = post("/api/v1/jobs", token(tenant, Role.ADMIN), oneOffBody(Instant.now().plusSeconds(3600)));
        if (r.status() != 201) {
            throw new IllegalStateException("job creation failed: " + r.status() + " " + r.raw());
        }
        return UUID.fromString(r.json().get("id").asText());
    }

    protected static String oneOffBody(Instant runAt) {
        return ONE_OFF_BODY_TEMPLATE.formatted(runAt);
    }

    // ------------------------------------------------------------------ runs and dead letters (engine stand-ins)

    /** Inserts a run directly, as the Engine would; returns its id. */
    protected long insertRun(UUID jobId, String tenant, String state) {
        return jdbc.sql("""
                        INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                             scheduled_at, due_at, idem_key, state)
                        SELECT j.id, j.tenant_id, j.partition_id, j.priority, j.version, j.max_run_time,
                               clock_timestamp(), clock_timestamp(), j.id || ':' || gen_random_uuid(), ?
                          FROM job j WHERE j.id = ? AND j.tenant_id = ?
                        RETURNING id""")
                .params(state, jobId, tenant)
                .query(Long.class)
                .single();
    }

    /** Marks a run RUNNING in {@code slot} with a fencing token and a live lease. */
    protected void markRunning(long runId, int slot) {
        jdbc.sql("""
                        UPDATE job_run SET state = 'RUNNING', slot_no = ?, fencing_epoch = 1,
                               fencing_seq = nextval('fence_seq'), owner_node = 'test-node', attempt = attempt + 1,
                               started_at = clock_timestamp(), lease_until = clock_timestamp() + interval '1 minute'
                         WHERE id = ?""")
                .params(slot, runId).update();
    }

    /** Dead-letters a run the way the Engine does: state, last_error and a dead_letter row. */
    protected void deadLetter(long runId, String tenant, String errorCode) {
        jdbc.sql("""
                        UPDATE job_run SET state = 'DEAD_LETTERED', attempt = 5, lease_lost_count = 2,
                               slot_no = NULL, lease_until = NULL, finished_at = clock_timestamp(),
                               last_error = ? WHERE id = ?""")
                .params(errorCode + ": handler failed", runId).update();
        jdbc.sql("INSERT INTO dead_letter (run_id, tenant_id, error_code, attempts, dead_at) "
                        + "VALUES (?, ?, ?, 5, clock_timestamp())")
                .params(runId, tenant, errorCode).update();
    }

    // ------------------------------------------------------------------ HTTP

    protected Resp get(String path, String token) {
        return send("GET", path, token, null);
    }

    protected Resp post(String path, String token, String json, String... headers) {
        return send("POST", path, token, json, headers);
    }

    protected Resp put(String path, String token, String json, String... headers) {
        return send("PUT", path, token, json, headers);
    }

    protected Resp delete(String path, String token) {
        return send("DELETE", path, token, null);
    }

    /**
     * Sends a request. {@code token} may be null (no Authorization header); {@code headers} are
     * name/value pairs, e.g. {@code "Idempotency-Key", "k1"}.
     */
    protected Resp send(String method, String path, String token, String json, String... headers) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(30))
                .method(method, json == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(json));
        if (json != null) {
            b.header("Content-Type", "application/json");
        }
        if (token != null) {
            b.header("Authorization", "Bearer " + token);
        }
        for (int i = 0; i + 1 < headers.length; i += 2) {
            b.header(headers[i], headers[i + 1]);
        }
        try {
            HttpResponse<String> r = client.send(b.build(), HttpResponse.BodyHandlers.ofString());
            return new Resp(r.statusCode(), r.body(), r, mapper);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** A response: status, raw body, parsed JSON (lazily) and headers. */
    public record Resp(int status, String raw, HttpResponse<String> response, ObjectMapper mapper) {

        public JsonNode json() {
            try {
                return raw == null || raw.isEmpty() ? mapper.nullNode() : mapper.readTree(raw);
            } catch (IOException e) {
                throw new UncheckedIOException("not JSON: " + raw, e);
            }
        }

        public String header(String name) {
            return response.headers().firstValue(name).orElse(null);
        }

        /** The problem {@code code}, or null. */
        public String code() {
            JsonNode code = json().get("code");
            return code == null ? null : code.asText();
        }

        @Override
        public String toString() {
            return status + " " + raw;
        }
    }
}
