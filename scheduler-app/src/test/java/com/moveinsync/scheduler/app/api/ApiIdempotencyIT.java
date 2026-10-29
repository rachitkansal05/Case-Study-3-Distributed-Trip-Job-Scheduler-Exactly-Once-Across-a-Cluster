package com.moveinsync.scheduler.app.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.api.support.ApiIntegrationTest;
import com.moveinsync.scheduler.app.security.Role;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Retry-safe writes (PLAN.md section 13, R9): with an {@code Idempotency-Key}, a retried
 * {@code POST /jobs}, trigger, pause or cap change performs its action once and replays the stored
 * status, {@code Location} and body ({@code Idempotent-Replayed: true}); the same key with another
 * request is a 409 {@code IDEMPOTENCY_KEY_REUSED}; keys are namespaced per tenant; failed requests
 * store nothing; records older than 24 h are replaced; concurrent duplicates yield one action.
 */
class ApiIdempotencyIT extends ApiIntegrationTest {

    private static final String KEY = "Idempotency-Key";
    private static final String REPLAYED = "Idempotent-Replayed";
    private static final int THREADS = 12;

    // ------------------------------------------------------------------ POST /jobs

    @Test
    void retriedCreateWithSameKeyCreatesOneJobAndReplaysTheResponse() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        String key = key();
        String body = oneOffBody(runAt());

        Resp first = post("/api/v1/jobs", admin, body, KEY, key);
        Resp retry = post("/api/v1/jobs", admin, body, KEY, key);

        assertThat(first.status()).as(first.toString()).isEqualTo(201);
        assertThat(first.header(REPLAYED)).isNull();
        assertThat(retry.status()).as(retry.toString()).isEqualTo(201);
        assertThat(retry.header(REPLAYED)).isEqualTo("true");
        assertThat(retry.json().get("id").asText()).isEqualTo(first.json().get("id").asText());
        assertThat(retry.header("Location")).isNotNull().isEqualTo(first.header("Location"))
                .endsWith("/api/v1/jobs/" + first.json().get("id").asText());
        assertThat(retry.json()).isEqualTo(first.json());
        assertThat(jobCount(tenant)).isEqualTo(1);
        assertThat(idempotencyRows(tenant, key)).isEqualTo(1);
    }

    @Test
    void keyOrderInTheBodyDoesNotMatter() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        String key = key();
        Instant at = runAt();
        String body = """
                {"kind":"ONE_OFF","handler":"PICKUP_ALERT","runAt":"%s",
                 "payload":{"tripId":42,"note":"a"},"maxRunTime":"PT5M","priority":3}""".formatted(at);
        String reordered = """
                {"priority":3,"maxRunTime":"PT5M","payload":{"note":"a","tripId":42},
                 "runAt":"%s","handler":"PICKUP_ALERT","kind":"ONE_OFF"}""".formatted(at);

        Resp first = post("/api/v1/jobs", admin, body, KEY, key);
        Resp retry = post("/api/v1/jobs", admin, reordered, KEY, key);

        assertThat(first.status()).as(first.toString()).isEqualTo(201);
        assertThat(retry.status()).as(retry.toString()).isEqualTo(201);
        assertThat(retry.header(REPLAYED)).isEqualTo("true");
        assertThat(retry.json().get("id")).isEqualTo(first.json().get("id"));
        assertThat(jobCount(tenant)).isEqualTo(1);
    }

    @Test
    void sameKeyWithDifferentBodyIs409AndCreatesNothing() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        String key = key();

        Resp first = post("/api/v1/jobs", admin, oneOffBody(runAt()), KEY, key);
        Resp other = post("/api/v1/jobs", admin, oneOffBody(runAt().plusSeconds(60)), KEY, key);

        assertThat(first.status()).isEqualTo(201);
        assertThat(other.status()).as(other.toString()).isEqualTo(409);
        assertThat(other.code()).isEqualTo("IDEMPOTENCY_KEY_REUSED");
        assertThat(other.header(REPLAYED)).isNull();
        assertThat(jobCount(tenant)).isEqualTo(1);
    }

    @Test
    void sameKeyOnADifferentEndpointIs409AndDoesNotRunTheAction() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        String key = key();

        assertThat(post("/api/v1/jobs", admin, oneOffBody(runAt()), KEY, key).status()).isEqualTo(201);
        Resp pause = post("/api/v1/tenants/" + tenant + "/pause", admin, null, KEY, key);

        assertThat(pause.status()).as(pause.toString()).isEqualTo(409);
        assertThat(pause.code()).isEqualTo("IDEMPOTENCY_KEY_REUSED");
        assertThat(paused(tenant)).isFalse();

        UUID job = createOneOffJob(tenant);
        Resp trigger = post("/api/v1/jobs/" + job + "/trigger", admin, null, KEY, key);
        assertThat(trigger.status()).as(trigger.toString()).isEqualTo(409);
        assertThat(trigger.code()).isEqualTo("IDEMPOTENCY_KEY_REUSED");
        assertThat(runCount(job)).isZero();
    }

    @Test
    void sameKeyInTwoTenantsIsIndependent() {
        String a = newTenant(1);
        String b = newTenant(1);
        String key = key();
        String body = oneOffBody(runAt());

        Resp ra = post("/api/v1/jobs", token(a, Role.ADMIN), body, KEY, key);
        Resp rb = post("/api/v1/jobs", token(b, Role.ADMIN), body, KEY, key);

        assertThat(ra.status()).isEqualTo(201);
        assertThat(rb.status()).as(rb.toString()).isEqualTo(201);
        assertThat(rb.header(REPLAYED)).isNull();
        assertThat(rb.json().get("id")).isNotEqualTo(ra.json().get("id"));
        assertThat(rb.json().get("tenantId").asText()).isEqualTo(b);
        assertThat(jobCount(a)).isEqualTo(1);
        assertThat(jobCount(b)).isEqualTo(1);
        assertThat(idempotencyRows(a, key)).isEqualTo(1);
        assertThat(idempotencyRows(b, key)).isEqualTo(1);
    }

    @Test
    void failedRequestStoresNothingAndTheKeyCanThenBeUsed() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        String key = key();
        // fails inside the idempotent action (service-level kind rule), so the transaction rolls back
        String invalid = """
                {"kind":"ONE_OFF","handler":"PICKUP_ALERT","runAt":"%s","cron":"%s",
                 "payload":{},"maxRunTime":"PT5M"}""".formatted(runAt(), "0 0 * * * *");

        Resp failed = post("/api/v1/jobs", admin, invalid, KEY, key);
        assertThat(failed.status()).as(failed.toString()).isEqualTo(400);
        assertThat(idempotencyRows(tenant, key)).isZero();

        Resp again = post("/api/v1/jobs", admin, invalid, KEY, key);
        assertThat(again.status()).isEqualTo(400);
        assertThat(again.header(REPLAYED)).isNull();

        Resp ok = post("/api/v1/jobs", admin, oneOffBody(runAt()), KEY, key);
        assertThat(ok.status()).as(ok.toString()).isEqualTo(201);
        assertThat(ok.header(REPLAYED)).isNull();
        assertThat(jobCount(tenant)).isEqualTo(1);
        assertThat(idempotencyRows(tenant, key)).isEqualTo(1);
    }

    @Test
    void recordOlderThan24HoursIsIgnoredAndReplaced() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        String key = key();
        String body = oneOffBody(runAt());

        Resp first = post("/api/v1/jobs", admin, body, KEY, key);
        assertThat(first.status()).isEqualTo(201);
        jdbc.sql("UPDATE api_idempotency SET created_at = clock_timestamp() - interval '25 hours' "
                        + "WHERE tenant_id = ? AND idem_key = ?")
                .params(tenant, key).update();

        Resp second = post("/api/v1/jobs", admin, body, KEY, key);

        assertThat(second.status()).as(second.toString()).isEqualTo(201);
        assertThat(second.header(REPLAYED)).isNull();
        assertThat(second.json().get("id")).isNotEqualTo(first.json().get("id"));
        assertThat(jobCount(tenant)).isEqualTo(2);
        assertThat(idempotencyRows(tenant, key)).isEqualTo(1);
        Boolean fresh = jdbc.sql("SELECT created_at > clock_timestamp() - interval '1 minute' FROM api_idempotency "
                        + "WHERE tenant_id = ? AND idem_key = ?")
                .params(tenant, key).query(Boolean.class).single();
        assertThat(fresh).isTrue();

        // the replaced record is the one now replayed
        Resp third = post("/api/v1/jobs", admin, body, KEY, key);
        assertThat(third.header(REPLAYED)).isEqualTo("true");
        assertThat(third.json().get("id")).isEqualTo(second.json().get("id"));
        assertThat(jobCount(tenant)).isEqualTo(2);
    }

    @Test
    void invalidKeyIs400NamingTheHeaderWithoutEchoingIt() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        UUID job = createOneOffJob(tenant);
        String spaced = "has space-" + UUID.randomUUID();
        String tooLong = "k".repeat(129);
        String veryLong = "z".repeat(200);

        for (String bad : List.of(spaced, tooLong, veryLong)) {
            Resp create = post("/api/v1/jobs", admin, oneOffBody(runAt()), KEY, bad);
            assertThat(create.status()).as(create.toString()).isEqualTo(400);
            assertThat(create.code()).isEqualTo("VALIDATION_FAILED");
            assertThat(create.json().get("errors").get(0).get("field").asText()).isEqualTo(KEY);
            assertThat(create.raw()).doesNotContain(bad);

            Resp trigger = post("/api/v1/jobs/" + job + "/trigger", admin, null, KEY, bad);
            assertThat(trigger.status()).as(trigger.toString()).isEqualTo(400);
            assertThat(trigger.json().get("errors").get(0).get("field").asText()).isEqualTo(KEY);
            assertThat(trigger.raw()).doesNotContain(bad);

            Resp pause = post("/api/v1/tenants/" + tenant + "/pause", admin, null, KEY, bad);
            assertThat(pause.status()).as(pause.toString()).isEqualTo(400);
            assertThat(pause.json().get("errors").get(0).get("field").asText()).isEqualTo(KEY);
        }
        // a 128-character key is still valid
        Resp max = post("/api/v1/jobs", admin, oneOffBody(runAt()), KEY, "m".repeat(128));
        assertThat(max.status()).as(max.toString()).isEqualTo(201);

        assertThat(jobCount(tenant)).isEqualTo(2);
        assertThat(runCount(job)).isZero();
        assertThat(paused(tenant)).isFalse();
    }

    // ------------------------------------------------------------------ pause / cap

    @Test
    void pauseWithKeyIsReplayedWithoutRunningTheActionAgain() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        String key = key();
        String path = "/api/v1/tenants/" + tenant + "/pause";

        Resp first = post(path, admin, null, KEY, key);
        assertThat(first.status()).isEqualTo(200);
        assertThat(first.json().get("paused").asBoolean()).isTrue();
        assertThat(paused(tenant)).isTrue();

        assertThat(post("/api/v1/tenants/" + tenant + "/resume", admin, null).status()).isEqualTo(200);
        assertThat(paused(tenant)).isFalse();

        Resp retry = post(path, admin, null, KEY, key);
        assertThat(retry.status()).isEqualTo(200);
        assertThat(retry.header(REPLAYED)).isEqualTo("true");
        assertThat(retry.json()).isEqualTo(first.json());
        // replayed, not re-executed: the later resume stands
        assertThat(paused(tenant)).isFalse();
    }

    @Test
    void capChangeWithKeyIsReplayedAndADifferentCapWithTheSameKeyIs409() {
        String tenant = newTenant(2);
        String operator = operatorToken();
        String key = key();
        String path = "/api/v1/tenants/" + tenant + "/cap";

        Resp first = put(path, operator, "{\"cap\":5}", KEY, key);
        assertThat(first.status()).as(first.toString()).isEqualTo(200);
        assertThat(first.json().get("configuredCap").asInt()).isEqualTo(5);

        // change the cap without a key, then retry the keyed request
        assertThat(put(path, operator, "{\"cap\":3}").status()).isEqualTo(200);
        Resp retry = put(path, operator, "{\"cap\":5}", KEY, key);
        assertThat(retry.status()).isEqualTo(200);
        assertThat(retry.header(REPLAYED)).isEqualTo("true");
        assertThat(retry.json()).isEqualTo(first.json());
        assertThat(slotCount(tenant)).isEqualTo(3);

        Resp other = put(path, operator, "{\"cap\":4}", KEY, key);
        assertThat(other.status()).isEqualTo(409);
        assertThat(other.code()).isEqualTo("IDEMPOTENCY_KEY_REUSED");
        assertThat(slotCount(tenant)).isEqualTo(3);

        // operator keys live in their own namespace
        assertThat(idempotencyRows("operator:platform", key)).isEqualTo(1);
        assertThat(idempotencyRows(tenant, key)).isZero();
    }

    // ------------------------------------------------------------------ concurrency

    @Test
    void concurrentIdenticalCreatesYieldExactlyOneJob() throws Exception {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        String key = key();
        String body = oneOffBody(runAt());

        List<Resp> responses = concurrently(() -> post("/api/v1/jobs", admin, body, KEY, key));

        assertThat(responses).allSatisfy(r -> assertThat(r.status()).as(r.toString()).isEqualTo(201));
        Set<String> ids = responses.stream().map(r -> r.json().get("id").asText()).collect(Collectors.toSet());
        assertThat(ids).hasSize(1);
        assertThat(responses.stream().filter(r -> r.header(REPLAYED) == null).count()).isEqualTo(1);
        assertThat(jobCount(tenant)).isEqualTo(1);
        assertThat(jdbc.sql("SELECT id::text FROM job WHERE tenant_id = ?").param(tenant).query(String.class).single())
                .isEqualTo(ids.iterator().next());
    }

    @Test
    void concurrentIdenticalTriggersYieldExactlyOneRun() throws Exception {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        UUID job = createOneOffJob(tenant);
        String key = key();

        List<Resp> responses = concurrently(() -> post("/api/v1/jobs/" + job + "/trigger", admin, null, KEY, key));

        assertThat(responses).allSatisfy(r -> assertThat(r.status()).as(r.toString()).isEqualTo(202));
        Set<Long> runIds = responses.stream().map(r -> r.json().get("runId").asLong()).collect(Collectors.toSet());
        assertThat(runIds).hasSize(1);
        assertThat(runCount(job)).isEqualTo(1);
        assertThat(jdbc.sql("SELECT id FROM job_run WHERE job_id = ?").param(job).query(Long.class).single())
                .isEqualTo(runIds.iterator().next());
    }

    // ------------------------------------------------------------------ helpers

    private List<Resp> concurrently(Supplier<Resp> request) throws Exception {
        CyclicBarrier start = new CyclicBarrier(THREADS);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            List<Future<Resp>> futures = new ArrayList<>();
            for (int i = 0; i < THREADS; i++) {
                futures.add(pool.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    return request.get();
                }));
            }
            List<Resp> out = new ArrayList<>();
            for (Future<Resp> f : futures) {
                out.add(f.get(60, TimeUnit.SECONDS));
            }
            return out;
        } finally {
            pool.shutdownNow();
        }
    }

    private static String key() {
        return "idem-" + UUID.randomUUID();
    }

    private static Instant runAt() {
        return Instant.now().plus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS);
    }

    private long jobCount(String tenant) {
        return jdbc.sql("SELECT count(*) FROM job WHERE tenant_id = ?").param(tenant).query(Long.class).single();
    }

    private long runCount(UUID job) {
        return jdbc.sql("SELECT count(*) FROM job_run WHERE job_id = ?").param(job).query(Long.class).single();
    }

    private long slotCount(String tenant) {
        return jdbc.sql("SELECT count(*) FROM tenant_slot WHERE tenant_id = ?").param(tenant).query(Long.class).single();
    }

    private long idempotencyRows(String namespace, String key) {
        return jdbc.sql("SELECT count(*) FROM api_idempotency WHERE tenant_id = ? AND idem_key = ?")
                .params(namespace, key).query(Long.class).single();
    }

    private boolean paused(String tenant) {
        return jdbc.sql("SELECT paused FROM tenant WHERE id = ?").param(tenant).query(Boolean.class).single();
    }
}
