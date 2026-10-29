package com.moveinsync.scheduler.app.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.api.support.ApiIntegrationTest;
import com.moveinsync.scheduler.app.security.Role;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Tenant creation and cap changes (PLAN.md section 10 "Changing a cap", R7): the effective cap is
 * the number of {@code tenant_slot} rows; raising inserts the missing rows at once, lowering
 * deletes only free rows at or above the new cap (a slot held by a RUNNING run survives and the
 * tenant reports {@code draining}), and concurrent changes serialise on the tenant row so the slot
 * rows always end up exactly {@code 0 .. configured-1}.
 */
class CapApiIT extends ApiIntegrationTest {

    // ------------------------------------------------------------------ POST /tenants

    @Test
    void createTenantInsertsCapSlotRows() {
        String id = uniqueTenantId("cap-new");

        Resp r = post("/api/v1/tenants", operatorToken(), """
                {"id":"%s","timeZone":"Europe/London","cap":3}""".formatted(id));

        assertThat(r.status()).as(r.toString()).isEqualTo(201);
        assertThat(r.header("Location")).endsWith("/api/v1/tenants/" + id);
        assertThat(r.json().get("id").asText()).isEqualTo(id);
        assertThat(r.json().get("timeZone").asText()).isEqualTo("Europe/London");
        assertThat(r.json().get("paused").asBoolean()).isFalse();
        assertThat(r.json().get("configuredCap").asInt()).isEqualTo(3);
        assertThat(r.json().get("effectiveCap").asInt()).isEqualTo(3);
        assertThat(r.json().get("draining").asBoolean()).isFalse();
        assertThat(slots(id)).containsExactly(0, 1, 2);
        assertThat(configuredCap(id)).isEqualTo(3);
        assertThat(jdbc.sql("SELECT time_zone FROM tenant WHERE id = ?").param(id).query(String.class).single())
                .isEqualTo("Europe/London");
    }

    @Test
    void createTenantWithoutCapUsesTheDefaultCap() {
        String id = uniqueTenantId("cap-default");
        int defaultCap = props.tenant().defaultCap();

        Resp r = post("/api/v1/tenants", operatorToken(), """
                {"id":"%s","timeZone":"Asia/Kolkata"}""".formatted(id));

        assertThat(r.status()).as(r.toString()).isEqualTo(201);
        assertThat(r.json().get("configuredCap").asInt()).isEqualTo(defaultCap);
        assertThat(slots(id)).containsExactlyElementsOf(range(defaultCap));
        assertThat(configuredCap(id)).isEqualTo(defaultCap);
    }

    @Test
    void createTenantWithCapZeroHasNoSlots() {
        String id = uniqueTenantId("cap-zero");
        Resp r = post("/api/v1/tenants", operatorToken(), """
                {"id":"%s","timeZone":"Asia/Kolkata","cap":0}""".formatted(id));
        assertThat(r.status()).as(r.toString()).isEqualTo(201);
        assertThat(slots(id)).isEmpty();
    }

    @Test
    void duplicateTenantIs409AndChangesNothing() {
        String id = uniqueTenantId("cap-dup");
        assertThat(post("/api/v1/tenants", operatorToken(), """
                {"id":"%s","timeZone":"Asia/Kolkata","cap":2}""".formatted(id)).status()).isEqualTo(201);

        Resp dup = post("/api/v1/tenants", operatorToken(), """
                {"id":"%s","timeZone":"Europe/London","cap":7}""".formatted(id));

        assertThat(dup.status()).as(dup.toString()).isEqualTo(409);
        assertThat(dup.code()).isEqualTo("TENANT_EXISTS");
        assertThat(slots(id)).containsExactly(0, 1);
        assertThat(configuredCap(id)).isEqualTo(2);
    }

    @Test
    void badTimeZoneIs400OnTimeZone() {
        for (String zone : List.of("Mars/Olympus_Mons", "+05:30", "UTC+5")) {
            String id = uniqueTenantId("cap-zone");
            Resp r = post("/api/v1/tenants", operatorToken(), """
                    {"id":"%s","timeZone":"%s","cap":1}""".formatted(id, zone));
            assertThat(r.status()).as(zone + " " + r).isEqualTo(400);
            assertThat(r.json().get("errors").get(0).get("field").asText()).isEqualTo("timeZone");
            assertThat(r.raw()).doesNotContain(zone);
            assertThat(jdbc.sql("SELECT count(*) FROM tenant WHERE id = ?").param(id).query(Long.class).single())
                    .isZero();
        }
    }

    // ------------------------------------------------------------------ PUT /tenants/{t}/cap

    @Test
    void raisingInsertsMissingSlots() {
        String tenant = newTenant(4);

        Resp r = putCap(tenant, 8);

        assertCap(r, tenant, 8, 8, false);
        assertThat(slots(tenant)).containsExactlyElementsOf(range(8));
        assertThat(configuredCap(tenant)).isEqualTo(8);
    }

    @Test
    void adminCanChangeOwnCap() {
        String tenant = newTenant(1);
        Resp r = put("/api/v1/tenants/" + tenant + "/cap", token(tenant, Role.ADMIN), "{\"cap\":2}");
        assertCap(r, tenant, 2, 2, false);
    }

    @Test
    void loweringWithoutRunsDeletesHighSlots() {
        String tenant = newTenant(8);

        Resp r = putCap(tenant, 2);

        assertCap(r, tenant, 2, 2, false);
        assertThat(slots(tenant)).containsExactly(0, 1);
        assertThat(configuredCap(tenant)).isEqualTo(2);
    }

    @Test
    void loweringKeepsASlotHeldByARunningRunAndReportsDraining() {
        String tenant = newTenant(6);
        UUID job = createOneOffJob(tenant);
        long run = insertRun(job, tenant, "SCHEDULED");
        markRunning(run, 5);

        Resp lowered = putCap(tenant, 2);

        assertCap(lowered, tenant, 2, 3, true);
        assertThat(slots(tenant)).containsExactly(0, 1, 5);
        assertThat(configuredCap(tenant)).isEqualTo(2);
        assertThat(runStateAndSlot(run)).isEqualTo("RUNNING/5");

        Resp raised = putCap(tenant, 4);

        assertCap(raised, tenant, 4, 5, true);
        assertThat(slots(tenant)).containsExactly(0, 1, 2, 3, 5);
        assertThat(runStateAndSlot(run)).isEqualTo("RUNNING/5");

        // once the run has released its slot, the next lowering removes it
        jdbc.sql("UPDATE job_run SET state = 'SUCCEEDED', slot_no = NULL, lease_until = NULL, "
                + "finished_at = clock_timestamp() WHERE id = ?").param(run).update();
        assertCap(putCap(tenant, 4), tenant, 4, 4, false);
        assertThat(slots(tenant)).containsExactly(0, 1, 2, 3);
    }

    @Test
    void capZeroDeletesAllFreeSlots() {
        String tenant = newTenant(5);

        assertCap(putCap(tenant, 0), tenant, 0, 0, false);
        assertThat(slots(tenant)).isEmpty();

        UUID job = createOneOffJob(tenant);
        assertCap(putCap(tenant, 3), tenant, 3, 3, false);
        long run = insertRun(job, tenant, "SCHEDULED");
        markRunning(run, 2);
        assertCap(putCap(tenant, 0), tenant, 0, 1, true);
        assertThat(slots(tenant)).containsExactly(2);
    }

    @Test
    void concurrentCapChangesSerialiseAndLeaveExactSlotRows() throws Exception {
        String tenant = newTenant(4);
        int threads = 10;
        List<Integer> requested = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            requested.add(ThreadLocalRandom.current().nextInt(0, 21));
        }
        long versionBefore = tenantVersion(tenant);
        String operator = operatorToken();
        CyclicBarrier start = new CyclicBarrier(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Resp> responses = new ArrayList<>();
        try {
            List<Future<Resp>> futures = new ArrayList<>();
            for (int cap : requested) {
                futures.add(pool.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    return put("/api/v1/tenants/" + tenant + "/cap", operator, "{\"cap\":" + cap + "}");
                }));
            }
            for (Future<Resp> f : futures) {
                responses.add(f.get(60, TimeUnit.SECONDS));
            }
        } finally {
            pool.shutdownNow();
        }

        for (int i = 0; i < threads; i++) {
            Resp r = responses.get(i);
            assertThat(r.status()).as(r.toString()).isEqualTo(200);
            // no running runs: each change, applied alone, leaves exactly its own cap
            assertCap(r, tenant, requested.get(i), requested.get(i), false);
        }
        int finalCap = configuredCap(tenant);
        assertThat(requested).contains(finalCap);
        assertThat(slots(tenant)).containsExactlyElementsOf(range(finalCap));
        // every change committed, one after the other
        assertThat(tenantVersion(tenant)).isEqualTo(versionBefore + threads);
    }

    @Test
    void capOutOfRangeOrMissingIs400OnCap() {
        String tenant = newTenant(3);
        for (String body : List.of("{\"cap\":-1}", "{\"cap\":10001}", "{}", "{\"cap\":null}", "{\"cap\":\"many\"}")) {
            Resp r = put("/api/v1/tenants/" + tenant + "/cap", operatorToken(), body);
            assertThat(r.status()).as(body + " " + r).isEqualTo(400);
            assertThat(r.json().get("errors").get(0).get("field").asText()).as(body).isEqualTo("cap");
        }
        assertThat(slots(tenant)).containsExactly(0, 1, 2);
        assertThat(configuredCap(tenant)).isEqualTo(3);

        assertCap(putCap(tenant, 10_000), tenant, 10_000, 10_000, false);
        assertCap(putCap(tenant, 1), tenant, 1, 1, false);
    }

    @Test
    void capOfUnknownTenantIs404() {
        Resp r = putCap(uniqueTenantId("cap-missing"), 3);
        assertThat(r.status()).isEqualTo(404);
        assertThat(r.code()).isEqualTo("TENANT_NOT_FOUND");
    }

    // ------------------------------------------------------------------ helpers

    private Resp putCap(String tenant, int cap) {
        return put("/api/v1/tenants/" + tenant + "/cap", operatorToken(), "{\"cap\":" + cap + "}");
    }

    private static void assertCap(Resp r, String tenant, int configured, int effective, boolean draining) {
        assertThat(r.status()).as(r.toString()).isEqualTo(200);
        assertThat(r.json().get("tenantId").asText()).isEqualTo(tenant);
        assertThat(r.json().get("configuredCap").asInt()).as("configuredCap").isEqualTo(configured);
        assertThat(r.json().get("effectiveCap").asInt()).as("effectiveCap").isEqualTo(effective);
        assertThat(r.json().get("draining").asBoolean()).as("draining").isEqualTo(draining);
    }

    private List<Integer> slots(String tenant) {
        return jdbc.sql("SELECT slot_no FROM tenant_slot WHERE tenant_id = ? ORDER BY slot_no")
                .param(tenant).query(Integer.class).list();
    }

    private static List<Integer> range(int n) {
        return IntStream.range(0, n).boxed().toList();
    }

    private int configuredCap(String tenant) {
        return jdbc.sql("SELECT configured_cap FROM tenant WHERE id = ?").param(tenant).query(Integer.class).single();
    }

    private long tenantVersion(String tenant) {
        return jdbc.sql("SELECT version FROM tenant WHERE id = ?").param(tenant).query(Long.class).single();
    }

    private String runStateAndSlot(long run) {
        return jdbc.sql("SELECT state || '/' || slot_no FROM job_run WHERE id = ?").param(run)
                .query(String.class).single();
    }
}
