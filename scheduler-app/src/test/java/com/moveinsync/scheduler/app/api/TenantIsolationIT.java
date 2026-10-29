package com.moveinsync.scheduler.app.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.moveinsync.scheduler.app.api.support.ApiIntegrationTest;
import com.moveinsync.scheduler.app.security.Role;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Tenant isolation (PLAN.md section 13, R14): the tenant comes only from the JWT, every read and
 * write of another tenant's resource answers 404 (never 403, and indistinguishable from a resource
 * that does not exist), cross-tenant writes change nothing, and OPERATOR crosses tenants without
 * ever seeing payloads.
 */
class TenantIsolationIT extends ApiIntegrationTest {

    private static final String TRIGGER_KEY_HEADER = "Idempotency-Key";

    // ------------------------------------------------------------------ reads

    @Test
    void adminAndEmployeeOfAnotherTenantGet404OnJobReads() {
        String a = newTenant(2);
        String b = newTenant(2);
        UUID jobB = createOneOffJob(b);
        insertRun(jobB, b, "SCHEDULED");

        for (Role role : List.of(Role.ADMIN, Role.EMPLOYEE)) {
            Resp job = get("/api/v1/jobs/" + jobB, token(a, role));
            assertThat(job.status()).as(role + " GET job").isEqualTo(404);
            assertThat(job.code()).isEqualTo("JOB_NOT_FOUND");
            assertThat(job.raw()).doesNotContain("98450").doesNotContain(b);

            Resp next = get("/api/v1/jobs/" + jobB + "/next-fire-times?count=3", token(a, role));
            assertThat(next.status()).as(role + " next-fire-times").isEqualTo(404);
            assertThat(next.code()).isEqualTo("JOB_NOT_FOUND");
        }

        Resp runs = get("/api/v1/jobs/" + jobB + "/runs", token(a, Role.ADMIN));
        assertThat(runs.status()).isEqualTo(404);
        assertThat(runs.code()).isEqualTo("JOB_NOT_FOUND");
    }

    @Test
    void crossTenantJob404IsIdenticalToNonExistentJob404() {
        String a = newTenant(1);
        String b = newTenant(1);
        UUID jobB = createOneOffJob(b);
        String tokenA = token(a, Role.ADMIN);

        for (String suffix : List.of("", "/next-fire-times", "/runs")) {
            Resp foreign = get("/api/v1/jobs/" + jobB + suffix, tokenA);
            Resp missing = get("/api/v1/jobs/" + UUID.randomUUID() + suffix, tokenA);
            assertThat(foreign.status()).as(suffix).isEqualTo(404).isEqualTo(missing.status());
            assertThat(fieldNames(foreign.json())).as(suffix).isEqualTo(fieldNames(missing.json()));
            for (String field : List.of("type", "title", "status", "detail", "code")) {
                assertThat(foreign.json().get(field)).as(suffix + " " + field).isEqualTo(missing.json().get(field));
            }
            assertThat(foreign.header("Content-Type")).isEqualTo(missing.header("Content-Type"));
        }
    }

    @Test
    void operatorReadsAnotherTenantsJobWithoutPayload() {
        String b = newTenant(1);
        UUID jobB = createOneOffJob(b);

        Resp r = get("/api/v1/jobs/" + jobB, operatorToken());
        assertThat(r.status()).isEqualTo(200);
        assertThat(r.json().get("tenantId").asText()).isEqualTo(b);
        assertThat(r.json().has("payload")).isFalse();
        assertThat(r.json().get("payloadRedacted").asBoolean()).isTrue();
        assertThat(r.raw()).doesNotContain("98450").doesNotContain("tripId");

        // and the owning ADMIN does see it
        Resp own = get("/api/v1/jobs/" + jobB, token(b, Role.ADMIN));
        assertThat(own.json().get("payload").get("tripId").asInt()).isEqualTo(42);
        assertThat(own.json().get("payloadRedacted").asBoolean()).isFalse();
    }

    // ------------------------------------------------------------------ job writes

    @Test
    void crossTenantTriggerIs404AndInsertsNoRun() {
        String a = newTenant(1);
        String b = newTenant(1);
        UUID jobB = createOneOffJob(b);
        long before = runCount(jobB);

        Resp r = post("/api/v1/jobs/" + jobB + "/trigger", token(a, Role.ADMIN), null,
                TRIGGER_KEY_HEADER, "iso-" + UUID.randomUUID());
        assertThat(r.status()).isEqualTo(404);
        assertThat(r.code()).isEqualTo("JOB_NOT_FOUND");
        assertThat(runCount(jobB)).isEqualTo(before);
    }

    @Test
    void crossTenantDeleteIs404AndLeavesJobAndRunsUntouched() {
        String a = newTenant(1);
        String b = newTenant(1);
        UUID jobB = createOneOffJob(b);
        long run = insertRun(jobB, b, "SCHEDULED");

        Resp r = delete("/api/v1/jobs/" + jobB, token(a, Role.ADMIN));
        assertThat(r.status()).isEqualTo(404);
        assertThat(r.code()).isEqualTo("JOB_NOT_FOUND");

        assertThat(jdbc.sql("SELECT enabled FROM job WHERE id = ?").param(jobB).query(Boolean.class).single())
                .isTrue();
        assertThat(jdbc.sql("SELECT state FROM job_run WHERE id = ?").param(run).query(String.class).single())
                .isEqualTo("SCHEDULED");
    }

    @Test
    void tenantIdInJobBodyIsIgnored() {
        String a = newTenant(1);
        String b = newTenant(1);
        String body = """
                {"kind":"ONE_OFF","handler":"PICKUP_ALERT","runAt":"%s","tenantId":"%s",
                 "payload":{"tripId":7},"maxRunTime":"PT5M"}""".formatted(Instant.now().plusSeconds(3600), b);

        Resp r = post("/api/v1/jobs", token(a, Role.ADMIN), body);

        // Unknown properties are ignored by Jackson: the job is created in the token's tenant.
        assertThat(r.status()).as(r.toString()).isEqualTo(201);
        UUID id = UUID.fromString(r.json().get("id").asText());
        assertThat(r.json().get("tenantId").asText()).isEqualTo(a);
        assertThat(jdbc.sql("SELECT tenant_id FROM job WHERE id = ?").param(id).query(String.class).single())
                .isEqualTo(a);
        assertThat(jdbc.sql("SELECT count(*) FROM job WHERE tenant_id = ?").param(b).query(Long.class).single())
                .isZero();
    }

    // ------------------------------------------------------------------ tenant writes

    @Test
    void crossTenantPauseResumeAndCapAre404AndChangeNothing() {
        String a = newTenant(1);
        String b = newTenant(3);
        String tokenA = token(a, Role.ADMIN);

        Resp pause = post("/api/v1/tenants/" + b + "/pause", tokenA, null);
        assertThat(pause.status()).isEqualTo(404);
        assertThat(pause.code()).isEqualTo("TENANT_NOT_FOUND");
        assertTenantUnchanged(b, false, 3);

        // resume of a paused tenant must not un-pause it either
        jdbc.sql("UPDATE tenant SET paused = true WHERE id = ?").param(b).update();
        Resp resume = post("/api/v1/tenants/" + b + "/resume", tokenA, null);
        assertThat(resume.status()).isEqualTo(404);
        assertThat(resume.code()).isEqualTo("TENANT_NOT_FOUND");
        assertTenantUnchanged(b, true, 3);
        jdbc.sql("UPDATE tenant SET paused = false WHERE id = ?").param(b).update();

        for (int cap : List.of(0, 10)) {
            Resp r = put("/api/v1/tenants/" + b + "/cap", tokenA, "{\"cap\":" + cap + "}");
            assertThat(r.status()).isEqualTo(404);
            assertThat(r.code()).isEqualTo("TENANT_NOT_FOUND");
            assertTenantUnchanged(b, false, 3);
        }
    }

    @Test
    void crossTenant404IsIdenticalToNonExistentTenant404() {
        String a = newTenant(1);
        String b = newTenant(1);
        String tokenA = token(a, Role.ADMIN);
        String ghost = uniqueTenantId("ghost");

        Resp foreign = post("/api/v1/tenants/" + b + "/pause", tokenA, null);
        Resp missing = post("/api/v1/tenants/" + ghost + "/pause", tokenA, null);
        assertThat(foreign.status()).isEqualTo(404).isEqualTo(missing.status());
        assertThat(fieldNames(foreign.json())).isEqualTo(fieldNames(missing.json()));
        assertThat(foreign.code()).isEqualTo(missing.code());
        assertThat(foreign.json().get("detail")).isEqualTo(missing.json().get("detail"));
    }

    // ------------------------------------------------------------------ dead letters

    @Test
    void adminNeverSeesAnotherTenantsDeadLetters() {
        String a = newTenant(1);
        String b = newTenant(1);
        long runA = deadLetteredRun(a);
        long runB = deadLetteredRun(b);
        String tokenA = token(a, Role.ADMIN);

        Resp named = get("/api/v1/dead-letters?tenant=" + b, tokenA);
        assertThat(named.status()).isEqualTo(404);
        assertThat(named.code()).isEqualTo("TENANT_NOT_FOUND");

        Resp own = get("/api/v1/dead-letters?size=200", tokenA);
        assertThat(own.status()).isEqualTo(200);
        List<Long> runIds = new ArrayList<>();
        for (JsonNode item : own.json().get("items")) {
            assertThat(item.get("tenantId").asText()).isEqualTo(a);
            runIds.add(item.get("runId").asLong());
        }
        assertThat(runIds).contains(runA).doesNotContain(runB);

        Resp ownNamed = get("/api/v1/dead-letters?tenant=" + a, tokenA);
        assertThat(ownNamed.status()).isEqualTo(200);
        assertThat(ownNamed.raw()).doesNotContain(b);

        // the operator sees B's row, which proves it exists
        Resp op = get("/api/v1/dead-letters?tenant=" + b, operatorToken());
        assertThat(op.status()).isEqualTo(200);
        assertThat(op.json().get("items").get(0).get("runId").asLong()).isEqualTo(runB);
    }

    // ------------------------------------------------------------------ helpers

    private long deadLetteredRun(String tenant) {
        UUID job = createOneOffJob(tenant);
        long run = insertRun(job, tenant, "SCHEDULED");
        deadLetter(run, tenant, "HANDLER_FAILED");
        return run;
    }

    private long runCount(UUID jobId) {
        return jdbc.sql("SELECT count(*) FROM job_run WHERE job_id = ?").param(jobId).query(Long.class).single();
    }

    private void assertTenantUnchanged(String tenant, boolean paused, int cap) {
        assertThat(jdbc.sql("SELECT paused FROM tenant WHERE id = ?").param(tenant).query(Boolean.class).single())
                .as("paused").isEqualTo(paused);
        assertThat(jdbc.sql("SELECT configured_cap FROM tenant WHERE id = ?").param(tenant)
                .query(Integer.class).single()).as("configured_cap").isEqualTo(cap);
        assertThat(jdbc.sql("SELECT count(*) FROM tenant_slot WHERE tenant_id = ?").param(tenant)
                .query(Long.class).single()).as("slot rows").isEqualTo(cap);
    }

    private static Set<String> fieldNames(JsonNode node) {
        Set<String> names = new TreeSet<>();
        for (Iterator<String> it = node.fieldNames(); it.hasNext(); ) {
            names.add(it.next());
        }
        return names;
    }
}
