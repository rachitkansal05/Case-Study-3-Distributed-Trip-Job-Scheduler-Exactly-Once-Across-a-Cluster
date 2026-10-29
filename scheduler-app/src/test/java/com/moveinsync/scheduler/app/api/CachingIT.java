package com.moveinsync.scheduler.app.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.moveinsync.scheduler.app.api.support.ApiIntegrationTest;
import com.moveinsync.scheduler.app.api.support.FakeFireTimeService;
import com.moveinsync.scheduler.app.security.Role;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;

/**
 * Caching and its staleness bounds (PLAN.md section 14, R9): next fire times are cached per
 * {@code (jobId, version, count)}, tenant settings (time zone) for at most 5 s, and only those two
 * caches exist. The pause flag and the cap are never cached, so their changes are visible on the
 * very next read.
 */
class CachingIT extends ApiIntegrationTest {

    @Autowired
    private CacheManager cacheManager;

    @Test
    void onlyTheTwoNamedCachesExist() {
        assertThat(cacheManager.getCacheNames()).containsExactlyInAnyOrder("tenant-settings", "next-fire-times");
        assertThat(cacheManager.getCache("tenant-settings")).isNotNull();
        assertThat(cacheManager.getCache("next-fire-times")).isNotNull();
        assertThat(cacheManager.getCache("anything-else")).isNull();
        assertThat(cacheManager.getCache("tenant-cap")).isNull();
        assertThat(cacheManager.getCacheNames()).containsExactlyInAnyOrder("tenant-settings", "next-fire-times");
    }

    @Test
    void nextFireTimesAreServedFromCachePerCount() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        String job = createRecurringJob(tenant);
        String path = "/api/v1/jobs/" + job + "/next-fire-times";

        int start = fireTimes.calls();
        Resp first = get(path + "?count=5", admin);
        assertThat(first.status()).as(first.toString()).isEqualTo(200);
        assertThat(first.json().get("fireTimes")).hasSize(5);
        assertThat(fireTimes.calls()).isEqualTo(start + 1);

        Resp second = get(path + "?count=5", admin);
        assertThat(fireTimes.calls()).as("same (job, version, count) is a cache hit").isEqualTo(start + 1);
        assertThat(second.json()).isEqualTo(first.json());

        // the key does not depend on the caller
        Resp employee = get(path + "?count=5", token(tenant, Role.EMPLOYEE));
        assertThat(employee.status()).isEqualTo(200);
        assertThat(fireTimes.calls()).isEqualTo(start + 1);

        Resp other = get(path + "?count=6", admin);
        assertThat(other.json().get("fireTimes")).hasSize(6);
        assertThat(fireTimes.calls()).as("a different count is a new entry").isEqualTo(start + 2);
        get(path + "?count=6", admin);
        assertThat(fireTimes.calls()).isEqualTo(start + 2);

        // a different job is a different entry
        String job2 = createRecurringJob(tenant);
        int beforeJob2 = fireTimes.calls();
        get("/api/v1/jobs/" + job2 + "/next-fire-times?count=5", admin);
        assertThat(fireTimes.calls()).isEqualTo(beforeJob2 + 1);
    }

    @Test
    void oneOffAndDisabledJobsNeverAskTheCronEngine() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        UUID oneOff = createOneOffJob(tenant);
        String recurring = createRecurringJob(tenant);
        assertThat(delete("/api/v1/jobs/" + recurring, admin).status()).isEqualTo(200);

        int start = fireTimes.calls();
        assertThat(get("/api/v1/jobs/" + oneOff + "/next-fire-times", admin).json().get("fireTimes")).hasSize(1);
        assertThat(get("/api/v1/jobs/" + recurring + "/next-fire-times", admin).json().get("fireTimes")).isEmpty();
        assertThat(fireTimes.calls()).isEqualTo(start);
    }

    @Test
    void tenantZoneChangeShowsUpAfterTheSettingsTtlWhileCachedFireTimesStayWithin30s() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        String job = createRecurringJob(tenant);
        String path = "/api/v1/jobs/" + job + "/next-fire-times?count=3";

        Resp before = get(path, admin);
        assertThat(before.json().get("timeZone").asText()).isEqualTo("Asia/Kolkata");
        int calls = fireTimes.calls();

        jdbc.sql("UPDATE tenant SET time_zone = 'UTC' WHERE id = ?").param(tenant).update();

        // tenant settings were cached moments ago: still the old zone
        assertThat(get(path, admin).json().get("timeZone").asText()).isEqualTo("Asia/Kolkata");

        // within the 5 s tenant-settings TTL (plus slack) the new zone appears
        await().atMost(Duration.ofSeconds(8)).pollInterval(Duration.ofMillis(250))
                .until(() -> "UTC".equals(get(path, admin).json().get("timeZone").asText()));

        // the next-fire-times entry (30 s TTL, keyed by job/version/count) is still the cached one
        JsonNode after = get(path, admin).json();
        assertThat(after.get("fireTimes")).isEqualTo(before.json().get("fireTimes"));
        assertThat(after.get("fireTimes").get(0).get("offset").asText()).isEqualTo("+05:30");
        assertThat(fireTimes.calls()).isEqualTo(calls);
    }

    @Test
    void pauseAndResumeAreVisibleImmediately() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        // warm the tenant-settings cache for this tenant
        createRecurringJob(tenant);

        for (int i = 0; i < 3; i++) {
            Resp pause = post("/api/v1/tenants/" + tenant + "/pause", admin, null);
            assertThat(pause.json().get("paused").asBoolean()).isTrue();
            assertThat(paused(tenant)).isTrue();

            Resp resume = post("/api/v1/tenants/" + tenant + "/resume", admin, null);
            assertThat(resume.json().get("paused").asBoolean()).isFalse();
            assertThat(paused(tenant)).isFalse();
        }

        // a flag changed behind the API's back does not confuse the next write either
        jdbc.sql("UPDATE tenant SET paused = true WHERE id = ?").param(tenant).update();
        assertThat(post("/api/v1/tenants/" + tenant + "/resume", admin, null).status()).isEqualTo(200);
        assertThat(paused(tenant)).isFalse();
    }

    @Test
    void capChangesAreVisibleImmediately() {
        String tenant = newTenant(2);
        String operator = operatorToken();
        String path = "/api/v1/tenants/" + tenant + "/cap";

        assertThat(put(path, operator, "{\"cap\":3}").json().get("effectiveCap").asInt()).isEqualTo(3);
        assertThat(slotCount(tenant)).isEqualTo(3);
        assertThat(put(path, operator, "{\"cap\":5}").json().get("effectiveCap").asInt()).isEqualTo(5);
        assertThat(slotCount(tenant)).isEqualTo(5);
        assertThat(put(path, operator, "{\"cap\":1}").json().get("effectiveCap").asInt()).isEqualTo(1);
        assertThat(slotCount(tenant)).isEqualTo(1);

        // the effective cap is the slot rows, read fresh: a slot held behind the API's back counts at once
        UUID job = createOneOffJob(tenant);
        jdbc.sql("INSERT INTO tenant_slot (tenant_id, slot_no) VALUES (?, 7)").param(tenant).update();
        long run = insertRun(job, tenant, "SCHEDULED");
        markRunning(run, 7);
        Resp r = put(path, operator, "{\"cap\":1}");
        assertThat(r.json().get("effectiveCap").asInt()).isEqualTo(2);
        assertThat(r.json().get("draining").asBoolean()).isTrue();
    }

    // ------------------------------------------------------------------ helpers

    private String createRecurringJob(String tenant) {
        Resp r = post("/api/v1/jobs", token(tenant, Role.ADMIN), """
                {"kind":"RECURRING","handler":"ROSTER_GEN","cron":"%s","payload":{},"maxRunTime":"PT10M"}"""
                .formatted(FakeFireTimeService.HOURLY));
        assertThat(r.status()).as(r.toString()).isEqualTo(201);
        return r.json().get("id").asText();
    }

    private boolean paused(String tenant) {
        return jdbc.sql("SELECT paused FROM tenant WHERE id = ?").param(tenant).query(Boolean.class).single();
    }

    private long slotCount(String tenant) {
        return jdbc.sql("SELECT count(*) FROM tenant_slot WHERE tenant_id = ?").param(tenant).query(Long.class).single();
    }
}
