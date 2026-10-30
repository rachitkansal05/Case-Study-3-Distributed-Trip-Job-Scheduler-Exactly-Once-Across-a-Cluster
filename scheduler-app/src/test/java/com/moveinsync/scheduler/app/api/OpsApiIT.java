package com.moveinsync.scheduler.app.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.moveinsync.scheduler.app.api.support.ApiIntegrationTest;
import com.moveinsync.scheduler.app.api.support.FakeFireTimeService;
import com.moveinsync.scheduler.app.security.Role;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The operations API (docs/design/architecture.md section 13, R9): pause/resume only flip {@code tenant.paused},
 * manual triggers are deduplicated by {@code Idempotency-Key} into one {@code job_run}, delete
 * disables the job and cancels only unstarted runs, run history pages and shows error codes only,
 * dead-letter replay reuses the same run row and idempotency key, next fire times honour count
 * bounds and the tenant zone, and the cluster view lists all 128 partitions.
 */
class OpsApiIT extends ApiIntegrationTest {

    private static final String KEY = "Idempotency-Key";

    // ------------------------------------------------------------------ pause / resume

    @Test
    void pauseAndResumeFlipOnlyTheTenantFlag() {
        String tenant = newTenant(2);
        String admin = token(tenant, Role.ADMIN);
        UUID job = createOneOffJob(tenant);
        long run = insertRun(job, tenant, "SCHEDULED");

        Resp pause = post("/api/v1/tenants/" + tenant + "/pause", admin, null);
        assertThat(pause.status()).as(pause.toString()).isEqualTo(200);
        assertThat(pause.json().get("tenantId").asText()).isEqualTo(tenant);
        assertThat(pause.json().get("paused").asBoolean()).isTrue();
        assertThat(paused(tenant)).isTrue();
        assertThat(runState(run)).isEqualTo("SCHEDULED");

        Resp resume = post("/api/v1/tenants/" + tenant + "/resume", operatorToken(), null);
        assertThat(resume.status()).as(resume.toString()).isEqualTo(200);
        assertThat(resume.json().get("tenantId").asText()).isEqualTo(tenant);
        assertThat(resume.json().get("paused").asBoolean()).isFalse();
        assertThat(paused(tenant)).isFalse();
        assertThat(runState(run)).isEqualTo("SCHEDULED");
    }

    @Test
    void pauseOfUnknownTenantIs404() {
        Resp r = post("/api/v1/tenants/" + uniqueTenantId("missing") + "/pause", operatorToken(), null);
        assertThat(r.status()).isEqualTo(404);
        assertThat(r.code()).isEqualTo("TENANT_NOT_FOUND");
    }

    // ------------------------------------------------------------------ trigger

    @Test
    void triggerCopiesTheJobAndIsDeduplicatedByHeader() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        Resp created = post("/api/v1/jobs", admin, """
                {"kind":"ONE_OFF","handler":"ROSTER_GEN","runAt":"%s","payload":{"r":1},
                 "maxRunTime":"PT7M","priority":2}""".formatted(Instant.now().plus(2, ChronoUnit.HOURS)));
        assertThat(created.status()).as(created.toString()).isEqualTo(201);
        UUID job = UUID.fromString(created.json().get("id").asText());
        String key = "trig-" + UUID.randomUUID();

        Resp first = post("/api/v1/jobs/" + job + "/trigger", admin, null, KEY, key);
        assertThat(first.status()).as(first.toString()).isEqualTo(202);
        long runId = first.json().get("runId").asLong();
        assertThat(first.json().get("jobId").asText()).isEqualTo(job.toString());
        assertThat(first.json().get("state").asText()).isEqualTo("SCHEDULED");
        assertThat(first.json().get("duplicate").asBoolean()).isFalse();
        assertThat(Instant.parse(first.json().get("dueAt").asText()))
                .isBetween(Instant.now().minusSeconds(30), Instant.now().plusSeconds(5));

        Map<String, Object> row = jdbc.sql("""
                        SELECT r.idem_key, r.state, r.priority = j.priority AS same_priority,
                               r.max_run_time = j.max_run_time AS same_max_run_time,
                               r.job_version = j.version AS same_version,
                               r.partition_id = j.partition_id AS same_partition, r.priority AS priority,
                               extract(epoch FROM r.max_run_time) AS max_run_time_s,
                               abs(extract(epoch FROM (clock_timestamp() - r.due_at))) < 10 AS due_now
                          FROM job_run r JOIN job j ON j.id = r.job_id WHERE r.id = ?""")
                .param(runId).query().singleRow();
        assertThat(row.get("idem_key")).isEqualTo(job + ":manual:" + key);
        assertThat(row.get("state")).isEqualTo("SCHEDULED");
        assertThat(row.get("same_priority")).isEqualTo(true);
        assertThat(row.get("same_max_run_time")).isEqualTo(true);
        assertThat(row.get("same_version")).isEqualTo(true);
        assertThat(row.get("same_partition")).isEqualTo(true);
        assertThat(((Number) row.get("priority")).intValue()).isEqualTo(2);
        assertThat(((Number) row.get("max_run_time_s")).intValue()).isEqualTo(420);
        assertThat(row.get("due_now")).isEqualTo(true);

        Resp again = post("/api/v1/jobs/" + job + "/trigger", admin, null, KEY, key);
        assertThat(again.status()).as(again.toString()).isEqualTo(202);
        assertThat(again.json().get("runId").asLong()).isEqualTo(runId);
        assertThat(runCount(job)).isEqualTo(1);

        Resp other = post("/api/v1/jobs/" + job + "/trigger", admin, null, KEY, "trig-" + UUID.randomUUID());
        assertThat(other.status()).isEqualTo(202);
        assertThat(other.json().get("runId").asLong()).isNotEqualTo(runId);
        assertThat(other.json().get("duplicate").asBoolean()).isFalse();
        assertThat(runCount(job)).isEqualTo(2);
    }

    /**
     * The controller and {@code TriggerView} document that repeating a key returns the same run
     * with {@code duplicate = true}.
     */
    @Test
    void repeatedTriggerReportsDuplicateTrue() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        UUID job = createOneOffJob(tenant);
        String key = "dup-" + UUID.randomUUID();

        Resp first = post("/api/v1/jobs/" + job + "/trigger", admin, null, KEY, key);
        Resp again = post("/api/v1/jobs/" + job + "/trigger", admin, null, KEY, key);

        assertThat(first.json().get("duplicate").asBoolean()).isFalse();
        assertThat(again.json().get("runId").asLong()).isEqualTo(first.json().get("runId").asLong());
        assertThat(again.json().get("duplicate").asBoolean()).as(again.toString()).isTrue();
    }

    @Test
    void triggerDedupOutlivesTheApiIdempotencyRecord() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        UUID job = createOneOffJob(tenant);
        String key = "old-" + UUID.randomUUID();

        Resp first = post("/api/v1/jobs/" + job + "/trigger", admin, null, KEY, key);
        jdbc.sql("UPDATE api_idempotency SET created_at = clock_timestamp() - interval '25 hours' "
                + "WHERE tenant_id = ? AND idem_key = ?").params(tenant, key).update();

        Resp later = post("/api/v1/jobs/" + job + "/trigger", admin, null, KEY, key);
        assertThat(later.status()).isEqualTo(202);
        assertThat(later.header("Idempotent-Replayed")).isNull();
        assertThat(later.json().get("runId").asLong()).isEqualTo(first.json().get("runId").asLong());
        assertThat(later.json().get("duplicate").asBoolean()).isTrue();
        assertThat(runCount(job)).isEqualTo(1);
    }

    @Test
    void triggerWithoutIdempotencyKeyIs400() {
        String tenant = newTenant(1);
        UUID job = createOneOffJob(tenant);

        Resp r = post("/api/v1/jobs/" + job + "/trigger", token(tenant, Role.ADMIN), null);

        assertThat(r.status()).as(r.toString()).isEqualTo(400);
        assertThat(r.code()).isEqualTo("IDEMPOTENCY_KEY_REQUIRED");
        assertThat(r.json().get("errors").get(0).get("field").asText()).isEqualTo(KEY);
        assertThat(runCount(job)).isZero();
    }

    // ------------------------------------------------------------------ delete

    @Test
    void deleteCancelsUnstartedRunsKeepsRunningAndBlocksTriggers() {
        String tenant = newTenant(2);
        String admin = token(tenant, Role.ADMIN);
        UUID job = createOneOffJob(tenant);
        long scheduled = insertRun(job, tenant, "SCHEDULED");
        long retryWait = insertRun(job, tenant, "RETRY_WAIT");
        long pausedRun = insertRun(job, tenant, "PAUSED");
        long running = insertRun(job, tenant, "SCHEDULED");
        markRunning(running, 0);
        long succeeded = insertRun(job, tenant, "SUCCEEDED");

        Resp r = delete("/api/v1/jobs/" + job, admin);

        assertThat(r.status()).as(r.toString()).isEqualTo(200);
        assertThat(r.json().get("jobId").asText()).isEqualTo(job.toString());
        assertThat(r.json().get("enabled").asBoolean()).isFalse();
        assertThat(r.json().get("cancelledRuns").asInt()).isEqualTo(3);
        assertThat(jdbc.sql("SELECT enabled FROM job WHERE id = ?").param(job).query(Boolean.class).single()).isFalse();
        for (long id : List.of(scheduled, retryWait, pausedRun)) {
            assertThat(runState(id)).as("run " + id).isEqualTo("CANCELLED");
            assertThat(jdbc.sql("SELECT finished_at IS NOT NULL FROM job_run WHERE id = ?").param(id)
                    .query(Boolean.class).single()).isTrue();
        }
        assertThat(runState(running)).isEqualTo("RUNNING");
        assertThat(jdbc.sql("SELECT slot_no FROM job_run WHERE id = ?").param(running).query(Integer.class).single())
                .isZero();
        assertThat(runState(succeeded)).isEqualTo("SUCCEEDED");

        Resp trigger = post("/api/v1/jobs/" + job + "/trigger", admin, null, KEY, "after-" + UUID.randomUUID());
        assertThat(trigger.status()).as(trigger.toString()).isEqualTo(409);
        assertThat(trigger.code()).isEqualTo("JOB_DISABLED");
        assertThat(runCount(job)).isEqualTo(5);

        Resp next = get("/api/v1/jobs/" + job + "/next-fire-times", admin);
        assertThat(next.status()).isEqualTo(200);
        assertThat(next.json().get("fireTimes").isArray()).isTrue();
        assertThat(next.json().get("fireTimes")).isEmpty();

        // deleting again is idempotent and cancels nothing more
        Resp again = delete("/api/v1/jobs/" + job, admin);
        assertThat(again.status()).isEqualTo(200);
        assertThat(again.json().get("cancelledRuns").asInt()).isZero();
    }

    // ------------------------------------------------------------------ run history

    @Test
    void runHistoryPagesAndShowsLagAndErrorCodeOnly() {
        String tenant = newTenant(2);
        String admin = token(tenant, Role.ADMIN);
        UUID job = createOneOffJob(tenant);
        List<Long> runs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            runs.add(insertRun(job, tenant, "SCHEDULED"));
        }
        long started = runs.get(4); // newest: first on page 0
        markRunning(started, 1);
        long failed = runs.get(3);
        jdbc.sql("UPDATE job_run SET state = 'RETRY_WAIT', attempt = 1, "
                        + "last_error = 'SINK_TIMEOUT: phone +91 98450 12345 unreachable secret-detail' WHERE id = ?")
                .param(failed).update();

        Resp p0 = get("/api/v1/jobs/" + job + "/runs?page=0&size=2", admin);
        assertThat(p0.status()).as(p0.toString()).isEqualTo(200);
        assertThat(p0.json().get("page").asInt()).isZero();
        assertThat(p0.json().get("size").asInt()).isEqualTo(2);
        assertThat(p0.json().get("hasMore").asBoolean()).isTrue();
        JsonNode items = p0.json().get("items");
        assertThat(items).hasSize(2);
        assertThat(items.get(0).get("runId").asLong()).isEqualTo(started);
        assertThat(items.get(0).get("state").asText()).isEqualTo("RUNNING");
        assertThat(items.get(0).get("attempt").asInt()).isEqualTo(1);
        assertThat(items.get(0).hasNonNull("startedAt")).isTrue();
        assertThat(items.get(0).hasNonNull("lagMillis")).isTrue();
        assertThat(items.get(0).get("lagMillis").asLong()).isGreaterThanOrEqualTo(0);
        assertThat(items.get(0).get("manual").asBoolean()).isFalse();
        assertThat(items.get(1).get("runId").asLong()).isEqualTo(failed);
        assertThat(items.get(1).get("errorCode").asText()).isEqualTo("SINK_TIMEOUT");
        assertThat(items.get(1).hasNonNull("lagMillis")).isFalse();
        for (String field : List.of("runId", "state", "attempt", "leaseLostCount", "scheduledAt", "dueAt",
                "coalescedCount", "manual")) {
            assertThat(items.get(1).has(field)).as(field).isTrue();
        }
        assertThat(p0.raw()).doesNotContain("98450").doesNotContain("secret-detail").doesNotContain("unreachable");

        Resp p1 = get("/api/v1/jobs/" + job + "/runs?page=1&size=2", admin);
        assertThat(p1.json().get("items")).hasSize(2);
        assertThat(p1.json().get("hasMore").asBoolean()).isTrue();

        Resp p2 = get("/api/v1/jobs/" + job + "/runs?page=2&size=2", operatorToken());
        assertThat(p2.status()).isEqualTo(200);
        assertThat(p2.json().get("items")).hasSize(1);
        assertThat(p2.json().get("items").get(0).get("runId").asLong()).isEqualTo(runs.get(0));
        assertThat(p2.json().get("hasMore").asBoolean()).isFalse();

        Resp all = get("/api/v1/jobs/" + job + "/runs", admin);
        assertThat(all.json().get("size").asInt()).isEqualTo(50);
        assertThat(all.json().get("items")).hasSize(5);
        assertThat(all.json().get("hasMore").asBoolean()).isFalse();

        for (String size : List.of("0", "201")) {
            Resp bad = get("/api/v1/jobs/" + job + "/runs?size=" + size, admin);
            assertThat(bad.status()).as(size).isEqualTo(400);
            assertThat(bad.json().get("errors").get(0).get("field").asText()).isEqualTo("size");
        }
    }

    @Test
    void manualRunsAreFlaggedInHistory() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        UUID job = createOneOffJob(tenant);
        long manual = post("/api/v1/jobs/" + job + "/trigger", admin, null, KEY, "m-" + UUID.randomUUID())
                .json().get("runId").asLong();

        JsonNode item = get("/api/v1/jobs/" + job + "/runs", admin).json().get("items").get(0);
        assertThat(item.get("runId").asLong()).isEqualTo(manual);
        assertThat(item.get("manual").asBoolean()).isTrue();
        assertThat(item.get("state").asText()).isEqualTo("SCHEDULED");
    }

    // ------------------------------------------------------------------ dead letters

    @Test
    void replayRestoresTheSameRowWithItsIdemKeyAndOnlyOnce() {
        String tenant = newTenant(2);
        UUID job = createOneOffJob(tenant);
        long run = insertRun(job, tenant, "SCHEDULED");
        markRunning(run, 1);
        deadLetter(run, tenant, "HANDLER_FAILED");
        String idemKey = jdbc.sql("SELECT idem_key FROM job_run WHERE id = ?").param(run).query(String.class).single();
        long runsBefore = runCount(job);

        Resp r = post("/api/v1/dead-letters/" + run + "/replay", operatorToken(), null);

        assertThat(r.status()).as(r.toString()).isEqualTo(200);
        assertThat(r.json().get("runId").asLong()).isEqualTo(run);
        assertThat(r.json().get("jobId").asText()).isEqualTo(job.toString());
        assertThat(r.json().get("state").asText()).isEqualTo("SCHEDULED");
        assertThat(r.json().get("idemKey").asText()).isEqualTo(idemKey);
        assertThat(r.json().hasNonNull("replayedAt")).isTrue();
        assertThat(Instant.parse(r.json().get("dueAt").asText()))
                .isBetween(Instant.now().minusSeconds(30), Instant.now().plusSeconds(5));

        Map<String, Object> row = jdbc.sql("""
                        SELECT state, attempt, lease_lost_count, idem_key, slot_no, lease_until, owner_node, finished_at,
                               abs(extract(epoch FROM (clock_timestamp() - due_at))) < 10 AS due_now
                          FROM job_run WHERE id = ?""")
                .param(run).query().singleRow();
        assertThat(row.get("state")).isEqualTo("SCHEDULED");
        assertThat(((Number) row.get("attempt")).intValue()).isZero();
        assertThat(((Number) row.get("lease_lost_count")).intValue()).isZero();
        assertThat(row.get("idem_key")).isEqualTo(idemKey);
        assertThat(row.get("slot_no")).isNull();
        assertThat(row.get("lease_until")).isNull();
        assertThat(row.get("owner_node")).isNull();
        assertThat(row.get("finished_at")).isNull();
        assertThat(row.get("due_now")).isEqualTo(true);
        assertThat(runCount(job)).isEqualTo(runsBefore);
        assertThat(jdbc.sql("SELECT replayed_at IS NOT NULL FROM dead_letter WHERE run_id = ?").param(run)
                .query(Boolean.class).single()).isTrue();

        Resp second = post("/api/v1/dead-letters/" + run + "/replay", operatorToken(), null);
        assertThat(second.status()).as(second.toString()).isEqualTo(409);
        assertThat(second.code()).isEqualTo("DEAD_LETTER_ALREADY_REPLAYED");
    }

    @Test
    void deadLetterListIsFilteredByTenantForOperatorAndScopedForAdmin() {
        String a = newTenant(1);
        String b = newTenant(1);
        UUID jobA = createOneOffJob(a);
        UUID jobB = createOneOffJob(b);
        long a1 = insertRun(jobA, a, "SCHEDULED");
        long a2 = insertRun(jobA, a, "SCHEDULED");
        long b1 = insertRun(jobB, b, "SCHEDULED");
        deadLetter(a1, a, "HANDLER_FAILED");
        deadLetter(a2, a, "SINK_TIMEOUT");
        deadLetter(b1, b, "HANDLER_FAILED");

        Resp op = get("/api/v1/dead-letters?tenant=" + a, operatorToken());
        assertThat(op.status()).as(op.toString()).isEqualTo(200);
        assertThat(runIds(op)).containsExactly(a2, a1);
        assertThat(op.json().get("items")).allSatisfy(i -> assertThat(i.get("tenantId").asText()).isEqualTo(a));
        JsonNode first = op.json().get("items").get(0);
        assertThat(first.get("errorCode").asText()).isEqualTo("SINK_TIMEOUT");
        assertThat(first.get("attempts").asInt()).isEqualTo(5);
        assertThat(first.get("runState").asText()).isEqualTo("DEAD_LETTERED");
        assertThat(first.get("jobId").asText()).isEqualTo(jobA.toString());
        assertThat(op.raw()).doesNotContain("handler failed").doesNotContain("98450");

        Resp paged = get("/api/v1/dead-letters?tenant=" + a + "&size=1", operatorToken());
        assertThat(runIds(paged)).containsExactly(a2);
        assertThat(paged.json().get("hasMore").asBoolean()).isTrue();

        Resp opAll = get("/api/v1/dead-letters?size=200", operatorToken());
        assertThat(opAll.status()).isEqualTo(200);

        Resp admin = get("/api/v1/dead-letters", token(b, Role.ADMIN));
        assertThat(admin.status()).as(admin.toString()).isEqualTo(200);
        assertThat(runIds(admin)).containsExactly(b1);

        Resp adminOwn = get("/api/v1/dead-letters?tenant=" + b, token(b, Role.ADMIN));
        assertThat(runIds(adminOwn)).containsExactly(b1);

        Resp adminOther = get("/api/v1/dead-letters?tenant=" + a, token(b, Role.ADMIN));
        assertThat(adminOther.status()).isEqualTo(404);
        assertThat(adminOther.raw()).doesNotContain("SINK_TIMEOUT");
    }

    // ------------------------------------------------------------------ next fire times

    @Test
    void nextFireTimesHonourCountBoundsAndTenantZone() {
        String tenant = newTenant(1);
        String admin = token(tenant, Role.ADMIN);
        Resp created = post("/api/v1/jobs", admin, """
                {"kind":"RECURRING","handler":"ROSTER_GEN","cron":"%s","payload":{},"maxRunTime":"PT10M"}"""
                .formatted(FakeFireTimeService.HOURLY));
        assertThat(created.status()).as(created.toString()).isEqualTo(201);
        String job = created.json().get("id").asText();

        for (String count : List.of("0", "101", "-1")) {
            Resp bad = get("/api/v1/jobs/" + job + "/next-fire-times?count=" + count, admin);
            assertThat(bad.status()).as(count + " " + bad).isEqualTo(400);
            assertThat(bad.code()).isEqualTo("VALIDATION_FAILED");
            assertThat(bad.json().get("errors").get(0).get("field").asText()).isEqualTo("count");
        }

        Resp def = get("/api/v1/jobs/" + job + "/next-fire-times", admin);
        assertThat(def.status()).as(def.toString()).isEqualTo(200);
        assertThat(def.json().get("jobId").asText()).isEqualTo(job);
        assertThat(def.json().get("timeZone").asText()).isEqualTo("Asia/Kolkata");
        assertThat(def.json().get("fireTimes")).hasSize(10);

        Resp max = get("/api/v1/jobs/" + job + "/next-fire-times?count=100", token(tenant, Role.EMPLOYEE));
        assertThat(max.status()).isEqualTo(200);
        assertThat(max.json().get("fireTimes")).hasSize(100);

        Instant previous = null;
        for (JsonNode ft : def.json().get("fireTimes")) {
            Instant instant = Instant.parse(ft.get("instant").asText());
            assertThat(instant).isEqualTo(instant.truncatedTo(ChronoUnit.HOURS));
            assertThat(ft.get("offset").asText()).isEqualTo("+05:30");
            assertThat(LocalDateTime.parse(ft.get("localTime").asText()))
                    .isEqualTo(LocalDateTime.ofInstant(instant, ZoneId.of("Asia/Kolkata")));
            assertThat(LocalDateTime.parse(ft.get("localTime").asText()).getMinute()).isEqualTo(30);
            if (previous != null) {
                assertThat(instant).isEqualTo(previous.plus(1, ChronoUnit.HOURS));
            }
            previous = instant;
        }
    }

    // ------------------------------------------------------------------ cluster

    @Test
    void clusterViewListsAll128PartitionsAndLiveNodes() {
        String node = "ops-it-node-" + UUID.randomUUID();
        int partition = 77;
        Map<String, Object> original = jdbc.sql("SELECT owner_node, epoch, lease_until FROM partition_lease "
                + "WHERE partition_id = ?").param(partition).query().singleRow();
        try {
            jdbc.sql("INSERT INTO scheduler_node (node_id, heartbeat_at, started_at) "
                    + "VALUES (?, clock_timestamp(), clock_timestamp())").param(node).update();
            jdbc.sql("UPDATE partition_lease SET owner_node = ?, epoch = epoch + 1, "
                    + "lease_until = clock_timestamp() + interval '1 minute' WHERE partition_id = ?")
                    .params(node, partition).update();

            Resp r = get("/api/v1/cluster", operatorToken());

            assertThat(r.status()).as(r.toString()).isEqualTo(200);
            JsonNode partitions = r.json().get("partitions");
            assertThat(partitions).hasSize(128);
            assertThat(r.json().get("ownedPartitions").asInt() + r.json().get("unownedPartitions").asInt())
                    .isEqualTo(128);
            assertThat(r.json().get("ownedPartitions").asInt()).isGreaterThanOrEqualTo(1);
            assertThat(r.json().hasNonNull("observedAt")).isTrue();
            JsonNode p = partitions.get(partition);
            assertThat(p.get("partitionId").asInt()).isEqualTo(partition);
            assertThat(p.get("ownerNode").asText()).isEqualTo(node);
            assertThat(p.get("leaseValid").asBoolean()).isTrue();
            assertThat(p.get("epoch").asLong()).isEqualTo(((Number) original.get("epoch")).longValue() + 1);

            JsonNode mine = null;
            for (JsonNode n : r.json().get("liveNodes")) {
                if (node.equals(n.get("nodeId").asText())) {
                    mine = n;
                }
            }
            assertThat(mine).as("live node " + node).isNotNull();
            assertThat(mine.get("partitionsOwned").asInt()).isEqualTo(1);

            assertThat(get("/api/v1/cluster", token(newTenant(1), Role.ADMIN)).status()).isEqualTo(403);
        } finally {
            jdbc.sql("UPDATE partition_lease SET owner_node = NULL, lease_until = NULL, epoch = ? "
                    + "WHERE partition_id = ?").params(original.get("epoch"), partition).update();
            jdbc.sql("DELETE FROM scheduler_node WHERE node_id = ?").param(node).update();
        }
    }

    // ------------------------------------------------------------------ helpers

    private List<Long> runIds(Resp page) {
        List<Long> ids = new ArrayList<>();
        page.json().get("items").forEach(i -> ids.add(i.get("runId").asLong()));
        return ids;
    }

    private String runState(long run) {
        return jdbc.sql("SELECT state FROM job_run WHERE id = ?").param(run).query(String.class).single();
    }

    private long runCount(UUID job) {
        return jdbc.sql("SELECT count(*) FROM job_run WHERE job_id = ?").param(job).query(Long.class).single();
    }

    private boolean paused(String tenant) {
        return jdbc.sql("SELECT paused FROM tenant WHERE id = ?").param(tenant).query(Boolean.class).single();
    }
}
