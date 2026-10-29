package com.moveinsync.scheduler.app.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.moveinsync.scheduler.app.api.support.ApiIntegrationTest;
import com.moveinsync.scheduler.app.api.support.FakeFireTimeService;
import com.moveinsync.scheduler.app.security.Role;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Job definition rules of {@code POST /api/v1/jobs} (PLAN.md section 13, R1/R14): every invalid
 * body is a 400 naming the offending field without echoing its value and creates nothing; valid
 * one-off and recurring jobs are stored with the documented defaults, version 1, the partition
 * {@code floorMod(id.hashCode(), 128)} and the first fire time as {@code next_fire_at}.
 */
class JobValidationIT extends ApiIntegrationTest {

    private static final Instant RUN_AT = Instant.now().plus(3, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS);

    /** One invalid-body case: a mutation of a valid base body and the expected error. */
    record Case(String name, boolean recurring, Consumer<ObjectNode> mutation, String code, String field,
                String echo) {
        @Override
        public String toString() {
            return name;
        }
    }

    static Stream<Arguments> invalidBodies() {
        String big = "x".repeat(64 * 1024);
        List<Case> cases = List.of(
                // kind rules
                new Case("one-off without runAt", false, b -> b.remove("runAt"), "VALIDATION_FAILED", "runAt", null),
                new Case("one-off with cron", false, b -> b.put("cron", FakeFireTimeService.HOURLY),
                        "VALIDATION_FAILED", "cron", null),
                new Case("recurring without cron", true, b -> b.remove("cron"), "VALIDATION_FAILED", "cron", null),
                new Case("recurring with blank cron", true, b -> b.put("cron", "   "), "VALIDATION_FAILED", "cron",
                        null),
                new Case("recurring with empty cron", true, b -> b.put("cron", ""), "VALIDATION_FAILED", "cron", null),
                new Case("recurring with runAt", true, b -> b.put("runAt", RUN_AT.toString()), "VALIDATION_FAILED",
                        "runAt", null),
                // cron engine
                new Case("invalid cron", true, b -> b.put("cron", "0 invalid-cron-zz * * *"), "INVALID_CRON", "cron",
                        "invalid-cron-zz"),
                new Case("cron with too few fields", true, b -> b.put("cron", "*/7 9"), "INVALID_CRON", "cron",
                        "*/7 9"),
                new Case("cron with too many fields", true, b -> b.put("cron", "0 0 1 1 1 1 2099"), "INVALID_CRON",
                        "cron", "1 1 2099"),
                new Case("cron that never fires", true, b -> b.put("cron", FakeFireTimeService.NEVER), "INVALID_CRON",
                        "cron", FakeFireTimeService.NEVER),
                // enums and required fields
                new Case("missing kind", false, b -> b.remove("kind"), "VALIDATION_FAILED", "kind", null),
                new Case("unknown kind", false, b -> b.put("kind", "SOMETIMES_ZZ"), "VALIDATION_FAILED", "kind",
                        "SOMETIMES_ZZ"),
                new Case("missing handler", false, b -> b.remove("handler"), "VALIDATION_FAILED", "handler", null),
                new Case("unknown handler", false, b -> b.put("handler", "SEND_FAX_ZZ"), "VALIDATION_FAILED",
                        "handler", "SEND_FAX_ZZ"),
                new Case("unknown misfire", false, b -> b.put("misfire", "PANIC_ZZ"), "VALIDATION_FAILED", "misfire",
                        "PANIC_ZZ"),
                // priority
                new Case("priority 0", false, b -> b.put("priority", 0), "VALIDATION_FAILED", "priority", null),
                new Case("priority 10", false, b -> b.put("priority", 10), "VALIDATION_FAILED", "priority", null),
                // maxRunTime
                new Case("missing maxRunTime", false, b -> b.remove("maxRunTime"), "VALIDATION_FAILED", "maxRunTime",
                        null),
                new Case("maxRunTime PT0S", false, b -> b.put("maxRunTime", "PT0S"), "VALIDATION_FAILED",
                        "maxRunTime", null),
                new Case("maxRunTime PT0.5S", false, b -> b.put("maxRunTime", "PT0.5S"), "VALIDATION_FAILED",
                        "maxRunTime", null),
                new Case("maxRunTime PT25H", false, b -> b.put("maxRunTime", "PT25H"), "VALIDATION_FAILED",
                        "maxRunTime", "PT25H"),
                new Case("negative maxRunTime", false, b -> b.put("maxRunTime", "-PT5M"), "VALIDATION_FAILED",
                        "maxRunTime", "-PT5M"),
                new Case("malformed maxRunTime", false, b -> b.put("maxRunTime", "five-minutes-zz"),
                        "VALIDATION_FAILED", "maxRunTime", "five-minutes-zz"),
                // misfireThreshold
                new Case("negative misfireThreshold", false, b -> b.put("misfireThreshold", "-PT1S"),
                        "VALIDATION_FAILED", "misfireThreshold", "-PT1S"),
                new Case("misfireThreshold P8D", false, b -> b.put("misfireThreshold", "P8D"), "VALIDATION_FAILED",
                        "misfireThreshold", "P8D"),
                // maxAttempts
                new Case("maxAttempts 0", false, b -> b.put("maxAttempts", 0), "VALIDATION_FAILED", "maxAttempts",
                        null),
                new Case("maxAttempts 101", false, b -> b.put("maxAttempts", 101), "VALIDATION_FAILED", "maxAttempts",
                        null),
                // payload
                new Case("missing payload", false, b -> b.remove("payload"), "VALIDATION_FAILED", "payload", null),
                new Case("payload array", false, b -> b.putArray("payload").add("arr-elem-zz"), "VALIDATION_FAILED",
                        "payload", "arr-elem-zz"),
                new Case("payload string", false, b -> b.put("payload", "phone +91 98450 12345"),
                        "VALIDATION_FAILED", "payload", "98450"),
                new Case("payload number", false, b -> b.put("payload", 42), "VALIDATION_FAILED", "payload", null),
                new Case("payload over 64 KiB", false, b -> b.putObject("payload").put("blob", big),
                        "VALIDATION_FAILED", "payload", "xxxxxxxxxx"),
                // runAt format
                new Case("malformed runAt", false, b -> b.put("runAt", "tomorrow-zz"), "VALIDATION_FAILED", "runAt",
                        "tomorrow-zz"));
        return cases.stream().map(Arguments::of);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidBodies")
    void invalidBodyIs400NamingTheField(Case c) {
        String tenant = newTenant(1);
        ObjectNode body = c.recurring() ? recurringBody() : oneOffBodyNode();
        c.mutation().accept(body);

        Resp r = post("/api/v1/jobs", token(tenant, Role.ADMIN), body.toString());

        assertThat(r.status()).as(r.toString().length() > 500 ? r.toString().substring(0, 500) : r.toString())
                .isEqualTo(400);
        assertThat(r.code()).as(r.toString()).isEqualTo(c.code());
        List<String> fields = new ArrayList<>();
        r.json().get("errors").forEach(e -> fields.add(e.get("field").asText()));
        assertThat(fields).as(r.toString()).contains(c.field());
        if (c.echo() != null) {
            assertThat(r.raw()).doesNotContain(c.echo());
        }
        assertThat(r.raw()).doesNotContain("98450").doesNotContain("Exception").doesNotContain("at com.");
        assertThat(jdbc.sql("SELECT count(*) FROM job WHERE tenant_id = ?").param(tenant).query(Long.class).single())
                .isZero();
    }

    @Test
    void misfireThresholdBoundsAreInclusive() {
        String tenant = newTenant(1);
        for (String threshold : List.of("PT0S", "P7D")) {
            ObjectNode body = oneOffBodyNode();
            body.put("misfireThreshold", threshold);
            Resp r = post("/api/v1/jobs", token(tenant, Role.ADMIN), body.toString());
            assertThat(r.status()).as(threshold + " " + r).isEqualTo(201);
        }
        for (String maxRunTime : List.of("PT1S", "PT24H")) {
            ObjectNode body = oneOffBodyNode();
            body.put("maxRunTime", maxRunTime);
            assertThat(post("/api/v1/jobs", token(tenant, Role.ADMIN), body.toString()).status())
                    .as(maxRunTime).isEqualTo(201);
        }
    }

    @Test
    void payloadOfExactly64KiBIsAccepted() {
        String tenant = newTenant(1);
        String prefix = "{\"b\":\"";
        String suffix = "\"}";
        String blob = "y".repeat(64 * 1024 - prefix.length() - suffix.length());
        ObjectNode body = oneOffBodyNode();
        body.putObject("payload").put("b", blob);
        Resp r = post("/api/v1/jobs", token(tenant, Role.ADMIN), body.toString());
        assertThat(r.status()).as(r.status() + "").isEqualTo(201);
    }

    @Test
    void validOneOffJobGetsDefaultsAndComputedFields() {
        String tenant = newTenant(1);

        Resp r = post("/api/v1/jobs", token(tenant, Role.ADMIN), oneOffBodyNode().toString());

        assertThat(r.status()).as(r.toString()).isEqualTo(201);
        JsonNode j = r.json();
        UUID id = UUID.fromString(j.get("id").asText());
        assertThat(r.header("Location")).endsWith("/api/v1/jobs/" + id);
        assertThat(j.get("tenantId").asText()).isEqualTo(tenant);
        assertThat(j.get("kind").asText()).isEqualTo("ONE_OFF");
        assertThat(j.get("handler").asText()).isEqualTo("PICKUP_ALERT");
        assertThat(j.has("cron")).isFalse();
        assertThat(Instant.parse(j.get("runAt").asText())).isEqualTo(RUN_AT);
        assertThat(j.get("payload").get("tripId").asInt()).isEqualTo(42);
        assertThat(j.get("payloadRedacted").asBoolean()).isFalse();
        assertThat(j.get("priority").asInt()).isEqualTo(5);
        assertThat(j.get("maxRunTime").asText()).isEqualTo("PT5M");
        assertThat(j.get("misfire").asText()).isEqualTo("FIRE_NOW");
        assertThat(j.get("misfireThreshold").asText()).isEqualTo("PT1M");
        assertThat(j.get("maxAttempts").asInt()).isEqualTo(5);
        assertThat(j.get("partitionId").asInt()).isEqualTo(Math.floorMod(id.hashCode(), 128)).isBetween(0, 127);
        assertThat(Instant.parse(j.get("nextFireAt").asText())).isEqualTo(RUN_AT);
        assertThat(j.get("version").asLong()).isEqualTo(1);
        assertThat(j.get("enabled").asBoolean()).isTrue();

        Map<String, Object> row = jobRow(id);
        assertThat(row.get("tenant_id")).isEqualTo(tenant);
        assertThat(row.get("kind")).isEqualTo("ONE_OFF");
        assertThat(row.get("handler")).isEqualTo("PICKUP_ALERT");
        assertThat(row.get("cron")).isNull();
        assertThat(row.get("run_at_ok")).isEqualTo(true);
        assertThat(row.get("next_fire_ok")).isEqualTo(true);
        assertThat(((Number) row.get("priority")).intValue()).isEqualTo(5);
        assertThat(((Number) row.get("max_run_time_s")).intValue()).isEqualTo(300);
        assertThat(row.get("misfire")).isEqualTo("FIRE_NOW");
        assertThat(((Number) row.get("misfire_threshold_s")).intValue()).isEqualTo(60);
        assertThat(((Number) row.get("max_attempts")).intValue()).isEqualTo(5);
        assertThat(((Number) row.get("partition_id")).intValue()).isEqualTo(Math.floorMod(id.hashCode(), 128));
        assertThat(((Number) row.get("version")).longValue()).isEqualTo(1);
        assertThat(row.get("enabled")).isEqualTo(true);
        assertThat(row.get("trip_id")).isEqualTo("42");
        // the API stores definitions only; runs are materialised by the Engine
        assertThat(jdbc.sql("SELECT count(*) FROM job_run WHERE job_id = ?").param(id).query(Long.class).single())
                .isZero();
    }

    @Test
    void validRecurringJobWithExplicitValuesStartsAtFirstFireTime() {
        String tenant = newTenant(1);
        ObjectNode body = recurringBody();
        body.put("priority", 1).put("misfire", "COALESCE").put("misfireThreshold", "PT10M").put("maxAttempts", 3);
        Instant before = Instant.now();

        Resp r = post("/api/v1/jobs", token(tenant, Role.ADMIN), body.toString());

        Instant after = Instant.now();
        assertThat(r.status()).as(r.toString()).isEqualTo(201);
        JsonNode j = r.json();
        UUID id = UUID.fromString(j.get("id").asText());
        assertThat(j.get("kind").asText()).isEqualTo("RECURRING");
        assertThat(j.get("cron").asText()).isEqualTo(FakeFireTimeService.HOURLY);
        assertThat(j.has("runAt")).isFalse();
        assertThat(j.get("priority").asInt()).isEqualTo(1);
        assertThat(j.get("misfire").asText()).isEqualTo("COALESCE");
        assertThat(j.get("misfireThreshold").asText()).isEqualTo("PT10M");
        assertThat(j.get("maxAttempts").asInt()).isEqualTo(3);
        assertThat(j.get("maxRunTime").asText()).isEqualTo("PT10M");
        assertThat(j.get("version").asLong()).isEqualTo(1);
        assertThat(j.get("partitionId").asInt()).isEqualTo(Math.floorMod(id.hashCode(), 128));
        Instant next = Instant.parse(j.get("nextFireAt").asText());
        // the fake fires at the top of every hour: the first one strictly after "now"
        assertThat(next).isIn(topOfNextHour(before), topOfNextHour(after));

        Map<String, Object> row = jobRow(id);
        assertThat(row.get("kind")).isEqualTo("RECURRING");
        assertThat(row.get("cron")).isEqualTo(FakeFireTimeService.HOURLY);
        assertThat(row.get("run_at_ok")).isNull();
        assertThat(((Number) row.get("priority")).intValue()).isEqualTo(1);
        assertThat(row.get("misfire")).isEqualTo("COALESCE");
        assertThat(((Number) row.get("misfire_threshold_s")).intValue()).isEqualTo(600);
        assertThat(((Number) row.get("max_attempts")).intValue()).isEqualTo(3);
        assertThat(((Number) row.get("version")).longValue()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT next_fire_at FROM job WHERE id = ?").param(id)
                .query(java.sql.Timestamp.class).single().toInstant()).isEqualTo(next);
    }

    // ------------------------------------------------------------------ helpers

    private ObjectNode oneOffBodyNode() {
        ObjectNode b = mapper.createObjectNode();
        b.put("kind", "ONE_OFF").put("handler", "PICKUP_ALERT").put("runAt", RUN_AT.toString());
        b.putObject("payload").put("tripId", 42);
        b.put("maxRunTime", "PT5M");
        return b;
    }

    private ObjectNode recurringBody() {
        ObjectNode b = mapper.createObjectNode();
        b.put("kind", "RECURRING").put("handler", "ROSTER_GEN").put("cron", FakeFireTimeService.HOURLY);
        b.putObject("payload").put("shift", "night");
        b.put("maxRunTime", "PT10M");
        return b;
    }

    private static Instant topOfNextHour(Instant t) {
        return t.truncatedTo(ChronoUnit.HOURS).plus(1, ChronoUnit.HOURS);
    }

    private Map<String, Object> jobRow(UUID id) {
        return jdbc.sql("""
                        SELECT tenant_id, kind, handler, cron, priority, misfire, max_attempts, partition_id, version,
                               enabled, payload ->> 'tripId' AS trip_id,
                               CASE WHEN run_at IS NULL THEN NULL ELSE run_at = :runAt END AS run_at_ok,
                               next_fire_at = coalesce(run_at, next_fire_at) AS next_fire_ok,
                               extract(epoch FROM max_run_time) AS max_run_time_s,
                               extract(epoch FROM misfire_threshold) AS misfire_threshold_s
                          FROM job WHERE id = :id""")
                .param("runAt", java.sql.Timestamp.from(RUN_AT))
                .param("id", id)
                .query().singleRow();
    }
}
