package com.moveinsync.scheduler.core.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.JobKind;
import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import com.moveinsync.scheduler.core.domain.TenantId;
import com.moveinsync.scheduler.core.time.CronSchedule;
import com.moveinsync.scheduler.core.time.InvalidCronException;
import com.moveinsync.scheduler.core.time.NextFireCalculator;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class JobDefinitionTest {

    private static final UUID ID = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final TenantId TENANT = TenantId.of("acme");
    private static final String PAYLOAD = "{\"employee\":\"secret-pii\"}";
    private static final Instant RUN_AT = Instant.parse("2026-11-01T05:30:00Z");
    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");

    private static OneOffJob oneOff(String payload, int priority, Duration maxRunTime, Duration threshold,
            int maxAttempts, long version) {
        return new OneOffJob(ID, TENANT, HandlerType.NO_SHOW_CANCEL, payload, priority, maxRunTime,
                MisfirePolicyType.SKIP, threshold, maxAttempts, version, true, RUN_AT);
    }

    private static OneOffJob validOneOff() {
        return oneOff(PAYLOAD, 5, Duration.ofMinutes(5), Duration.ofSeconds(60), 5, 0);
    }

    private static RecurringJob recurring(String cron) {
        return new RecurringJob(ID, TENANT, HandlerType.ROSTER_GEN, "[1,2]", 1, Duration.ofMinutes(10),
                MisfirePolicyType.COALESCE, Duration.ZERO, 1, 3, false, CronSchedule.parse(cron), NEW_YORK);
    }

    @Test
    void validOneOffJob() {
        OneOffJob job = validOneOff();
        assertThat(job.kind()).isEqualTo(JobKind.ONE_OFF);
        assertThat(job.id()).isEqualTo(ID);
        assertThat(job.tenantId()).isEqualTo(TENANT);
        assertThat(job.payloadJson()).isEqualTo(PAYLOAD);
        assertThat(job.runAt()).isEqualTo(RUN_AT);
        assertThat(job.toString()).doesNotContain("secret-pii");
    }

    @Test
    void validRecurringJob() {
        RecurringJob job = recurring("30 1 * * *");
        assertThat(job.kind()).isEqualTo(JobKind.RECURRING);
        assertThat(job.enabled()).isFalse();
        assertThat(job.version()).isEqualTo(3);
        assertThat(job.misfireThreshold()).isZero();
        assertThat(job.toString()).contains("30 1 * * *").doesNotContain("[1,2]");
    }

    @Test
    void nullFieldsRejected() {
        Duration d = Duration.ofMinutes(1);
        assertThatThrownBy(() -> new OneOffJob(null, TENANT, HandlerType.PICKUP_ALERT, PAYLOAD, 5, d,
                MisfirePolicyType.SKIP, d, 1, 0, true, RUN_AT)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OneOffJob(ID, null, HandlerType.PICKUP_ALERT, PAYLOAD, 5, d,
                MisfirePolicyType.SKIP, d, 1, 0, true, RUN_AT)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OneOffJob(ID, TENANT, null, PAYLOAD, 5, d,
                MisfirePolicyType.SKIP, d, 1, 0, true, RUN_AT)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OneOffJob(ID, TENANT, HandlerType.PICKUP_ALERT, null, 5, d,
                MisfirePolicyType.SKIP, d, 1, 0, true, RUN_AT)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OneOffJob(ID, TENANT, HandlerType.PICKUP_ALERT, PAYLOAD, 5, null,
                MisfirePolicyType.SKIP, d, 1, 0, true, RUN_AT)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OneOffJob(ID, TENANT, HandlerType.PICKUP_ALERT, PAYLOAD, 5, d,
                null, d, 1, 0, true, RUN_AT)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OneOffJob(ID, TENANT, HandlerType.PICKUP_ALERT, PAYLOAD, 5, d,
                MisfirePolicyType.SKIP, null, 1, 0, true, RUN_AT)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OneOffJob(ID, TENANT, HandlerType.PICKUP_ALERT, PAYLOAD, 5, d,
                MisfirePolicyType.SKIP, d, 1, 0, true, null))
                .isInstanceOf(NullPointerException.class).hasMessage("runAt");
        assertThatThrownBy(() -> new RecurringJob(ID, TENANT, HandlerType.ROSTER_GEN, PAYLOAD, 5, d,
                MisfirePolicyType.SKIP, d, 1, 0, true, null, NEW_YORK))
                .isInstanceOf(NullPointerException.class).hasMessage("cron");
        assertThatThrownBy(() -> new RecurringJob(ID, TENANT, HandlerType.ROSTER_GEN, PAYLOAD, 5, d,
                MisfirePolicyType.SKIP, d, 1, 0, true, CronSchedule.parse("* * * * *"), null))
                .isInstanceOf(NullPointerException.class).hasMessage("zone");
    }

    @Test
    void payloadValidatedWithoutEchoingIt() {
        Duration d = Duration.ofMinutes(1);
        assertThatThrownBy(() -> oneOff("   ", 5, d, d, 1, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> oneOff("", 5, d, d, 1, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> oneOff("secret-pii-value", 5, d, d, 1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining("secret-pii-value");
        assertThat(oneOff("  [ ]  ", 5, d, d, 1, 0).payloadJson()).isEqualTo("  [ ]  ");
    }

    @Test
    void priorityRange() {
        Duration d = Duration.ofMinutes(1);
        assertThat(oneOff(PAYLOAD, 1, d, d, 1, 0).priority()).isEqualTo(1);
        assertThat(oneOff(PAYLOAD, 9, d, d, 1, 0).priority()).isEqualTo(9);
        assertThatThrownBy(() -> oneOff(PAYLOAD, 0, d, d, 1, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> oneOff(PAYLOAD, 10, d, d, 1, 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void maxRunTimeMustBePositive() {
        Duration d = Duration.ofMinutes(1);
        assertThatThrownBy(() -> oneOff(PAYLOAD, 5, Duration.ZERO, d, 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> oneOff(PAYLOAD, 5, Duration.ofSeconds(-1), d, 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(oneOff(PAYLOAD, 5, Duration.ofMillis(1), d, 1, 0).maxRunTime()).isEqualTo(Duration.ofMillis(1));
    }

    @Test
    void misfireThresholdNonNegative() {
        Duration d = Duration.ofMinutes(1);
        assertThat(oneOff(PAYLOAD, 5, d, Duration.ZERO, 1, 0).misfireThreshold()).isZero();
        assertThatThrownBy(() -> oneOff(PAYLOAD, 5, d, Duration.ofMillis(-1), 1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void maxAttemptsRange() {
        Duration d = Duration.ofMinutes(1);
        assertThatThrownBy(() -> oneOff(PAYLOAD, 5, d, d, 0, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> oneOff(PAYLOAD, 5, d, d, 32_768, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThat(oneOff(PAYLOAD, 5, d, d, 32_767, 0).maxAttempts()).isEqualTo(32_767);
    }

    @Test
    void versionNonNegative() {
        Duration d = Duration.ofMinutes(1);
        assertThatThrownBy(() -> oneOff(PAYLOAD, 5, d, d, 1, -1)).isInstanceOf(IllegalArgumentException.class);
        assertThat(oneOff(PAYLOAD, 5, d, d, 1, Long.MAX_VALUE).version()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void recurringValidatesCommonFieldsToo() {
        assertThatThrownBy(() -> new RecurringJob(ID, TENANT, HandlerType.ROSTER_GEN, PAYLOAD, 0,
                Duration.ofMinutes(1), MisfirePolicyType.SKIP, Duration.ZERO, 1, 0, true,
                CronSchedule.parse("* * * * *"), NEW_YORK)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> recurring("61 * * * *")).isInstanceOf(InvalidCronException.class);
    }

    @Test
    void oneOffFirstFireAfter() {
        OneOffJob job = validOneOff();
        assertThat(job.firstFireAfter(RUN_AT.minusSeconds(1))).contains(RUN_AT);
        assertThat(job.firstFireAfter(RUN_AT)).isEmpty();
        assertThat(job.firstFireAfter(RUN_AT.plusSeconds(1))).isEmpty();
    }

    @Test
    void recurringOccurrencesUseWallClockRules() {
        RecurringJob job = recurring("30 1 * * *");
        assertThat(job.firstFireAfter(Instant.parse("2026-11-01T00:00:00Z")))
                .contains(Instant.parse("2026-11-01T05:30:00Z"));
        List<Instant> next = job.nextOccurrences(Instant.parse("2026-10-31T06:00:00Z"), 3).stream()
                .map(NextFireCalculator.FireTime::instant)
                .toList();
        assertThat(next).containsExactly(Instant.parse("2026-11-01T05:30:00Z"),
                Instant.parse("2026-11-02T06:30:00Z"), Instant.parse("2026-11-03T06:30:00Z"));
        assertThatThrownBy(() -> job.nextOccurrences(Instant.EPOCH, -1)).isInstanceOf(IllegalArgumentException.class);
        assertThat(recurring("0 0 31 2 *").firstFireAfter(Instant.EPOCH)).isEmpty();
    }

    @Test
    void polymorphicDispatchViaSwitch() {
        List<JobDefinition> jobs = List.of(validOneOff(), recurring("0 9 * * MON"));
        List<String> described = jobs.stream().map(JobDefinitionTest::describe).toList();
        assertThat(described).containsExactly("one-off at 2026-11-01T05:30:00Z", "cron 0 9 * * MON in America/New_York");
        for (JobDefinition job : jobs) {
            JobKind expected = switch (job) {
                case OneOffJob o -> JobKind.ONE_OFF;
                case RecurringJob r -> JobKind.RECURRING;
            };
            assertThat(job.kind()).isEqualTo(expected);
        }
        Instant after = Instant.parse("2026-10-30T00:00:00Z");
        assertThat(jobs.get(0).firstFireAfter(after)).contains(RUN_AT);
        assertThat(jobs.get(1).firstFireAfter(after)).contains(Instant.parse("2026-11-02T14:00:00Z"));
    }

    private static String describe(JobDefinition job) {
        return switch (job) {
            case OneOffJob o -> "one-off at " + o.runAt();
            case RecurringJob r -> "cron " + r.cron().expression() + " in " + r.zone();
        };
    }
}
