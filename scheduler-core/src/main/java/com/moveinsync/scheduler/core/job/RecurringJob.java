package com.moveinsync.scheduler.core.job;

import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.JobKind;
import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import com.moveinsync.scheduler.core.domain.TenantId;
import com.moveinsync.scheduler.core.time.CronSchedule;
import com.moveinsync.scheduler.core.time.NextFireCalculator;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * A job that fires on a cron expression evaluated in the local wall time of {@code zone} (plan
 * section 08; DST rules are those of {@link NextFireCalculator}). Common field rules are on {@link
 * JobDefinition}; {@code cron} and {@code zone} must be non-null. {@link #toString()} omits the
 * payload, which may hold PII.
 *
 * @param id job id
 * @param tenantId owning tenant
 * @param handler business handler
 * @param payloadJson handler payload (JSON object or array text)
 * @param priority 1 (most urgent) to 9
 * @param maxRunTime longest a single attempt may run, &gt; 0
 * @param misfirePolicy misfire behaviour
 * @param misfireThreshold lateness that counts as a misfire, &gt;= 0
 * @param maxAttempts attempts before dead-lettering, &gt;= 1
 * @param version definition version, &gt;= 0
 * @param enabled false while paused
 * @param cron the schedule, in local wall time
 * @param zone the tenant zone the schedule is evaluated in
 */
public record RecurringJob(
        UUID id,
        TenantId tenantId,
        HandlerType handler,
        String payloadJson,
        int priority,
        Duration maxRunTime,
        MisfirePolicyType misfirePolicy,
        Duration misfireThreshold,
        int maxAttempts,
        long version,
        boolean enabled,
        CronSchedule cron,
        ZoneId zone) implements JobDefinition {

    private static final NextFireCalculator CALCULATOR = new NextFireCalculator();

    public RecurringJob {
        JobValidation.validateCommon(id, tenantId, handler, payloadJson, priority, maxRunTime,
                misfirePolicy, misfireThreshold, maxAttempts, version);
        Objects.requireNonNull(cron, "cron");
        Objects.requireNonNull(zone, "zone");
    }

    @Override
    public JobKind kind() {
        return JobKind.RECURRING;
    }

    /** The next cron occurrence strictly after {@code after}, or empty if it can never fire. */
    @Override
    public Optional<Instant> firstFireAfter(Instant after) {
        return CALCULATOR.nextAfter(cron, zone, after).map(NextFireCalculator.FireTime::instant);
    }

    /**
     * The next {@code n} occurrences strictly after {@code after}, strictly increasing.
     *
     * @param n 0 to {@link NextFireCalculator#MAX_COUNT}
     * @throws IllegalArgumentException if {@code n} is out of range
     */
    public List<NextFireCalculator.FireTime> nextOccurrences(Instant after, int n) {
        return CALCULATOR.next(cron, zone, after, n);
    }

    @Override
    public String toString() {
        return "RecurringJob[id=" + id + ", tenantId=" + tenantId + ", handler=" + handler
                + ", cron=" + cron + ", zone=" + zone + ", version=" + version
                + ", enabled=" + enabled + "]";
    }
}
