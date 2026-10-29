package com.moveinsync.scheduler.core.job;

import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.JobKind;
import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import com.moveinsync.scheduler.core.domain.TenantId;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * A job that fires once, at {@code runAt} ({@code job.run_at}). Common field rules are on {@link
 * JobDefinition}; {@code runAt} must be non-null and may lie in the past (the run is then subject
 * to the misfire policy). {@link #toString()} omits the payload, which may hold PII.
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
 * @param runAt the single fire instant
 */
public record OneOffJob(
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
        Instant runAt) implements JobDefinition {

    public OneOffJob {
        JobValidation.validateCommon(id, tenantId, handler, payloadJson, priority, maxRunTime,
                misfirePolicy, misfireThreshold, maxAttempts, version);
        Objects.requireNonNull(runAt, "runAt");
    }

    @Override
    public JobKind kind() {
        return JobKind.ONE_OFF;
    }

    /** {@code runAt} if it is strictly after {@code after}, otherwise empty. */
    @Override
    public Optional<Instant> firstFireAfter(Instant after) {
        Objects.requireNonNull(after, "after");
        return runAt.isAfter(after) ? Optional.of(runAt) : Optional.empty();
    }

    @Override
    public String toString() {
        return "OneOffJob[id=" + id + ", tenantId=" + tenantId + ", handler=" + handler
                + ", runAt=" + runAt + ", version=" + version + ", enabled=" + enabled + "]";
    }
}
