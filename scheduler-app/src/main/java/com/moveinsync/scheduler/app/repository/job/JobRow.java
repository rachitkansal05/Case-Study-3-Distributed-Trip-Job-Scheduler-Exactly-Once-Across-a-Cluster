package com.moveinsync.scheduler.app.repository.job;

import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.JobKind;
import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * One {@code job} row.
 *
 * @param payloadJson raw JSON; may hold PII, so it is never logged and only shown to ADMIN
 */
public record JobRow(
        UUID id,
        String tenantId,
        JobKind kind,
        HandlerType handler,
        String cron,
        Instant runAt,
        String payloadJson,
        int priority,
        Duration maxRunTime,
        MisfirePolicyType misfire,
        Duration misfireThreshold,
        int maxAttempts,
        int partitionId,
        Instant nextFireAt,
        long version,
        boolean enabled) {

    @Override
    public String toString() {
        return "JobRow[id=" + id + ", tenantId=" + tenantId + ", kind=" + kind + ", handler=" + handler
                + ", version=" + version + ", enabled=" + enabled + ", payload=<redacted>]";
    }
}
