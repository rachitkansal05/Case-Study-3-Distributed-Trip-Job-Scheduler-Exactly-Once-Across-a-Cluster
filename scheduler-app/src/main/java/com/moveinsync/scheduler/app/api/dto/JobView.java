package com.moveinsync.scheduler.app.api.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.JobKind;
import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * A job definition. {@code payload} is present only for a tenant ADMIN; for EMPLOYEE and OPERATOR
 * it is omitted and {@code payloadRedacted} is true.
 */
public record JobView(
        UUID id,
        String tenantId,
        JobKind kind,
        HandlerType handler,
        String cron,
        Instant runAt,
        JsonNode payload,
        boolean payloadRedacted,
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
        return "JobView[id=" + id + ", kind=" + kind + ", handler=" + handler + ", payload=<redacted>]";
    }
}
