package com.moveinsync.scheduler.app.api.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.JobKind;
import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.time.Duration;
import java.time.Instant;

/**
 * Body of {@code POST /api/v1/jobs} (ADMIN). The tenant is taken from the token, never from here.
 * Kind rules (ONE_OFF needs {@code runAt} and no {@code cron}; RECURRING the reverse), duration
 * bounds, payload shape and cron are checked by the job service.
 *
 * @param cron             RECURRING only: evaluated in the tenant's time zone
 * @param runAt            ONE_OFF only: UTC instant
 * @param payload          JSON object handed to the handler; may hold PII
 * @param priority         1 (most urgent) .. 9; default 5
 * @param maxRunTime       ISO-8601 duration, e.g. {@code PT5M}; 1 s .. 24 h
 * @param misfire          default {@code FIRE_NOW}
 * @param misfireThreshold default {@code scheduler.misfire.default-threshold}
 * @param maxAttempts      default {@code scheduler.retry.max-attempts}
 */
public record CreateJobRequest(
        @NotNull JobKind kind,
        @NotNull HandlerType handler,
        @Size(min = 1, max = 256) String cron,
        Instant runAt,
        @NotNull JsonNode payload,
        @Min(1) @Max(9) Integer priority,
        @NotNull Duration maxRunTime,
        MisfirePolicyType misfire,
        Duration misfireThreshold,
        @Min(1) @Max(100) Integer maxAttempts) {

    @Override
    public String toString() {
        return "CreateJobRequest[kind=" + kind + ", handler=" + handler + ", payload=<redacted>]";
    }
}
