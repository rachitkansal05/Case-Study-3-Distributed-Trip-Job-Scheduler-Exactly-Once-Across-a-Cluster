package com.moveinsync.scheduler.core.job;

import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import com.moveinsync.scheduler.core.domain.TenantId;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/** Shared field checks for {@link JobDefinition} records (rules are documented there). */
final class JobValidation {

    private JobValidation() {
    }

    static void validateCommon(UUID id, TenantId tenantId, HandlerType handler, String payloadJson,
            int priority, Duration maxRunTime, MisfirePolicyType misfirePolicy, Duration misfireThreshold,
            int maxAttempts, long version) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(payloadJson, "payloadJson");
        Objects.requireNonNull(maxRunTime, "maxRunTime");
        Objects.requireNonNull(misfirePolicy, "misfirePolicy");
        Objects.requireNonNull(misfireThreshold, "misfireThreshold");
        String trimmed = payloadJson.strip();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("payloadJson must not be blank");
        }
        char first = trimmed.charAt(0);
        if (first != '{' && first != '[') {
            throw new IllegalArgumentException("payloadJson must be a JSON object or array");
        }
        if (priority < JobDefinition.MIN_PRIORITY || priority > JobDefinition.MAX_PRIORITY) {
            throw new IllegalArgumentException("priority must be " + JobDefinition.MIN_PRIORITY + "-"
                    + JobDefinition.MAX_PRIORITY + ": " + priority);
        }
        if (maxRunTime.isZero() || maxRunTime.isNegative()) {
            throw new IllegalArgumentException("maxRunTime must be > 0");
        }
        if (misfireThreshold.isNegative()) {
            throw new IllegalArgumentException("misfireThreshold must be >= 0");
        }
        if (maxAttempts < 1 || maxAttempts > JobDefinition.MAX_ATTEMPTS_LIMIT) {
            throw new IllegalArgumentException(
                    "maxAttempts must be 1-" + JobDefinition.MAX_ATTEMPTS_LIMIT + ": " + maxAttempts);
        }
        if (version < 0) {
            throw new IllegalArgumentException("version must be >= 0: " + version);
        }
    }
}
