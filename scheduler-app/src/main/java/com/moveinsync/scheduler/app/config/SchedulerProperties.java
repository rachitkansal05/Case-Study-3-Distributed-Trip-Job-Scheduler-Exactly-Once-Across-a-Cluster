package com.moveinsync.scheduler.app.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * All timing and sizing knobs of a scheduler node ({@code scheduler.*} in {@code application.yml}).
 * Defaults and their reasoning are in docs/design/architecture.md sections 06, 07, 09 and 10. Validated at startup, so
 * an inconsistent configuration (e.g. renewal slower than the lease) stops the node from starting.
 */
@Validated
@ConfigurationProperties(prefix = "scheduler")
public record SchedulerProperties(
        @NotBlank @Size(max = 64) String nodeId,
        @Min(1) @Max(1024) int partitions,
        @Valid @NotNull Lease runLease,
        @Valid @NotNull PartitionLease partitionLease,
        @Valid @NotNull Membership membership,
        @Valid @NotNull Claim claim,
        @Valid @NotNull OrphanSweep orphanSweep,
        @Valid @NotNull Workers workers,
        @NotNull Duration selfFenceMargin,
        @Valid @NotNull Tenant tenant,
        @Valid @NotNull Misfire misfire,
        @Valid @NotNull Retry retry,
        @Valid @NotNull Sink sink,
        @Valid @NotNull Security security) {

    /** Run lease: 10 s, renewed every 3 s. */
    public record Lease(@NotNull Duration duration, @NotNull Duration renewInterval) {
        @AssertTrue(message = "run-lease.renew-interval must be shorter than run-lease.duration")
        public boolean isRenewBeforeExpiry() {
            return duration == null || renewInterval == null || renewInterval.compareTo(duration) < 0;
        }
    }

    /** Partition lease: 6 s, renewed every 2 s, partition loop every 1 s. */
    public record PartitionLease(@NotNull Duration duration, @NotNull Duration renewInterval,
                                 @NotNull Duration loopInterval) {
        @AssertTrue(message = "partition-lease.renew-interval must be shorter than partition-lease.duration")
        public boolean isRenewBeforeExpiry() {
            return duration == null || renewInterval == null || renewInterval.compareTo(duration) < 0;
        }
    }

    /** Heartbeat every 2 s; a node is dead when its heartbeat is 6 s old (DB time). */
    public record Membership(@NotNull Duration heartbeatInterval, @NotNull Duration deadAfter) {
        @AssertTrue(message = "membership.heartbeat-interval must be shorter than membership.dead-after")
        public boolean isHeartbeatBeforeDeath() {
            return heartbeatInterval == null || deadAfter == null || heartbeatInterval.compareTo(deadAfter) < 0;
        }
    }

    /** Claim poll every 200 ms. */
    public record Claim(@NotNull Duration pollInterval) {
    }

    /** Orphan sweep every 500 ms, in any partition (takeover bound 10 s + 0.5 s + 0.2 s = 10.7 s). */
    public record OrphanSweep(@NotNull Duration interval) {
    }

    /** 64 workers per node, one reserved for recovered (orphan) runs. */
    public record Workers(@Min(1) int count, @Min(0) int reservedForRecovery) {
        @AssertTrue(message = "workers.reserved-for-recovery must be less than workers.count")
        public boolean isReservedLessThanCount() {
            return reservedForRecovery < count;
        }
    }

    /** Default concurrency cap for new tenants (slot rows). */
    public record Tenant(@Min(0) int defaultCap) {
    }

    /** Default misfire threshold for new jobs. */
    public record Misfire(@NotNull Duration defaultThreshold) {
    }

    /** Retries: 5 attempts, 20 lease-lost attempts, full-jitter backoff base 2 s, cap 5 min. */
    public record Retry(@Min(1) int maxAttempts, @Min(1) int leaseLostLimit,
                        @NotNull Duration backoffBase, @NotNull Duration backoffCap) {
    }

    /** Where the notification sink lives. */
    public record Sink(@NotBlank String baseUrl) {
    }

    /** JWT HMAC key (HS256). The default is a dev-only value; override with an env variable. */
    public record Security(@NotBlank @Size(min = 32) String jwtHmacSecret) {
        @Override
        public String toString() {
            return "Security[jwtHmacSecret=<redacted>]";
        }
    }

    /** Self-fence must leave room inside the run lease. */
    @AssertTrue(message = "self-fence-margin must be shorter than run-lease.duration")
    public boolean isSelfFenceInsideLease() {
        return selfFenceMargin == null || runLease == null || runLease.duration() == null
                || selfFenceMargin.compareTo(runLease.duration()) < 0;
    }
}
