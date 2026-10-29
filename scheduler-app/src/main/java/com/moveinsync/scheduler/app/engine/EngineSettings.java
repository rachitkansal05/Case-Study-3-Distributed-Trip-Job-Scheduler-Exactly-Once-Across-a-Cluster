package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.app.config.SchedulerProperties;
import java.time.Duration;
import java.util.Objects;

/**
 * Every value one {@link SchedulerNode} needs, as a plain immutable value. Built from the Spring
 * properties in production ({@link #from}) and with {@link #builder(String)} in tests, which start
 * from the plan defaults and shorten only what a test needs.
 */
public record EngineSettings(
        String nodeId,
        int partitions,
        Duration runLease,
        Duration runRenewInterval,
        Duration selfFenceMargin,
        Duration partitionLease,
        Duration partitionRenewInterval,
        Duration partitionLoopInterval,
        Duration heartbeatInterval,
        Duration deadAfter,
        Duration pollInterval,
        Duration orphanSweepInterval,
        int workers,
        int reservedForRecovery,
        int leaseLostLimit,
        Duration backoffBase,
        Duration backoffCap,
        Duration materializerInterval,
        Duration materializerHorizon,
        int maxOccurrencesPerJob,
        int materializerJobBatch,
        Duration compactorInterval,
        Duration metricsRefreshInterval,
        Duration clockRefreshInterval,
        boolean epochGuardEnabled,
        Duration epochGuardTimeout) {

    public EngineSettings {
        Objects.requireNonNull(nodeId, "nodeId");
        if (nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        if (runRenewInterval.compareTo(runLease) >= 0 || selfFenceMargin.compareTo(runLease) >= 0) {
            throw new IllegalArgumentException("run lease must exceed renew interval and self-fence margin");
        }
        if (partitionRenewInterval.compareTo(partitionLease) >= 0) {
            throw new IllegalArgumentException("partition renew interval must be shorter than the lease");
        }
        if (reservedForRecovery >= workers) {
            throw new IllegalArgumentException("reservedForRecovery must be < workers");
        }
    }

    /** Settings of a production node. */
    public static EngineSettings from(SchedulerProperties p, EngineProperties e) {
        return new EngineSettings(
                p.nodeId(), p.partitions(),
                p.runLease().duration(), p.runLease().renewInterval(), p.selfFenceMargin(),
                p.partitionLease().duration(), p.partitionLease().renewInterval(),
                p.partitionLease().loopInterval(),
                p.membership().heartbeatInterval(), p.membership().deadAfter(),
                p.claim().pollInterval(), p.orphanSweep().interval(),
                p.workers().count(), p.workers().reservedForRecovery(),
                p.retry().leaseLostLimit(), p.retry().backoffBase(), p.retry().backoffCap(),
                e.materializer().interval(), e.materializer().horizon(),
                e.materializer().maxOccurrencesPerJob(), e.materializer().jobBatchSize(),
                e.compactor().interval(), e.metrics().refreshInterval(), e.clock().refreshInterval(),
                e.epochGuard().enabled(), e.epochGuard().timeout());
    }

    /** Builder pre-filled with the plan defaults (PLAN sections 06, 07, 09). */
    public static Builder builder(String nodeId) {
        return new Builder(nodeId);
    }

    /** A builder starting from these settings. */
    public Builder toBuilder() {
        Builder b = new Builder(nodeId);
        b.partitions = partitions;
        b.runLease = runLease;
        b.runRenewInterval = runRenewInterval;
        b.selfFenceMargin = selfFenceMargin;
        b.partitionLease = partitionLease;
        b.partitionRenewInterval = partitionRenewInterval;
        b.partitionLoopInterval = partitionLoopInterval;
        b.heartbeatInterval = heartbeatInterval;
        b.deadAfter = deadAfter;
        b.pollInterval = pollInterval;
        b.orphanSweepInterval = orphanSweepInterval;
        b.workers = workers;
        b.reservedForRecovery = reservedForRecovery;
        b.leaseLostLimit = leaseLostLimit;
        b.backoffBase = backoffBase;
        b.backoffCap = backoffCap;
        b.materializerInterval = materializerInterval;
        b.materializerHorizon = materializerHorizon;
        b.maxOccurrencesPerJob = maxOccurrencesPerJob;
        b.materializerJobBatch = materializerJobBatch;
        b.compactorInterval = compactorInterval;
        b.metricsRefreshInterval = metricsRefreshInterval;
        b.clockRefreshInterval = clockRefreshInterval;
        b.epochGuardEnabled = epochGuardEnabled;
        b.epochGuardTimeout = epochGuardTimeout;
        return b;
    }

    /** Mutable builder; every field starts at the plan default. */
    public static final class Builder {
        private String nodeId;
        private int partitions = 128;
        private Duration runLease = Duration.ofSeconds(10);
        private Duration runRenewInterval = Duration.ofSeconds(3);
        private Duration selfFenceMargin = Duration.ofSeconds(2);
        private Duration partitionLease = Duration.ofSeconds(6);
        private Duration partitionRenewInterval = Duration.ofSeconds(2);
        private Duration partitionLoopInterval = Duration.ofSeconds(1);
        private Duration heartbeatInterval = Duration.ofSeconds(2);
        private Duration deadAfter = Duration.ofSeconds(6);
        private Duration pollInterval = Duration.ofMillis(200);
        private Duration orphanSweepInterval = Duration.ofSeconds(1);
        private int workers = 64;
        private int reservedForRecovery = 1;
        private int leaseLostLimit = 20;
        private Duration backoffBase = Duration.ofSeconds(2);
        private Duration backoffCap = Duration.ofMinutes(5);
        private Duration materializerInterval = Duration.ofSeconds(5);
        private Duration materializerHorizon = Duration.ofMinutes(15);
        private int maxOccurrencesPerJob = 1000;
        private int materializerJobBatch = 500;
        private Duration compactorInterval = Duration.ofSeconds(1);
        private Duration metricsRefreshInterval = Duration.ofSeconds(5);
        private Duration clockRefreshInterval = Duration.ofSeconds(30);
        private boolean epochGuardEnabled = true;
        private Duration epochGuardTimeout = Duration.ofSeconds(30);

        private Builder(String nodeId) {
            this.nodeId = nodeId;
        }

        public Builder nodeId(String v) { nodeId = v; return this; }
        public Builder partitions(int v) { partitions = v; return this; }
        public Builder runLease(Duration v) { runLease = v; return this; }
        public Builder runRenewInterval(Duration v) { runRenewInterval = v; return this; }
        public Builder selfFenceMargin(Duration v) { selfFenceMargin = v; return this; }
        public Builder partitionLease(Duration v) { partitionLease = v; return this; }
        public Builder partitionRenewInterval(Duration v) { partitionRenewInterval = v; return this; }
        public Builder partitionLoopInterval(Duration v) { partitionLoopInterval = v; return this; }
        public Builder heartbeatInterval(Duration v) { heartbeatInterval = v; return this; }
        public Builder deadAfter(Duration v) { deadAfter = v; return this; }
        public Builder pollInterval(Duration v) { pollInterval = v; return this; }
        public Builder orphanSweepInterval(Duration v) { orphanSweepInterval = v; return this; }
        public Builder workers(int v) { workers = v; return this; }
        public Builder reservedForRecovery(int v) { reservedForRecovery = v; return this; }
        public Builder leaseLostLimit(int v) { leaseLostLimit = v; return this; }
        public Builder backoffBase(Duration v) { backoffBase = v; return this; }
        public Builder backoffCap(Duration v) { backoffCap = v; return this; }
        public Builder materializerInterval(Duration v) { materializerInterval = v; return this; }
        public Builder materializerHorizon(Duration v) { materializerHorizon = v; return this; }
        public Builder maxOccurrencesPerJob(int v) { maxOccurrencesPerJob = v; return this; }
        public Builder materializerJobBatch(int v) { materializerJobBatch = v; return this; }
        public Builder compactorInterval(Duration v) { compactorInterval = v; return this; }
        public Builder metricsRefreshInterval(Duration v) { metricsRefreshInterval = v; return this; }
        public Builder clockRefreshInterval(Duration v) { clockRefreshInterval = v; return this; }
        public Builder epochGuardEnabled(boolean v) { epochGuardEnabled = v; return this; }
        public Builder epochGuardTimeout(Duration v) { epochGuardTimeout = v; return this; }

        public EngineSettings build() {
            return new EngineSettings(nodeId, partitions, runLease, runRenewInterval, selfFenceMargin,
                    partitionLease, partitionRenewInterval, partitionLoopInterval, heartbeatInterval,
                    deadAfter, pollInterval, orphanSweepInterval, workers, reservedForRecovery,
                    leaseLostLimit, backoffBase, backoffCap, materializerInterval, materializerHorizon,
                    maxOccurrencesPerJob, materializerJobBatch, compactorInterval, metricsRefreshInterval,
                    clockRefreshInterval, epochGuardEnabled, epochGuardTimeout);
        }
    }
}
