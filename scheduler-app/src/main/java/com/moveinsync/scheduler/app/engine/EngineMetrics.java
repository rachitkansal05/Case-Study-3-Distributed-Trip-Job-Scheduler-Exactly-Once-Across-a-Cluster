package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.app.repository.claim.ClaimObserver;
import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.LeaseLostReason;
import com.moveinsync.scheduler.core.domain.TenantId;
import com.moveinsync.scheduler.core.policy.RetryDecider;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Engine metrics with the exact names of plan section 14 (Prometheus names in brackets):
 *
 * <ul>
 *   <li>{@code scheduler.fire.lag} [{@code scheduler_fire_lag_seconds}] histogram, SLO buckets
 *       0.1 .. 30 s, tags {@code tenant_tier}, {@code handler}; DB time {@code started_at -
 *       scheduled_at} of a run's first start.
 *   <li>{@code scheduler.runs} [{@code scheduler_runs}] gauge, tag {@code state} = running |
 *       retrying | dead_lettered (DB counts, refreshed every 5 s).
 *   <li>{@code scheduler.queue.depth} [{@code scheduler_queue_depth}] gauge: due and not started.
 *   <li>{@code scheduler.run.outcome} [{@code scheduler_run_outcome_total}] counter, tag {@code
 *       outcome} = success | retryable | permanent | timeout.
 *   <li>{@code scheduler.stale.commit.rejected} [{@code scheduler_stale_commit_rejected_total}].
 *   <li>{@code scheduler.tenant.cap.rejections} [{@code scheduler_tenant_cap_rejections_total}],
 *       tag {@code tenant}.
 *   <li>{@code scheduler.partitions.owned} [{@code scheduler_partitions_owned}], tag {@code node}.
 *   <li>{@code scheduler.lease.renew.failures} [{@code scheduler_lease_renew_failures_total}].
 *   <li>{@code scheduler.claim.batch} [{@code scheduler_claim_batch_seconds}] timer.
 *   <li>{@code scheduler.tenant.cap} [{@code scheduler_tenant_cap}] gauge, tags {@code tenant},
 *       {@code kind} = configured | effective.
 *   <li>{@code scheduler.lease.lost} [{@code scheduler_lease_lost_total}], tag {@code reason} =
 *       {@link LeaseLostReason#metricTag()}.
 *   <li>{@code scheduler.orphan.takeovers} [{@code scheduler_orphan_takeovers_total}] and {@code
 *       scheduler.orphan.takeover.time} [{@code scheduler_orphan_takeover_time_seconds}] histogram
 *       (DB time from the dead owner's last successful lease renewal, the last moment it was
 *       proven alive, to the restart; at least one run lease). Buckets include the exact takeover
 *       bounds 10.7 s (lease 10 s + sweep 0.5 s + claim 0.2 s) and 11.2 s (the alert threshold),
 *       so those alerts read a bucket instead of interpolating.
 *   <li>{@code scheduler.dead.letters} [{@code scheduler_dead_letters_total}], tag {@code
 *       error_code}: runs dead-lettered (committed), e.g. {@code FENCING_CONFLICT} at the lease-lost
 *       limit.
 * </ul>
 *
 * <p>Series are registered eagerly where the label values are known up front (fire lag per
 * handler type, outcomes, lease-lost reasons, dead letters for {@code FENCING_CONFLICT}, cap
 * rejections per tenant on each cap refresh), so a new node exports them before its first event.
 *
 * One instance per node; tests give each simulated node its own registry.
 */
public final class EngineMetrics implements ClaimObserver {

    /** All tenants share one tier until tiers exist; keeps the label set stable. */
    public static final String DEFAULT_TIER = "standard";

    private static final Duration[] LAG_SLO = {
        Duration.ofMillis(100), Duration.ofMillis(250), Duration.ofMillis(500), Duration.ofSeconds(1),
        Duration.ofSeconds(2), Duration.ofSeconds(5), Duration.ofSeconds(10), Duration.ofSeconds(30)};

    /* A takeover always takes at least one run lease (10 s by default), so the buckets are dense
       around lease + sweep + claim; the low ones serve short test leases. */
    private static final Duration[] TAKEOVER_SLO = {
        Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(5), Duration.ofSeconds(10),
        Duration.ofMillis(10_500), Duration.ofMillis(10_700), Duration.ofSeconds(11), Duration.ofMillis(11_200),
        Duration.ofSeconds(12), Duration.ofSeconds(15), Duration.ofSeconds(30)};

    private final MeterRegistry registry;
    private final String nodeId;
    private final Timer claimBatch;
    private final Counter staleCommit;
    private final Counter renewFailures;
    private final Counter orphanTakeovers;
    private final Timer orphanTakeoverTime;
    private final AtomicLong partitionsOwned = new AtomicLong();
    private final AtomicLong queueDepth = new AtomicLong();
    private final AtomicLong running = new AtomicLong();
    private final AtomicLong retrying = new AtomicLong();
    private final AtomicLong deadLettered = new AtomicLong();
    private final MultiGauge tenantCaps;

    public EngineMetrics(MeterRegistry registry, String nodeId) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.claimBatch = Timer.builder("scheduler.claim.batch")
                .description("Duration of one per-tenant claim transaction")
                .publishPercentileHistogram()
                .register(registry);
        this.staleCommit = Counter.builder("scheduler.stale.commit.rejected")
                .description("Commits rejected by the fenced predicate (lease lost)").register(registry);
        this.renewFailures = Counter.builder("scheduler.lease.renew.failures")
                .description("Lease renewals that failed or updated 0 rows").register(registry);
        this.orphanTakeovers = Counter.builder("scheduler.orphan.takeovers")
                .description("Expired runs restarted by the orphan sweep").register(registry);
        this.orphanTakeoverTime = Timer.builder("scheduler.orphan.takeover.time")
                .description("Time from the dead owner's last lease renewal to the orphan restart (DB time)")
                .serviceLevelObjectives(TAKEOVER_SLO).register(registry);
        registry.gauge("scheduler.partitions.owned", Tags.of("node", nodeId), partitionsOwned);
        registry.gauge("scheduler.queue.depth", Tags.empty(), queueDepth);
        registry.gauge("scheduler.runs", Tags.of("state", "running"), running);
        registry.gauge("scheduler.runs", Tags.of("state", "retrying"), retrying);
        registry.gauge("scheduler.runs", Tags.of("state", "dead_lettered"), deadLettered);
        this.tenantCaps = MultiGauge.builder("scheduler.tenant.cap")
                .description("Configured cap and effective cap (slot rows) per tenant").register(registry);
        for (LeaseLostReason reason : LeaseLostReason.values()) {
            leaseLostCounter(reason);
        }
        for (String outcome : List.of("success", "retryable", "permanent", "timeout")) {
            outcomeCounter(outcome);
        }
        for (HandlerType handler : HandlerType.values()) {
            fireLagTimer(handler.name());
        }
        deadLetterCounter(RetryDecider.FENCING_CONFLICT);
    }

    /** The registry the meters live in. */
    public MeterRegistry registry() {
        return registry;
    }

    // ------------------------------------------------------------------ ClaimObserver

    @Override
    public void onClaimBatch(TenantId tenant, Duration elapsed, int started) {
        claimBatch.record(elapsed);
    }

    @Override
    public void onCapRejected(TenantId tenant) {
        capRejectionCounter(tenant.value()).increment();
    }

    @Override
    public void onFirstStart(ClaimedRun run, Duration fireLag) {
        fireLagTimer(run.handler().name()).record(fireLag);
    }

    @Override
    public void onDeadLettered(long runId, String errorCode) {
        deadLetterCounter(errorCode).increment();
    }

    @Override
    public void onOrphanTakeover(ClaimedRun run, Duration sinceLastRenewal) {
        orphanTakeovers.increment();
        orphanTakeoverTime.record(sinceLastRenewal);
    }

    // ------------------------------------------------------------------ engine events

    /** One finished attempt: success | retryable | permanent | timeout. */
    public void outcome(String outcome) {
        outcomeCounter(outcome).increment();
    }

    public void staleCommitRejected() {
        staleCommit.increment();
    }

    public void leaseLost(LeaseLostReason reason) {
        leaseLostCounter(reason).increment();
    }

    public void renewFailure() {
        renewFailures.increment();
    }

    public void partitionsOwned(int count) {
        partitionsOwned.set(count);
    }

    /** DB-count gauges, refreshed periodically. */
    public void runCounts(long runningRuns, long retryingRuns, long deadLetteredRuns, long dueNotStarted) {
        running.set(runningRuns);
        retrying.set(retryingRuns);
        deadLettered.set(deadLetteredRuns);
        queueDepth.set(dueNotStarted);
    }

    /** Tenant caps: tenant -> {configured, effective}. */
    public void tenantCaps(Map<String, int[]> caps) {
        List<MultiGauge.Row<?>> rows = new ArrayList<>(caps.size() * 2);
        caps.forEach((tenant, v) -> {
            rows.add(MultiGauge.Row.of(Tags.of("tenant", tenant, "kind", "configured"), v[0]));
            rows.add(MultiGauge.Row.of(Tags.of("tenant", tenant, "kind", "effective"), v[1]));
        });
        tenantCaps.register(rows, true);
        caps.keySet().forEach(this::capRejectionCounter); // export 0 before the first rejection
    }

    // ------------------------------------------------------------------ test/ops accessors

    public double staleCommitRejectedCount() {
        return staleCommit.count();
    }

    public double leaseLostCount(LeaseLostReason reason) {
        return leaseLostCounter(reason).count();
    }

    public double renewFailureCount() {
        return renewFailures.count();
    }

    public double orphanTakeoverCount() {
        return orphanTakeovers.count();
    }

    /** Sum of all recorded takeover times (with one takeover: its value). */
    public Duration orphanTakeoverTimeTotal() {
        return Duration.ofNanos((long) orphanTakeoverTime.totalTime(TimeUnit.NANOSECONDS));
    }

    public double outcomeCount(String outcome) {
        return outcomeCounter(outcome).count();
    }

    public double deadLetterCount(String errorCode) {
        return deadLetterCounter(errorCode).count();
    }

    private Timer fireLagTimer(String handler) {
        return Timer.builder("scheduler.fire.lag")
                .description("started_at - scheduled_at on the DB clock")
                .tags("tenant_tier", DEFAULT_TIER, "handler", handler)
                .serviceLevelObjectives(LAG_SLO)
                .register(registry);
    }

    private Counter capRejectionCounter(String tenant) {
        return Counter.builder("scheduler.tenant.cap.rejections").tag("tenant", tenant)
                .description("Claims skipped because the tenant's slots were all busy").register(registry);
    }

    private Counter deadLetterCounter(String errorCode) {
        return Counter.builder("scheduler.dead.letters").tag("error_code", errorCode)
                .description("Runs dead-lettered, by error code").register(registry);
    }

    private Counter leaseLostCounter(LeaseLostReason reason) {
        return Counter.builder("scheduler.lease.lost").tag("reason", reason.metricTag())
                .description("Runs this node stopped because it lost the lease").register(registry);
    }

    private Counter outcomeCounter(String outcome) {
        return Counter.builder("scheduler.run.outcome").tag("outcome", outcome)
                .description("Finished attempts by outcome").register(registry);
    }

    @Override
    public String toString() {
        return "EngineMetrics[" + nodeId + "]";
    }
}
