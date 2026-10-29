package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.core.domain.LeaseLostReason;
import com.moveinsync.scheduler.core.ports.ClaimProtocol;
import com.moveinsync.scheduler.core.ports.SchedulerClock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Renews the lease of every running run every 3 s (plan section 07) and enforces the max run time.
 *
 * <ul>
 *   <li>The send time is captured <b>before</b> the renew statement, and {@link
 *       ActiveRun#renewed(long)} ({@code LeaseClock.renewed}) is called only after the DB confirmed
 *       1 row, so the self-fence deadline is never later than the real expiry minus the margin.
 *   <li>0 rows means the lease is lost (expired or taken over): the handler is interrupted at once
 *       and nothing more is sent.
 *   <li>An exception (DB unreachable) is only counted: the run keeps its lease until it expires,
 *       and the self-fence stops new effects before that.
 * </ul>
 *
 * The scheduler thread only triggers; each renewal runs on its own virtual thread so a slow DB call
 * for one run never delays the others. A frozen or dead node renews nothing.
 */
final class LeaseRenewer {

    private static final Logger log = LoggerFactory.getLogger(LeaseRenewer.class);

    private final ClaimProtocol protocol;
    private final SchedulerClock clock;
    private final Duration interval;
    private final NodeGate gate;
    private final EngineMetrics metrics;
    private final String nodeId;
    private final ScheduledExecutorService scheduler;

    LeaseRenewer(String nodeId, ClaimProtocol protocol, SchedulerClock clock, Duration interval, NodeGate gate,
                 EngineMetrics metrics) {
        this.nodeId = nodeId;
        this.protocol = Objects.requireNonNull(protocol, "protocol");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.interval = Objects.requireNonNull(interval, "interval");
        this.gate = Objects.requireNonNull(gate, "gate");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.scheduler = Executors.newScheduledThreadPool(1,
                Thread.ofPlatform().daemon().name(nodeId + "-renewer").factory());
    }

    /** Starts renewing {@code run} and arms its max-run-time stop. */
    void register(ActiveRun run) {
        AtomicBoolean inFlight = new AtomicBoolean();
        long ms = interval.toMillis();
        ScheduledFuture<?> renew = scheduler.scheduleWithFixedDelay(() -> {
            if (gate.isRunning() && !run.finished() && run.lost() == null && inFlight.compareAndSet(false, true)) {
                Thread.ofVirtual().name(nodeId + "-renew-" + run.run().runId()).start(() -> {
                    try {
                        renew(run);
                    } finally {
                        inFlight.set(false);
                    }
                });
            }
        }, ms, ms, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> timeout = scheduler.schedule(run::timeout,
                run.run().maxRunTime().toMillis(), TimeUnit.MILLISECONDS);
        run.tasks(renew, timeout);
    }

    private void renew(ActiveRun run) {
        long sentNanos = clock.monotonicNanos();
        boolean ok;
        try {
            ok = protocol.renew(run.run().runId(), run.run().token());
        } catch (RuntimeException e) {
            metrics.renewFailure();
            log.warn("lease renew failed for run {}: {}", run.run().runId(), e.toString());
            return;
        }
        if (ok) {
            run.renewed(sentNanos);
            return;
        }
        metrics.renewFailure();
        if (!run.finished() && run.lost() == null) {
            MDC.put("runId", String.valueOf(run.run().runId()));
            log.warn("lease lost on renew (fenced write updated 0 rows); stopping handler");
            MDC.remove("runId");
            run.loseLease(LeaseLostReason.FENCED_WRITE_REJECTED, true);
        }
    }

    void shutdown(Duration timeout) {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    void kill() {
        scheduler.shutdownNow();
    }
}
