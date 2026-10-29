package com.moveinsync.scheduler.app.engine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.app.handler.JobHandler;
import com.moveinsync.scheduler.app.handler.LeaseLostException;
import com.moveinsync.scheduler.app.repository.claim.JdbcRunQueries;
import com.moveinsync.scheduler.app.repository.claim.JdbcRunQueries.JobSpec;
import com.moveinsync.scheduler.app.sink.ChannelAwareEffectSink;
import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.LeaseLostReason;
import com.moveinsync.scheduler.core.domain.RunContext;
import com.moveinsync.scheduler.core.domain.RunFailure;
import com.moveinsync.scheduler.core.lease.LeaseClock;
import com.moveinsync.scheduler.core.policy.FailureClassifier;
import com.moveinsync.scheduler.core.policy.FailureKind;
import com.moveinsync.scheduler.core.policy.PermanentJobException;
import com.moveinsync.scheduler.core.policy.RetryDecider;
import com.moveinsync.scheduler.core.policy.RetryableJobException;
import com.moveinsync.scheduler.core.ports.ClaimProtocol;
import com.moveinsync.scheduler.core.ports.EffectSink;
import com.moveinsync.scheduler.core.ports.SchedulerClock;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Executes one claimed run on a worker thread: self-fence, handler, fenced commit, and outcome
 * classification (plan sections 07 and 09). MDC carries {@code nodeId, runId, jobId, tenantId,
 * fencingToken, attempt} while the run executes; payloads are never logged.
 *
 * <ul>
 *   <li>Success → {@code commitSuccess}. A rejected commit (0 rows) means the lease was lost:
 *       {@code scheduler_stale_commit_rejected_total} and {@code scheduler_lease_lost_total}.
 *   <li>{@link LeaseLostException} (self-fence, sink 409, fenced checkpoint) → nothing is written;
 *       the reap returns the run to {@code RETRY_WAIT} without using an attempt.
 *   <li>Any other failure → {@link RetryDecider#onFailure} → fenced {@code commitFailure} (retry
 *       with backoff, or dead letter). Max run time → {@code TIMEOUT}, retryable.
 * </ul>
 */
final class RunExecutor {

    private static final Logger log = LoggerFactory.getLogger(RunExecutor.class);

    /** A claimed run plus the monotonic time taken before its claim statement was sent. */
    record Ticket(ClaimedRun run, long claimNanos) {
    }

    private record SpecKey(UUID jobId, long version) {
    }

    private final String nodeId;
    private final ClaimProtocol protocol;
    private final EffectSink sink;
    private final HandlerRegistry handlers;
    private final JdbcRunQueries queries;
    private final RetryDecider retryDecider;
    private final FailureClassifier classifier = new FailureClassifier();
    private final LeaseRenewer renewer;
    private final SchedulerClock clock;
    private final Duration runLease;
    private final Duration selfFenceMargin;
    private final NodeGate gate;
    private final EngineMetrics metrics;
    private final Cache<SpecKey, JobSpec> specs = Caffeine.newBuilder().maximumSize(50_000).build();

    RunExecutor(String nodeId, EngineSettings settings, ClaimProtocol protocol, EffectSink sink,
                HandlerRegistry handlers, JdbcRunQueries queries, RetryDecider retryDecider, LeaseRenewer renewer,
                SchedulerClock clock, NodeGate gate, EngineMetrics metrics) {
        this.nodeId = nodeId;
        this.protocol = Objects.requireNonNull(protocol, "protocol");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.handlers = Objects.requireNonNull(handlers, "handlers");
        this.queries = Objects.requireNonNull(queries, "queries");
        this.retryDecider = Objects.requireNonNull(retryDecider, "retryDecider");
        this.renewer = Objects.requireNonNull(renewer, "renewer");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.runLease = settings.runLease();
        this.selfFenceMargin = settings.selfFenceMargin();
        this.gate = Objects.requireNonNull(gate, "gate");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
    }

    void execute(Ticket ticket) {
        ClaimedRun run = ticket.run();
        MDC.put("nodeId", nodeId);
        MDC.put("runId", String.valueOf(run.runId()));
        MDC.put("jobId", run.jobId().toString());
        MDC.put("tenantId", run.tenantId().value());
        MDC.put("fencingToken", run.token().toHeader());
        MDC.put("attempt", String.valueOf(run.attempt()));
        ActiveRun active = new ActiveRun(run,
                LeaseClock.startedAt(clock, ticket.claimNanos(), runLease, selfFenceMargin), Thread.currentThread());
        int maxAttempts = 1;
        try {
            renewer.register(active);
            JobSpec spec = spec(run);
            maxAttempts = spec.maxAttempts();
            JobHandler<?> handler = handlers.get(run.handler());
            RunContext context = new RunContext(run.runId(), run.jobId(), run.tenantId(), run.attempt(), run.idemKey(),
                    run.token(), run.checkpointJson(), run.claimedAt().plus(run.maxRunTime()));
            handler.handle(new RunEffectContext(active, context, spec.payloadJson(), sinkFor(run), protocol, gate));
            if (active.lost() != null) {
                leaseLost(active.lost());
                return;
            }
            gate.pass(); // a pause can land between the last effect and the commit
            if (protocol.commitSuccess(run.runId(), run.token())) {
                metrics.outcome("success");
            } else {
                staleCommit();
            }
        } catch (LeaseLostException e) {
            leaseLost(e.reason());
        } catch (NodeGate.NodeKilledException e) {
            // the process is gone: nothing more happens on this node
        } catch (Throwable failure) {
            onFailure(active, run, maxAttempts, failure);
        } finally {
            active.finish();
            Thread.interrupted(); // never leak an interrupt aimed at this run
            MDC.clear();
        }
    }

    private void onFailure(ActiveRun active, ClaimedRun run, int maxAttempts, Throwable failure) {
        if (gate.isKilled()) {
            return;
        }
        if (active.lost() != null) {
            leaseLost(active.lost()); // interrupted because the lease was lost: not a job failure
            return;
        }
        if (failure instanceof Error error && !(failure instanceof AssertionError)) {
            log.error("handler error: {}", error.getClass().getName());
        }
        boolean timeout = active.timedOut();
        Throwable cause = timeout ? new RetryableJobException("TIMEOUT", "max run time exceeded") : failure;
        if (cause instanceof InterruptedException) {
            cause = new RetryableJobException("INTERRUPTED", "worker interrupted");
        }
        RunFailure decision = retryDecider.onFailure(cause, run.attempt(), maxAttempts);
        Thread.interrupted(); // the commit must not be cut short by the interrupt that stopped the handler
        try {
            gate.pass();
            if (!protocol.commitFailure(run.runId(), run.token(), decision)) {
                staleCommit();
                return;
            }
        } catch (NodeGate.NodeKilledException e) {
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (RuntimeException e) {
            log.warn("failure commit failed; the lease will expire and the run is retried: {}", e.toString());
            return;
        }
        String outcome = timeout ? "timeout"
                : classifier.classify(cause) == FailureKind.PERMANENT ? "permanent" : "retryable";
        metrics.outcome(outcome);
        log.info("attempt failed: {} -> {}", classifier.errorCodeOf(cause),
                decision instanceof RunFailure.Retry ? "RETRY_WAIT" : "DEAD_LETTERED");
    }

    private JobSpec spec(ClaimedRun run) {
        SpecKey key = new SpecKey(run.jobId(), run.jobVersion());
        JobSpec cached = specs.getIfPresent(key);
        if (cached != null) {
            return cached;
        }
        JobSpec spec = queries.jobSpec(run.jobId())
                .orElseThrow(() -> new PermanentJobException("JOB_NOT_FOUND", "job definition missing"));
        specs.put(key, spec);
        return spec;
    }

    private EffectSink sinkFor(ClaimedRun run) {
        return sink instanceof ChannelAwareEffectSink channelAware ? channelAware.boundTo(run.handler()) : sink;
    }

    private void leaseLost(LeaseLostReason reason) {
        metrics.leaseLost(reason);
        log.info("lease lost ({}); stopped without writing", reason.metricTag());
    }

    private void staleCommit() {
        metrics.staleCommitRejected();
        metrics.leaseLost(LeaseLostReason.FENCED_WRITE_REJECTED);
        log.warn("commit rejected: fencing token stale or lease expired");
    }
}
