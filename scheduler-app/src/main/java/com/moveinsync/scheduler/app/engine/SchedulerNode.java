package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.app.repository.claim.JdbcClaimProtocol;
import com.moveinsync.scheduler.app.repository.claim.JdbcRunQueries;
import com.moveinsync.scheduler.app.repository.claim.JdbcSlotCompaction;
import com.moveinsync.scheduler.app.repository.cluster.DbSchedulerClock;
import com.moveinsync.scheduler.app.repository.cluster.JdbcMembershipStore;
import com.moveinsync.scheduler.app.repository.cluster.JdbcPartitionLeaseStore;
import com.moveinsync.scheduler.core.policy.ExponentialFullJitterBackoff;
import com.moveinsync.scheduler.core.policy.FailureClassifier;
import com.moveinsync.scheduler.core.policy.RetryDecider;
import com.moveinsync.scheduler.core.ports.EffectSink;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One scheduler node: assembles the stores, the claim protocol and the engine loops (plan section
 * 04) and owns their lifecycle. Every node runs the same code; there is no leader.
 *
 * <ul>
 *   <li>{@link #start()}: epoch guard (plan F5), DB clock, first heartbeat, then the loops.
 *   <li>{@link #stop(Duration)}: graceful. Stop claiming, let claimed runs finish (bounded), release
 *       partitions and deregister so others take over at once.
 *   <li>{@link #kill()}: abrupt, like {@code kill -9}: every thread stops, nothing is released or
 *       deregistered, leases simply expire. In tests also close the node's pool to drop its
 *       connections.
 *   <li>{@link #freeze()} / {@link #unfreeze()}: a stop-the-world pause (SIGSTOP, long GC): loops,
 *       renewals, effects and commits all wait; the monotonic self-fence clock keeps running.
 * </ul>
 */
public final class SchedulerNode {

    private static final Logger log = LoggerFactory.getLogger(SchedulerNode.class);

    private final EngineSettings settings;
    private final NodeGate gate = new NodeGate();
    private final DbSchedulerClock clock;
    private final JdbcClaimProtocol protocol;
    private final JdbcMembershipStore membership;
    private final EngineMetrics metrics;
    private final EpochGuard epochGuard;
    private final MembershipLoop membershipLoop;
    private final PartitionLoop partitionLoop;
    private final Dispatcher dispatcher;
    private final LeaseRenewer renewer;
    private final Poller poller;
    private final OrphanSweep orphanSweep;
    private final SlotCompactor compactor;
    private final TaskLoop materializerLoop;
    private final TaskLoop gaugeLoop;
    private final TaskLoop clockLoop;
    private volatile State state = State.NEW;

    /** Lifecycle state. */
    public enum State { NEW, RUNNING, STOPPED, KILLED }

    public SchedulerNode(EngineSettings settings, DataSource dataSource, EffectSink sink, HandlerRegistry handlers,
                         MeterRegistry meters) {
        this.settings = Objects.requireNonNull(settings, "settings");
        Objects.requireNonNull(dataSource, "dataSource");
        Objects.requireNonNull(sink, "sink");
        Objects.requireNonNull(handlers, "handlers");
        String id = settings.nodeId();
        this.clock = new DbSchedulerClock(dataSource);
        this.metrics = new EngineMetrics(meters, id);
        RetryDecider decider = new RetryDecider(backoff(settings), new FailureClassifier(), settings.leaseLostLimit());
        this.protocol = new JdbcClaimProtocol(dataSource, settings.runLease(), decider, metrics, settings.deadAfter());
        this.membership = new JdbcMembershipStore(dataSource, settings.deadAfter());
        JdbcPartitionLeaseStore leases = new JdbcPartitionLeaseStore(dataSource);
        JdbcRunQueries queries = new JdbcRunQueries(dataSource);
        this.epochGuard = new EpochGuard(dataSource, sink, settings.epochGuardTimeout());

        this.partitionLoop = new PartitionLoop(id, settings, gate, leases, membership, clock, metrics);
        this.membershipLoop = new MembershipLoop(id, settings.heartbeatInterval(), gate, membership,
                partitionLoop::lastCompletedNanos, settings.partitionLease());
        this.renewer = new LeaseRenewer(id, protocol, clock, settings.runRenewInterval(), gate, metrics);
        RunExecutor executor = new RunExecutor(id, settings, protocol, sink, handlers, queries, decider, renewer, clock,
                gate, metrics);
        this.dispatcher = new Dispatcher(id, settings.workers(), settings.reservedForRecovery(), executor::execute);
        this.poller = new Poller(id, settings.pollInterval(), gate, protocol, queries, partitionLoop::owned, dispatcher,
                clock);
        this.dispatcher.onFinished(run -> poller.slotFreed(run.tenantId()));
        this.orphanSweep = new OrphanSweep(id, settings.orphanSweepInterval(), gate, protocol, dispatcher, clock);
        this.compactor = new SlotCompactor(id, settings.compactorInterval(), gate, new JdbcSlotCompaction(dataSource));
        Materializer materializer = new Materializer(dataSource, partitionLoop::owned, settings.materializerHorizon(),
                settings.maxOccurrencesPerJob(), settings.materializerJobBatch());
        this.materializerLoop = new TaskLoop("materializer", id, settings.materializerInterval(), gate,
                materializer::materializeOnce);
        this.gaugeLoop = new TaskLoop("gauges", id, settings.metricsRefreshInterval(), gate, () -> {
            long[] c = queries.runCounts();
            metrics.runCounts(c[0], c[1], c[2], c[3]);
            metrics.tenantCaps(queries.tenantCaps());
        });
        this.clockLoop = new TaskLoop("db-clock", id, settings.clockRefreshInterval(), gate, clock::refresh);
    }

    private static ExponentialFullJitterBackoff backoff(EngineSettings s) {
        // java.util.Random is thread-safe; ThreadLocalRandom.current() must not be shared across threads
        return new ExponentialFullJitterBackoff(s.backoffBase(), s.backoffCap(), new Random());
    }

    /**
     * Starts the node.
     *
     * @throws EpochGuard.EpochGuardException if the epoch guard refuses (the node stays stopped)
     */
    public synchronized void start() {
        if (state != State.NEW) {
            throw new IllegalStateException("node " + settings.nodeId() + " is " + state);
        }
        if (settings.epochGuardEnabled()) {
            epochGuard.check();
        }
        clock.refresh();
        membership.heartbeat(settings.nodeId());
        for (PeriodicLoop loop : loops()) {
            loop.start();
        }
        state = State.RUNNING;
        log.info("node {} started ({} workers, {} reserved for recovery)", settings.nodeId(), settings.workers(),
                settings.reservedForRecovery());
    }

    /** Graceful shutdown; claimed runs get up to {@code timeout} to finish. */
    public synchronized void stop(Duration timeout) {
        if (state != State.RUNNING) {
            state = state == State.NEW ? State.STOPPED : state;
            return;
        }
        Duration loopTimeout = Duration.ofSeconds(5);
        for (PeriodicLoop loop : List.of(poller, orphanSweep, materializerLoop, compactor)) {
            loop.stop(loopTimeout);
        }
        dispatcher.drain(timeout);
        renewer.shutdown(loopTimeout);
        partitionLoop.stop(loopTimeout);
        membershipLoop.stop(loopTimeout);
        gaugeLoop.stop(loopTimeout);
        clockLoop.stop(loopTimeout);
        try {
            partitionLoop.releaseAll();
            membership.deregister(settings.nodeId());
        } catch (RuntimeException e) {
            log.warn("could not release partitions / deregister; leases will expire: {}", e.toString());
        }
        state = State.STOPPED;
        log.info("node {} stopped", settings.nodeId());
    }

    /** Abrupt stop without releasing anything (kill -9 simulation). */
    public synchronized void kill() {
        gate.kill();
        for (PeriodicLoop loop : loops()) {
            loop.kill();
        }
        renewer.kill();
        dispatcher.kill();
        state = State.KILLED;
        log.info("node {} killed", settings.nodeId());
    }

    /** Stop-the-world pause: nothing on this node makes progress until {@link #unfreeze()}. */
    public void freeze() {
        gate.freeze();
        log.info("node {} frozen", settings.nodeId());
    }

    public void unfreeze() {
        gate.unfreeze();
        log.info("node {} unfrozen", settings.nodeId());
    }

    public String nodeId() {
        return settings.nodeId();
    }

    public State state() {
        return state;
    }

    /** Partitions this node polls right now. */
    public Set<Integer> ownedPartitions() {
        return partitionLoop.owned();
    }

    public EngineMetrics metrics() {
        return metrics;
    }

    public JdbcClaimProtocol claimProtocol() {
        return protocol;
    }

    /** Runs executing or queued on this node. */
    public int activeRuns() {
        return dispatcher.busy() + dispatcher.queued();
    }

    /** True while heartbeats are withheld because the partition loop stalled. */
    public boolean heartbeatSilenced() {
        return membershipLoop.silenced();
    }

    /** Observes every partition-loop tick (monotonic time, owned/acquired/renewed/released sets). */
    public void onPartitionTick(Consumer<PartitionTick> listener) {
        Objects.requireNonNull(listener, "listener");
        partitionLoop.onTick(r -> listener.accept(
                new PartitionTick(r.monotonicNanos(), r.owned(), r.acquired(), r.renewed(), r.released())));
    }

    /** Public view of one partition-loop tick. */
    public record PartitionTick(long monotonicNanos, Set<Integer> owned, Set<Integer> acquired, Set<Integer> renewed,
                                Set<Integer> released) {
    }

    private List<PeriodicLoop> loops() {
        return List.of(membershipLoop, partitionLoop, poller, orphanSweep, materializerLoop, compactor, gaugeLoop,
                clockLoop);
    }
}
