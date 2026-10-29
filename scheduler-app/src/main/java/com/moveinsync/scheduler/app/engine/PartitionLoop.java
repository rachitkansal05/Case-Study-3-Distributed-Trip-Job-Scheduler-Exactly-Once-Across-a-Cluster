package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.core.cluster.PartitionAssigner;
import com.moveinsync.scheduler.core.cluster.PartitionPlanner;
import com.moveinsync.scheduler.core.ports.MembershipStore;
import com.moveinsync.scheduler.core.ports.PartitionLeaseStore;
import com.moveinsync.scheduler.core.ports.PartitionLeaseStore.PartitionLease;
import com.moveinsync.scheduler.core.ports.SchedulerClock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partition lease loop (plan section 06.4): every 1 s read all lease rows and the live set, let
 * {@link PartitionPlanner} decide, then acquire, renew and release. The database decides races.
 *
 * <p>Two additions on top of the planner, both about the 7 s recovery bound after a crash:
 *
 * <ul>
 *   <li><b>Hold a takeover.</b> Right after a crash the dead node is still in the live set for up
 *       to the membership timeout, so the planner would hand a partition we just took over back to
 *       it (release on the next tick) and the row would sit unowned until membership drops the dead
 *       node. We keep a partition taken over from node X while X is still in the live set, for at
 *       most {@code deadAfter}; if X is in fact alive it gets the partition back after that.
 *   <li><b>Wake at expiry.</b> When a foreign lease that we would take over expires before the next
 *       regular tick, the next tick is scheduled just after that expiry instead of up to a whole
 *       interval later.
 * </ul>
 */
final class PartitionLoop extends PeriodicLoop {

    private static final Logger log = LoggerFactory.getLogger(PartitionLoop.class);
    private static final Duration EXPIRY_SLACK = Duration.ofMillis(30);
    private static final Duration MIN_DELAY = Duration.ofMillis(20);

    /** What one tick did (for tests and logs). */
    record TickReport(long monotonicNanos, Set<Integer> owned, Set<Integer> acquired, Set<Integer> renewed,
                      Set<Integer> released) {
    }

    private record Takeover(String previousOwner, long atNanos) {
    }

    private final String nodeId;
    private final PartitionLeaseStore leases;
    private final MembershipStore membership;
    private final SchedulerClock clock;
    private final PartitionPlanner planner;
    private final PartitionAssigner assigner = new PartitionAssigner();
    private final Duration leaseDuration;
    private final Duration deadAfter;
    private final EngineMetrics metrics;
    private final Map<Integer, Takeover> takenOver = new HashMap<>();
    private volatile Set<Integer> owned = Set.of();
    private volatile Duration nextDelay;
    private volatile Consumer<TickReport> listener = r -> { };

    PartitionLoop(String nodeId, EngineSettings s, NodeGate gate, PartitionLeaseStore leases,
                  MembershipStore membership, SchedulerClock clock, EngineMetrics metrics) {
        super("partitions", nodeId, s.partitionLoopInterval(), gate);
        this.nodeId = nodeId;
        this.leases = Objects.requireNonNull(leases, "leases");
        this.membership = Objects.requireNonNull(membership, "membership");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.leaseDuration = s.partitionLease();
        this.deadAfter = s.deadAfter();
        this.planner = new PartitionPlanner(PartitionPlanner.Settings.of(s.partitionLease(), s.partitionRenewInterval()));
        this.nextDelay = s.partitionLoopInterval();
    }

    /** Partitions this node polls (owned after the last tick). */
    Set<Integer> owned() {
        return owned;
    }

    void onTick(Consumer<TickReport> l) {
        listener = Objects.requireNonNull(l, "listener");
    }

    @Override
    protected Duration nextDelay() {
        return nextDelay;
    }

    @Override
    protected void tick() {
        Instant now = clock.now();
        List<PartitionLease> rows = leases.findAll();
        Set<String> live = new TreeSet<>();
        for (MembershipStore.NodeInfo n : membership.liveNodes(deadAfter)) {
            live.add(n.nodeId());
        }
        live.add(nodeId);
        PartitionPlanner.Plan plan = planner.plan(rows, live, nodeId, now);
        Map<Integer, PartitionLease> byId = new HashMap<>();
        rows.forEach(r -> byId.put(r.partitionId(), r));

        long nowNanos = System.nanoTime();
        takenOver.entrySet().removeIf(e -> !live.contains(e.getValue().previousOwner())
                || nowNanos - e.getValue().atNanos() > deadAfter.toNanos());

        Set<Integer> newOwned = new TreeSet<>(plan.keep());
        Set<Integer> acquired = new TreeSet<>();
        Set<Integer> renewed = new TreeSet<>();
        Set<Integer> released = new TreeSet<>();
        for (int p : plan.acquire()) {
            String previous = byId.get(p).ownerNode();
            if (leases.tryAcquireOrRenew(p, nodeId, leaseDuration).isPresent()) {
                newOwned.add(p);
                acquired.add(p);
                if (previous != null && !previous.equals(nodeId)) {
                    takenOver.put(p, new Takeover(previous, nowNanos));
                }
            }
        }
        for (int p : plan.renew()) {
            if (leases.tryAcquireOrRenew(p, nodeId, leaseDuration).isPresent()) {
                newOwned.add(p);
                renewed.add(p);
            }
        }
        for (int p : plan.release()) {
            if (takenOver.containsKey(p)) {
                // hold: the desired owner is the node we just took it from, still in the live set
                PartitionLease row = byId.get(p);
                boolean due = row.leaseUntil() == null
                        || Duration.between(now, row.leaseUntil()).compareTo(planner.settings().renewBefore()) <= 0;
                if (!due) {
                    newOwned.add(p);
                } else if (leases.tryAcquireOrRenew(p, nodeId, leaseDuration).isPresent()) {
                    newOwned.add(p);
                    renewed.add(p);
                }
                continue;
            }
            leases.release(p, nodeId);
            released.add(p);
        }
        owned = Collections.unmodifiableSet(newOwned);
        metrics.partitionsOwned(newOwned.size());
        nextDelay = computeNextDelay(rows, live, now);
        if (!acquired.isEmpty() || !released.isEmpty()) {
            log.debug("partitions: owned={} acquired={} released={}", newOwned.size(), acquired, released);
        }
        listener.accept(new TickReport(System.nanoTime(), owned, acquired, renewed, released));
    }

    /** Next tick at the regular interval, or just after a foreign lease we would take expires. */
    private Duration computeNextDelay(List<PartitionLease> rows, Set<String> live, Instant now) {
        Duration delay = interval();
        for (PartitionLease row : rows) {
            String owner = row.ownerNode();
            if (owner == null || owner.equals(nodeId) || row.leaseUntil() == null || row.leaseUntil().isBefore(now)) {
                continue;
            }
            Set<String> candidates = new HashSet<>(live);
            candidates.remove(owner);
            if (!nodeId.equals(assigner.desiredOwner(row.partitionId(), candidates))) {
                continue;
            }
            Duration untilExpiry = Duration.between(now, row.leaseUntil()).plus(EXPIRY_SLACK);
            if (untilExpiry.compareTo(delay) < 0) {
                delay = untilExpiry;
            }
        }
        return delay.compareTo(MIN_DELAY) < 0 ? MIN_DELAY : delay;
    }

    /** Graceful shutdown: release everything this node holds so others take over at once. */
    void releaseAll() {
        for (int p : leases.ownedBy(nodeId)) {
            leases.release(p, nodeId);
        }
        owned = Set.of();
        metrics.partitionsOwned(0);
    }
}
