package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.core.ports.MembershipStore;
import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Heartbeat every 2 s (plan section 06.2). <b>Stops heartbeating while the partition loop is
 * stalled for more than one partition-lease period</b>: other nodes treat an expired partition of
 * a still-live node as taken over but then hand it back to the desired owner (PartitionPlanner,
 * "known effect"); if that owner's partition loop is stuck its partitions would stay unowned for
 * as long as its heartbeat continued. Going silent lets membership drop it, so the partitions move.
 */
final class MembershipLoop extends PeriodicLoop {

    private static final Logger log = LoggerFactory.getLogger(MembershipLoop.class);

    private final MembershipStore store;
    private final String nodeId;
    private final LongSupplier partitionLoopLastTick;
    private final long stallLimitNanos;
    private boolean silenced;

    MembershipLoop(String nodeId, Duration interval, NodeGate gate, MembershipStore store,
                   LongSupplier partitionLoopLastTickNanos, Duration partitionLease) {
        super("membership", nodeId, interval, gate);
        this.nodeId = nodeId;
        this.store = Objects.requireNonNull(store, "store");
        this.partitionLoopLastTick = Objects.requireNonNull(partitionLoopLastTickNanos, "partitionLoopLastTick");
        this.stallLimitNanos = partitionLease.toNanos();
    }

    @Override
    protected void tick() {
        long stalled = System.nanoTime() - partitionLoopLastTick.getAsLong();
        if (stalled > stallLimitNanos) {
            if (!silenced) {
                log.warn("partition loop stalled for {} ms; stopping heartbeats so partitions can move",
                        stalled / 1_000_000);
                silenced = true;
            }
            return;
        }
        if (silenced) {
            log.info("partition loop recovered; heartbeating again");
            silenced = false;
        }
        store.heartbeat(nodeId);
    }

    /** True while heartbeats are withheld because the partition loop stalled. */
    boolean silenced() {
        return silenced;
    }
}
