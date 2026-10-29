package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.ports.ClaimProtocol;
import com.moveinsync.scheduler.core.ports.SchedulerClock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Orphan sweep (plan section 06.6): every 1 s, take over expired RUNNING runs in <b>any</b>
 * partition, so takeover never waits for partition ownership. Restarted runs go to the recovered
 * lane and may use the reserved worker. Takeover bound: 10 s lease + 1 s sweep + claim ≈ 11.2 s.
 */
final class OrphanSweep extends PeriodicLoop {

    private final String nodeId;
    private final ClaimProtocol protocol;
    private final Dispatcher dispatcher;
    private final SchedulerClock clock;

    OrphanSweep(String nodeId, Duration interval, NodeGate gate, ClaimProtocol protocol, Dispatcher dispatcher,
                SchedulerClock clock) {
        super("orphan-sweep", nodeId, interval, gate);
        this.nodeId = nodeId;
        this.protocol = Objects.requireNonNull(protocol, "protocol");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    protected void tick() {
        int free = dispatcher.freeForRecovery();
        if (free <= 0) {
            return;
        }
        long claimNanos = clock.monotonicNanos();
        List<ClaimedRun> taken = protocol.sweepOrphans(nodeId, free);
        if (!taken.isEmpty()) {
            dispatcher.submit(taken, claimNanos);
        }
    }
}
