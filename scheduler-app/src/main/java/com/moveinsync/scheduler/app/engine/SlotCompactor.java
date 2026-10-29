package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.app.repository.claim.JdbcSlotCompaction;
import java.time.Duration;
import java.util.Objects;

/**
 * Applies {@code CapChangePlanner} plans every tick (plan section 10): inserts missing low slots,
 * deletes free high slots, and moves running runs from high slots into freed low slots, re-checking
 * each step under its locks (slot rows, then the run row). While a reduction drains, claims yield,
 * so no new run starts until the running count is below the new cap.
 */
final class SlotCompactor extends PeriodicLoop {

    private final JdbcSlotCompaction compaction;

    SlotCompactor(String nodeId, Duration interval, NodeGate gate, JdbcSlotCompaction compaction) {
        super("slot-compactor", nodeId, interval, gate);
        this.compaction = Objects.requireNonNull(compaction, "compaction");
    }

    @Override
    protected void tick() {
        for (JdbcSlotCompaction.TenantSlots tenant : compaction.tenantsNeedingWork()) {
            compaction.apply(tenant);
        }
    }
}
