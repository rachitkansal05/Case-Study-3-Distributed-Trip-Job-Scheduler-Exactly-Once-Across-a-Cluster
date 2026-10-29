package com.moveinsync.scheduler.app.repository.claim;

import com.moveinsync.scheduler.core.domain.TenantId;
import java.util.List;

/**
 * Test seams inside a claim transaction, used only by race tests to force exact interleavings
 * with barriers (for example "B reads an expired lease, A renews, B reaps"). Production uses
 * {@link #NONE}. Package-private on purpose.
 */
interface ClaimHooks {

    ClaimHooks NONE = new ClaimHooks() {
    };

    /** Step 1 finished: {@code slots} are locked by this transaction. */
    default void afterSlotsLocked(TenantId tenant, List<Integer> slots) {
    }

    /** About to send the conditional reap of {@code holderRunId}, which looked expired. */
    default void beforeReap(long holderRunId) {
    }

    /** The conditional reap of {@code holderRunId} returned; the transaction is still open. */
    default void afterReap(long holderRunId, boolean freed) {
    }
}
