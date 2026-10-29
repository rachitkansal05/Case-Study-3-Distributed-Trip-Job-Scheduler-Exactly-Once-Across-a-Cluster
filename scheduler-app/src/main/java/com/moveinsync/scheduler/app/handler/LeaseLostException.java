package com.moveinsync.scheduler.app.handler;

import com.moveinsync.scheduler.core.domain.LeaseLostReason;
import java.util.Objects;

/**
 * This node lost the right to act for the run (plan section 09): not a job failure, does not use
 * up an attempt, nothing more may be sent or written. The run returns to {@code RETRY_WAIT}
 * through the lease reap, which counts {@code lease_lost_count}.
 */
public final class LeaseLostException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final LeaseLostReason reason;

    public LeaseLostException(LeaseLostReason reason) {
        super("lease lost: " + Objects.requireNonNull(reason, "reason").metricTag(), null, false, false);
        this.reason = reason;
    }

    /** Why the lease was lost (metric tag of {@code scheduler_lease_lost_total}). */
    public LeaseLostReason reason() {
        return reason;
    }
}
