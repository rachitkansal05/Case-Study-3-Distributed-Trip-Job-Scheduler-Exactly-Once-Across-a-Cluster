package com.moveinsync.scheduler.core.domain;

/** Result of sending one effect to the sink (plan sections 07 and 12). */
public enum DeliveryOutcome {
    /** First delivery for this idempotency key; the effect happened now. */
    DELIVERED,
    /** The key was delivered before (legitimate retry); treat as success. */
    ALREADY_DELIVERED,
    /**
     * The sink has seen a higher token for this run (HTTP 409). Lease lost, not a job failure:
     * stop the handler, do not count the attempt, record {@link LeaseLostReason#STALE_TOKEN}.
     */
    STALE_TOKEN;

    /** True when the effect is known to have happened exactly once. */
    public boolean isSuccess() {
        return this != STALE_TOKEN;
    }
}
