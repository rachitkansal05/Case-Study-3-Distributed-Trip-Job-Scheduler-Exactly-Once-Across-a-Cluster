package com.moveinsync.scheduler.core.domain;

/** Business handler a job runs (column {@code job.handler}). */
public enum HandlerType {
    /** "Cab assigned" alerts sent before pickup; chunked (50 per chunk) with checkpoints. */
    PICKUP_ALERT,
    /** Nightly roster regeneration. */
    ROSTER_GEN,
    /** Cancels a trip when the employee does not show up. */
    NO_SHOW_CANCEL,
    /** Closes a billing period. */
    BILLING_CLOSE
}
