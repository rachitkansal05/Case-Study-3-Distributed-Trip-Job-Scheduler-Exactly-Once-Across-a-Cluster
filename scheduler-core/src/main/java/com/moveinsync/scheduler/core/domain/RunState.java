package com.moveinsync.scheduler.core.domain;

/**
 * Lifecycle state of one run (column {@code job_run.state}); see the state diagram in plan
 * section 04. Only the claim protocol writes these transitions.
 */
public enum RunState {
    /** Materialised and waiting for its due time. */
    SCHEDULED,
    /** Held back because the tenant is paused; resumes to {@link #SCHEDULED}. */
    PAUSED,
    /** Claimed: holds a tenant slot, a fencing token and a lease. */
    RUNNING,
    /** Failed with attempts left, or its lease expired; due again at {@code due_at}. */
    RETRY_WAIT,
    /** Committed with a matching token and a valid lease. */
    SUCCEEDED,
    /** Dropped by the misfire policy (SKIP, or absorbed by COALESCE). */
    SKIPPED,
    /** Attempts (or lease-lost attempts) exhausted, or a permanent error; see {@code dead_letter}. */
    DEAD_LETTERED,
    /** Unstarted run of a job that was disabled. */
    CANCELLED;

    /** True when no further transition happens without operator action. */
    public boolean isTerminal() {
        return this == SUCCEEDED || this == SKIPPED || this == DEAD_LETTERED || this == CANCELLED;
    }

    /** True for states the claim may pick up ({@code SCHEDULED}, {@code RETRY_WAIT}). */
    public boolean isClaimable() {
        return this == SCHEDULED || this == RETRY_WAIT;
    }
}
