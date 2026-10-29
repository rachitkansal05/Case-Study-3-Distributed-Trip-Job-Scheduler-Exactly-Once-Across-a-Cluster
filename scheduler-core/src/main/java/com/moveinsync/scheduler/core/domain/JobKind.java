package com.moveinsync.scheduler.core.domain;

/** Shape of a job definition (column {@code job.kind}). */
public enum JobKind {
    /** Fires once at {@code job.run_at}. */
    ONE_OFF,
    /** Fires on a cron expression evaluated in the tenant's local wall time (plan section 08). */
    RECURRING
}
