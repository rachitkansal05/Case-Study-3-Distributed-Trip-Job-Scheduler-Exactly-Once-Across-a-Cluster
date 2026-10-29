package com.moveinsync.scheduler.core.domain;

/**
 * Why a node lost the right to act for a run. A lease loss is not a job failure: it does not count
 * toward {@code max_attempts} but toward the separate lease-lost limit (default 20, then
 * {@code FENCING_CONFLICT} dead letter). Used as the {@code reason} tag of
 * {@code scheduler_lease_lost_total}.
 */
public enum LeaseLostReason {
    /** The sink answered 409: it has already seen a higher token for this run. */
    STALE_TOKEN("stale_409"),
    /** A fenced renew, checkpoint or commit updated 0 rows. */
    FENCED_WRITE_REJECTED("fenced_write_0_rows"),
    /** The local monotonic deadline ({@code claimTime + lease - margin}) passed before a send. */
    SELF_FENCE("self_fence");

    private final String metricTag;

    LeaseLostReason(String metricTag) {
        this.metricTag = metricTag;
    }

    /** Stable, low-cardinality metric tag value. */
    public String metricTag() {
        return metricTag;
    }
}
