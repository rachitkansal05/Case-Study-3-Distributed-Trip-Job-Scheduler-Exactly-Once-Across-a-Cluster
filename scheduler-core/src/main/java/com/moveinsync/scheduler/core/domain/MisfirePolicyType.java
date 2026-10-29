package com.moveinsync.scheduler.core.domain;

/**
 * Misfire policy selector (column {@code job.misfire}); behaviour is in plan section 09. The
 * algorithms live in {@code core.policy} (Core team).
 */
public enum MisfirePolicyType {
    /** Every missed run fires immediately, in order, each with its own key. */
    FIRE_NOW,
    /** Missed runs become {@code SKIPPED}; the next future occurrence runs normally. */
    SKIP,
    /** The newest missed run fires once with {@code coalesced_count = n}; the others are skipped. */
    COALESCE
}
