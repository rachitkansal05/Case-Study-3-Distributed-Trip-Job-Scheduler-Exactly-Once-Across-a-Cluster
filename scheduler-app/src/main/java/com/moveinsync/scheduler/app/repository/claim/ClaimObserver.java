package com.moveinsync.scheduler.app.repository.claim;

import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.TenantId;
import java.time.Duration;

/**
 * Callbacks from the claim protocol for metrics. Called after the transaction committed (never
 * inside it), on the calling thread; implementations must be cheap and must not throw.
 */
public interface ClaimObserver {

    /** No-op observer. */
    ClaimObserver NONE = new ClaimObserver() {
    };

    /** One {@code claimDue} call finished (committed or rolled back). */
    default void onClaimBatch(TenantId tenant, Duration elapsed, int started) {
    }

    /** Due runs existed but no slot of the tenant was free (cap full or draining). */
    default void onCapRejected(TenantId tenant) {
    }

    /**
     * A run started for the first time.
     *
     * @param fireLag {@code started_at - scheduled_at}, both DB time
     */
    default void onFirstStart(ClaimedRun run, Duration fireLag) {
    }

    /**
     * The orphan sweep took over an expired run.
     *
     * @param sinceLastRenewal time from the dead owner's last successful lease renewal (or claim),
     *     i.e. expired {@code lease_until} minus the run lease, to the restart; DB time. This is
     *     the owner-loss to restart interval the takeover bound (lease + sweep + claim) limits.
     */
    default void onOrphanTakeover(ClaimedRun run, Duration sinceLastRenewal) {
    }

    /** An expired holder was reaped (lease lost by its node). */
    default void onReaped(long runId, boolean deadLettered) {
    }

    /**
     * A run was dead-lettered (committed): by a failing attempt's fenced commit, or by a reap at the
     * lease-lost limit ({@code FENCING_CONFLICT}).
     *
     * @param errorCode the stable error code stored in {@code dead_letter.error_code}
     */
    default void onDeadLettered(long runId, String errorCode) {
    }
}
