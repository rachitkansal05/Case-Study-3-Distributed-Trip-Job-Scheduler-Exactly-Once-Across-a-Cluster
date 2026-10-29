package com.moveinsync.scheduler.core.policy;

import java.time.Duration;

/**
 * Delay before the next attempt of a failed run (plan section 09, requirement R8). A retry sets
 * {@code state = RETRY_WAIT, due_at = now() + delayFor(attempt)}.
 *
 * <p>Implementations must be thread-safe: one instance is shared by all executor threads.
 */
public interface BackoffPolicy {

    /**
     * Delay to wait after attempt number {@code attempt} failed.
     *
     * @param attempt the 1-based attempt that just failed, {@code >= 1}
     * @return a non-negative delay
     * @throws IllegalArgumentException if {@code attempt < 1}
     */
    Duration delayFor(int attempt);
}
