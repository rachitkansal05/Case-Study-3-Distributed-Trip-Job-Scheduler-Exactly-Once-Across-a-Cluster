package com.moveinsync.scheduler.core.domain;

import java.time.Duration;
import java.util.Objects;

/**
 * Decided outcome of a failed attempt, passed to {@code ClaimProtocol.commitFailure}. The caller
 * decides (error classification, backoff, {@code max_attempts}); the protocol only applies it with
 * the fenced predicate.
 *
 * <p>A lease loss is not a {@code RunFailure}: a node that lost its lease cannot write the run.
 * {@code errorCode} is a stable code (e.g. {@code TIMEOUT}, {@code INVALID_PAYLOAD}); {@code
 * safeMessage} must never contain payload values or other personal data.
 */
public sealed interface RunFailure {

    String errorCode();

    String safeMessage();

    /** Retry later: {@code state = RETRY_WAIT, due_at = clock_timestamp() + delay}. */
    record Retry(Duration delay, String errorCode, String safeMessage) implements RunFailure {
        public Retry {
            Objects.requireNonNull(delay, "delay");
            Objects.requireNonNull(errorCode, "errorCode");
            if (delay.isNegative()) {
                throw new IllegalArgumentException("delay must be >= 0");
            }
        }
    }

    /** Give up: {@code state = DEAD_LETTERED} and a {@code dead_letter} row, in one transaction. */
    record DeadLetter(String errorCode, String safeMessage) implements RunFailure {
        public DeadLetter {
            Objects.requireNonNull(errorCode, "errorCode");
        }
    }
}
