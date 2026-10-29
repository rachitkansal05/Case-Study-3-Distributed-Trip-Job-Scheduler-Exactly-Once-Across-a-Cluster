package com.moveinsync.scheduler.core.policy;

import com.moveinsync.scheduler.core.domain.RunFailure;
import java.util.Objects;
import java.util.Optional;

/**
 * Decides what happens after a run attempt ends badly (plan section 09, requirements R4 and R8).
 *
 * <p><b>Handler failure</b> ({@link #onFailure}): counts toward {@code max_attempts}.
 *
 * <ul>
 *   <li>Permanent ({@link PermanentJobException}) → {@link RunFailure.DeadLetter} immediately, even
 *       on attempt 1.
 *   <li>Retryable and {@code attempt < maxAttempts} → {@link RunFailure.Retry} with {@code
 *       backoff.delayFor(attempt)}.
 *   <li>Retryable and {@code attempt >= maxAttempts} → {@link RunFailure.DeadLetter}.
 * </ul>
 *
 * <p><b>Lease loss</b> ({@link #onLeaseLost}): a stale-token 409, a fenced write that updated 0
 * rows, or a self-fence. It is not a job failure and does <b>not</b> use up an attempt; it has
 * its own limit ({@value #DEFAULT_LEASE_LOST_LIMIT} by default). Below the limit nothing is
 * written by this node: the run returns to {@code RETRY_WAIT} through the lease reap. When the
 * count reaches the limit (the 20th loss by default), the run is dead-lettered with {@value
 * #FENCING_CONFLICT} so a fencing bug cannot loop forever.
 *
 * <p>Immutable; thread-safe if the backoff policy is.
 */
public final class RetryDecider {

    /** Default number of lease losses after which a run is dead-lettered. */
    public static final int DEFAULT_LEASE_LOST_LIMIT = 20;

    /** Error code of the dead letter written when the lease-lost limit is reached. */
    public static final String FENCING_CONFLICT = "FENCING_CONFLICT";

    private final BackoffPolicy backoff;
    private final FailureClassifier classifier;
    private final int leaseLostLimit;

    /**
     * @param backoff delay policy for retries
     * @param classifier failure classifier
     * @param leaseLostLimit lease losses that trigger the {@value #FENCING_CONFLICT} dead letter,
     *     {@code >= 1}
     */
    public RetryDecider(BackoffPolicy backoff, FailureClassifier classifier, int leaseLostLimit) {
        this.backoff = Objects.requireNonNull(backoff, "backoff");
        this.classifier = Objects.requireNonNull(classifier, "classifier");
        if (leaseLostLimit < 1) {
            throw new IllegalArgumentException("leaseLostLimit must be >= 1");
        }
        this.leaseLostLimit = leaseLostLimit;
    }

    /** Uses {@link #DEFAULT_LEASE_LOST_LIMIT}. */
    public RetryDecider(BackoffPolicy backoff, FailureClassifier classifier) {
        this(backoff, classifier, DEFAULT_LEASE_LOST_LIMIT);
    }

    /**
     * Outcome of a failed attempt.
     *
     * @param failure what the handler threw
     * @param attempt the 1-based attempt that failed, {@code >= 1}
     * @param maxAttempts {@code job.max_attempts}, {@code >= 1}
     * @throws IllegalArgumentException if {@code attempt} or {@code maxAttempts} is below 1
     */
    public RunFailure onFailure(Throwable failure, int attempt, int maxAttempts) {
        Objects.requireNonNull(failure, "failure");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        String code = classifier.errorCodeOf(failure);
        String message = classifier.safeMessageOf(failure);
        if (classifier.classify(failure) == FailureKind.RETRYABLE && attempt < maxAttempts) {
            return new RunFailure.Retry(backoff.delayFor(attempt), code, message);
        }
        return new RunFailure.DeadLetter(code, message);
    }

    /**
     * Outcome of a lease loss.
     *
     * @param leaseLostCount lease losses of this run so far, including this one, {@code >= 0}
     * @return a {@value #FENCING_CONFLICT} dead letter once {@code leaseLostCount >=
     *     leaseLostLimit}, else empty (the reap moves the run back to {@code RETRY_WAIT})
     */
    public Optional<RunFailure.DeadLetter> onLeaseLost(int leaseLostCount) {
        if (leaseLostCount < 0) {
            throw new IllegalArgumentException("leaseLostCount must be >= 0");
        }
        if (leaseLostCount < leaseLostLimit) {
            return Optional.empty();
        }
        return Optional.of(new RunFailure.DeadLetter(
                FENCING_CONFLICT, "lease lost " + leaseLostCount + " times; fencing conflict suspected"));
    }

    /** The configured lease-lost limit. */
    public int leaseLostLimit() {
        return leaseLostLimit;
    }
}
