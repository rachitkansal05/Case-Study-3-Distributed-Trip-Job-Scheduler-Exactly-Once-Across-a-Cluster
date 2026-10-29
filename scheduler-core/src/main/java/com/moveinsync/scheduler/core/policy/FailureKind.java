package com.moveinsync.scheduler.core.policy;

/** How a handler failure is treated by {@link RetryDecider}; produced by {@link FailureClassifier}. */
public enum FailureKind {
    /** Retry with backoff while attempts are left. */
    RETRYABLE,
    /** Dead-letter immediately. */
    PERMANENT
}
