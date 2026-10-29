package com.moveinsync.scheduler.core.policy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.core.domain.RunFailure;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/** Plan section 09: retry vs permanent, last attempt dead-letters, no PII in messages. */
class RetryDeciderTest {

    private static final BackoffPolicy FIXED = attempt -> Duration.ofSeconds(attempt * 10L);
    private final FailureClassifier classifier = new FailureClassifier();
    private final RetryDecider decider = new RetryDecider(FIXED, classifier);

    @Test
    void retryableWithAttemptsLeftRetriesWithBackoff() {
        RunFailure f = decider.onFailure(new RetryableJobException("TIMEOUT", "handler timed out"), 2, 5);
        assertThat(f).isEqualTo(new RunFailure.Retry(Duration.ofSeconds(20), "TIMEOUT", "handler timed out"));
    }

    @Test
    void lastAttemptDeadLetters() {
        RunFailure f = decider.onFailure(new RetryableJobException("UPSTREAM_5XX", "sink returned 503"), 5, 5);
        assertThat(f).isEqualTo(new RunFailure.DeadLetter("UPSTREAM_5XX", "sink returned 503"));
        assertThat(decider.onFailure(new RetryableJobException("TIMEOUT", "t"), 7, 5))
                .isInstanceOf(RunFailure.DeadLetter.class);
        assertThat(decider.onFailure(new RetryableJobException("TIMEOUT", "t"), 1, 1))
                .isInstanceOf(RunFailure.DeadLetter.class);
    }

    @Test
    void permanentDeadLettersImmediately() {
        RunFailure f = decider.onFailure(new PermanentJobException("INVALID_PAYLOAD", "payload rejected"), 1, 5);
        assertThat(f).isEqualTo(new RunFailure.DeadLetter("INVALID_PAYLOAD", "payload rejected"));
    }

    @Test
    void unknownExceptionIsRetryableAndNeverLeaksItsMessage() {
        String pii = "employee phone +91-98765-43210";
        RunFailure f = decider.onFailure(new IllegalStateException(pii), 1, 5);
        assertThat(f).isInstanceOf(RunFailure.Retry.class);
        assertThat(f.errorCode()).isEqualTo(FailureClassifier.UNEXPECTED_ERROR);
        assertThat(f.safeMessage()).isEqualTo("IllegalStateException").doesNotContain(pii);

        RunFailure last = decider.onFailure(new IOException(pii), 5, 5);
        assertThat(last).isInstanceOf(RunFailure.DeadLetter.class);
        assertThat(last.safeMessage()).isEqualTo("IOException").doesNotContain(pii);
    }

    @Test
    void classifierUnwrapsCommonWrappers() {
        PermanentJobException inner = new PermanentJobException("INVALID_PAYLOAD", "bad");
        assertThat(classifier.classify(new ExecutionException(inner))).isEqualTo(FailureKind.PERMANENT);
        assertThat(classifier.classify(new CompletionException(new ExecutionException(inner))))
                .isEqualTo(FailureKind.PERMANENT);
        assertThat(classifier.errorCodeOf(new InvocationTargetException(inner))).isEqualTo("INVALID_PAYLOAD");
        assertThat(classifier.safeMessageOf(new CompletionException("secret", new TimeoutException("secret2"))))
                .isEqualTo("TimeoutException");
        assertThat(classifier.classify(new TimeoutException("x"))).isEqualTo(FailureKind.RETRYABLE);
        assertThat(classifier.classify(new RetryableJobException("TIMEOUT", "t"))).isEqualTo(FailureKind.RETRYABLE);
        // A wrapper without a cause is classified as itself.
        assertThat(classifier.safeMessageOf(new ExecutionException("secret", null))).isEqualTo("ExecutionException");
        // Wrapped job exception keeps its own code even when the cause chain is deep.
        Throwable deep = inner;
        for (int i = 0; i < 10; i++) {
            deep = new CompletionException(deep);
        }
        assertThat(classifier.errorCodeOf(deep)).isEqualTo("INVALID_PAYLOAD");
    }

    @Test
    void anonymousClassFallsBackToClassName() {
        RuntimeException anon = new RuntimeException("secret") {};
        assertThat(classifier.safeMessageOf(anon)).doesNotContain("secret").isNotBlank();
    }

    @Test
    void exceptionsValidateErrorCode() {
        assertThatThrownBy(() -> new RetryableJobException("timeout", "m")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryableJobException("", "m")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PermanentJobException("1BAD", "m")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PermanentJobException("BAD CODE", "m"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining("BAD CODE");
        assertThatThrownBy(() -> new PermanentJobException(null, "m")).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new PermanentJobException("X", null)).isInstanceOf(NullPointerException.class);
        IOException cause = new IOException();
        RetryableJobException r = new RetryableJobException("UPSTREAM_5XX", "upstream failed", cause);
        assertThat(r.errorCode()).isEqualTo("UPSTREAM_5XX");
        assertThat(r.getMessage()).isEqualTo("upstream failed");
        assertThat(r.getCause()).isSameAs(cause);
    }

    @Test
    void argumentValidation() {
        RetryableJobException e = new RetryableJobException("TIMEOUT", "t");
        assertThatThrownBy(() -> decider.onFailure(e, 0, 5)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> decider.onFailure(e, 1, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> decider.onFailure(null, 1, 5)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new RetryDecider(FIXED, classifier, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryDecider(null, classifier)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new RetryDecider(FIXED, null)).isInstanceOf(NullPointerException.class);
        assertThat(decider.leaseLostLimit()).isEqualTo(RetryDecider.DEFAULT_LEASE_LOST_LIMIT);
    }

    @Test
    void realBackoffDelayIsWithinCeiling() {
        RetryDecider real = new RetryDecider(ExponentialFullJitterBackoff.withDefaults(), classifier);
        RunFailure f = real.onFailure(new RetryableJobException("TIMEOUT", "t"), 1, 5);
        assertThat(((RunFailure.Retry) f).delay()).isBetween(Duration.ZERO, Duration.ofSeconds(4));
    }
}
