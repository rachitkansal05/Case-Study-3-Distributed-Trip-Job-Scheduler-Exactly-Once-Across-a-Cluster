package com.moveinsync.scheduler.core.policy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Classifies a handler failure by type (plan section 09).
 *
 * <ul>
 *   <li>{@link PermanentJobException} → {@link FailureKind#PERMANENT}, its own code and safe
 *       message.
 *   <li>{@link RetryableJobException} → {@link FailureKind#RETRYABLE}, its own code and safe
 *       message.
 *   <li>Anything else (timeouts, I/O, unexpected bugs) → {@link FailureKind#RETRYABLE} with code
 *       {@value #UNEXPECTED_ERROR}. Its safe message is only the exception's class simple name:
 *       {@code getMessage()} of an unknown exception is <b>never</b> used, because it may contain
 *       payload data or other personal data.
 * </ul>
 *
 * <p>Before classifying, transparent wrappers ({@link ExecutionException}, {@link
 * CompletionException}, {@link InvocationTargetException}, {@link UndeclaredThrowableException})
 * are unwrapped to their cause, up to {@value #MAX_UNWRAP_DEPTH} levels. Stateless and
 * thread-safe.
 */
public final class FailureClassifier {

    /** Error code for any failure that is not a {@code RetryableJobException}/{@code PermanentJobException}. */
    public static final String UNEXPECTED_ERROR = "UNEXPECTED_ERROR";

    /** Maximum number of wrapper levels removed by {@link #unwrap(Throwable)}. */
    public static final int MAX_UNWRAP_DEPTH = 16;

    /** Creates a classifier; it has no configuration. */
    public FailureClassifier() {}

    /** Retryable or permanent; see the class comment. */
    public FailureKind classify(Throwable failure) {
        return unwrap(failure) instanceof PermanentJobException
                ? FailureKind.PERMANENT
                : FailureKind.RETRYABLE;
    }

    /** Stable error code: the job exception's own code, else {@value #UNEXPECTED_ERROR}. */
    public String errorCodeOf(Throwable failure) {
        Throwable t = unwrap(failure);
        if (t instanceof PermanentJobException p) {
            return p.errorCode();
        }
        if (t instanceof RetryableJobException r) {
            return r.errorCode();
        }
        return UNEXPECTED_ERROR;
    }

    /**
     * Message safe to persist: the job exception's safe message, else only the simple class name of
     * the (unwrapped) exception, never its {@code getMessage()}.
     */
    public String safeMessageOf(Throwable failure) {
        Throwable t = unwrap(failure);
        if (t instanceof PermanentJobException p) {
            return p.safeMessage();
        }
        if (t instanceof RetryableJobException r) {
            return r.safeMessage();
        }
        String simple = t.getClass().getSimpleName();
        return simple.isEmpty() ? t.getClass().getName() : simple;
    }

    /**
     * Removes transparent wrapper exceptions (see the class comment) while they have a cause.
     *
     * @return the innermost non-wrapper throwable, or the last wrapper reached at the depth limit
     */
    public Throwable unwrap(Throwable failure) {
        Throwable t = Objects.requireNonNull(failure, "failure");
        for (int depth = 0; depth < MAX_UNWRAP_DEPTH && isWrapper(t) && t.getCause() != null; depth++) {
            t = t.getCause();
        }
        return t;
    }

    private static boolean isWrapper(Throwable t) {
        return t instanceof ExecutionException
                || t instanceof CompletionException
                || t instanceof InvocationTargetException
                || t instanceof UndeclaredThrowableException;
    }
}
