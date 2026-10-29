package com.moveinsync.scheduler.core.policy;

/**
 * Thrown by a handler for a failure that retrying cannot fix (bad payload, upstream 4xx). The run
 * is dead-lettered immediately, whatever attempts are left (plan section 09).
 *
 * <p>{@link #errorCode()} is a stable code matching {@code [A-Z][A-Z0-9_]*} (e.g. {@code
 * INVALID_PAYLOAD}); it is stored and shown to operators. The message is the <b>safe message</b>:
 * it is persisted and must never contain payload values or other personal data.
 */
public class PermanentJobException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String errorCode;
    private final String safeMessage;

    /**
     * @param errorCode stable code, {@code [A-Z][A-Z0-9_]*}
     * @param safeMessage message free of payload data
     * @throws IllegalArgumentException if {@code errorCode} is malformed
     */
    public PermanentJobException(String errorCode, String safeMessage) {
        this(errorCode, safeMessage, null);
    }

    /**
     * @param errorCode stable code, {@code [A-Z][A-Z0-9_]*}
     * @param safeMessage message free of payload data
     * @param cause underlying cause (kept for local debugging; never persisted), may be null
     * @throws IllegalArgumentException if {@code errorCode} is malformed
     */
    public PermanentJobException(String errorCode, String safeMessage, Throwable cause) {
        super(ErrorCodes.requireSafeMessage(safeMessage), cause);
        this.errorCode = ErrorCodes.requireCode(errorCode);
        this.safeMessage = safeMessage;
    }

    /** Stable error code. */
    public String errorCode() {
        return errorCode;
    }

    /** Message free of payload data (same as the constructor argument). */
    public String safeMessage() {
        return safeMessage;
    }
}
