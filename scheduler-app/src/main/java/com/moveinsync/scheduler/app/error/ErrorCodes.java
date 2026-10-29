package com.moveinsync.scheduler.app.error;

/** Stable error codes returned in the {@code code} property of every problem response. */
public final class ErrorCodes {

    public static final String JOB_NOT_FOUND = "JOB_NOT_FOUND";
    public static final String TENANT_NOT_FOUND = "TENANT_NOT_FOUND";
    public static final String DEAD_LETTER_NOT_FOUND = "DEAD_LETTER_NOT_FOUND";
    public static final String NOT_FOUND = "NOT_FOUND";

    public static final String VALIDATION_FAILED = "VALIDATION_FAILED";
    public static final String INVALID_CRON = "INVALID_CRON";
    public static final String MALFORMED_REQUEST = "MALFORMED_REQUEST";
    public static final String IDEMPOTENCY_KEY_REQUIRED = "IDEMPOTENCY_KEY_REQUIRED";

    public static final String IDEMPOTENCY_KEY_REUSED = "IDEMPOTENCY_KEY_REUSED";
    public static final String TENANT_EXISTS = "TENANT_EXISTS";
    public static final String JOB_DISABLED = "JOB_DISABLED";
    public static final String DEAD_LETTER_ALREADY_REPLAYED = "DEAD_LETTER_ALREADY_REPLAYED";
    public static final String CONCURRENT_MODIFICATION = "CONCURRENT_MODIFICATION";

    public static final String UNAUTHORIZED = "UNAUTHORIZED";
    public static final String FORBIDDEN = "FORBIDDEN";
    public static final String METHOD_NOT_ALLOWED = "METHOD_NOT_ALLOWED";
    public static final String UNSUPPORTED_MEDIA_TYPE = "UNSUPPORTED_MEDIA_TYPE";
    public static final String NOT_ACCEPTABLE = "NOT_ACCEPTABLE";

    public static final String SCHEDULER_STORE_UNAVAILABLE = "SCHEDULER_STORE_UNAVAILABLE";
    public static final String INTERNAL_ERROR = "INTERNAL_ERROR";

    private ErrorCodes() {
    }
}
