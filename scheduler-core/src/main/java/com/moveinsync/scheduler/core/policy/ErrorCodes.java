package com.moveinsync.scheduler.core.policy;

import java.util.Objects;
import java.util.regex.Pattern;

/** Validation shared by the job exceptions; package-private. */
final class ErrorCodes {

    /** Stable error code syntax: upper-case letters, digits and underscores, starting with a letter. */
    static final Pattern CODE = Pattern.compile("[A-Z][A-Z0-9_]*");

    private ErrorCodes() {}

    static String requireCode(String errorCode) {
        Objects.requireNonNull(errorCode, "errorCode");
        if (!CODE.matcher(errorCode).matches()) {
            // Never echo the value: a caller might pass something it should not have.
            throw new IllegalArgumentException("errorCode must match [A-Z][A-Z0-9_]*");
        }
        return errorCode;
    }

    static String requireSafeMessage(String safeMessage) {
        return Objects.requireNonNull(safeMessage, "safeMessage");
    }
}
