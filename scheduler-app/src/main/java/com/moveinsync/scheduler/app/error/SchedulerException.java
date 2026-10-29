package com.moveinsync.scheduler.app.error;

import java.util.Objects;
import org.springframework.http.HttpStatus;

/**
 * Root of the API's typed exception hierarchy (PLAN.md section 14). Each subclass fixes the HTTP
 * status; the {@link #code()} is a stable, machine-readable identifier and the message is safe to
 * show to a client: it never contains payload values, SQL or stack traces.
 */
public abstract sealed class SchedulerException extends RuntimeException
        permits NotFoundException, ValidationException, ConflictException, StoreUnavailableException,
                ForbiddenException {

    private final String code;

    protected SchedulerException(String code, String safeMessage, Throwable cause) {
        super(Objects.requireNonNull(safeMessage, "safeMessage"), cause);
        this.code = Objects.requireNonNull(code, "code");
    }

    /** Stable error code, e.g. {@code JOB_NOT_FOUND}. */
    public String code() {
        return code;
    }

    /** HTTP status the error maps to. */
    public abstract HttpStatus status();
}
