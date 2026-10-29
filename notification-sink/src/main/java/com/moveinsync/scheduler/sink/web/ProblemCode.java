package com.moveinsync.scheduler.sink.web;

import org.springframework.http.HttpStatus;

/** Stable machine-readable error codes, sent as the {@code code} property of every ProblemDetail. */
public enum ProblemCode {
    STALE_TOKEN(HttpStatus.CONFLICT, "Stale fencing token"),
    MISSING_IDEMPOTENCY_KEY(HttpStatus.BAD_REQUEST, "Missing Idempotency-Key header"),
    INVALID_IDEMPOTENCY_KEY(HttpStatus.BAD_REQUEST, "Invalid Idempotency-Key header"),
    MISSING_FENCING_TOKEN(HttpStatus.BAD_REQUEST, "Missing Fencing-Token header"),
    INVALID_FENCING_TOKEN(HttpStatus.BAD_REQUEST, "Invalid Fencing-Token header"),
    VALIDATION_FAILED(HttpStatus.BAD_REQUEST, "Validation failed"),
    MALFORMED_BODY(HttpStatus.BAD_REQUEST, "Malformed request body"),
    ADMIN_UNAUTHORIZED(HttpStatus.UNAUTHORIZED, "Admin token required"),
    NOT_FOUND(HttpStatus.NOT_FOUND, "Not found"),
    METHOD_NOT_ALLOWED(HttpStatus.METHOD_NOT_ALLOWED, "Method not allowed"),
    UNSUPPORTED_MEDIA_TYPE(HttpStatus.UNSUPPORTED_MEDIA_TYPE, "Unsupported media type"),
    NOT_ACCEPTABLE(HttpStatus.NOT_ACCEPTABLE, "Not acceptable"),
    INJECTED_FAULT(HttpStatus.SERVICE_UNAVAILABLE, "Injected fault"),
    STORE_UNAVAILABLE(HttpStatus.SERVICE_UNAVAILABLE, "Sink store unavailable"),
    INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error");

    private final HttpStatus status;
    private final String title;

    ProblemCode(HttpStatus status, String title) {
        this.status = status;
        this.title = title;
    }

    /** Default HTTP status; {@link #INJECTED_FAULT} may be sent with 500 instead. */
    public HttpStatus status() {
        return status;
    }

    public String title() {
        return title;
    }
}
