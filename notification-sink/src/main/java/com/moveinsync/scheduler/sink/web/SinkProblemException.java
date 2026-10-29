package com.moveinsync.scheduler.sink.web;

import java.util.List;
import java.util.Objects;
import org.springframework.http.HttpStatus;

/**
 * An expected, client-visible failure, rendered as a ProblemDetail by {@link ApiExceptionHandler}.
 * The detail and field messages are fixed texts written by us: they never contain request values.
 */
public class SinkProblemException extends RuntimeException {

    private final ProblemCode code;
    private final HttpStatus status;
    private final List<FieldViolation> violations;

    public SinkProblemException(ProblemCode code, String detail) {
        this(code, code.status(), detail, List.of());
    }

    public SinkProblemException(ProblemCode code, HttpStatus status, String detail, List<FieldViolation> violations) {
        super(detail, null, false, false);
        this.code = Objects.requireNonNull(code, "code");
        this.status = Objects.requireNonNull(status, "status");
        this.violations = List.copyOf(violations);
    }

    public static SinkProblemException validation(List<FieldViolation> violations) {
        return new SinkProblemException(ProblemCode.VALIDATION_FAILED, ProblemCode.VALIDATION_FAILED.status(),
                "The request body is invalid.", violations);
    }

    public ProblemCode code() {
        return code;
    }

    public HttpStatus status() {
        return status;
    }

    public List<FieldViolation> violations() {
        return violations;
    }

    /**
     * One invalid field.
     *
     * @param field JSON field or parameter name
     * @param message fixed description of the rule that failed (never the rejected value)
     */
    public record FieldViolation(String field, String message) {
    }
}
