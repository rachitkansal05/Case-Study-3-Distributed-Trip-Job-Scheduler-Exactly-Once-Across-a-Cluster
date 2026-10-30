package com.moveinsync.scheduler.app.error;

import java.util.List;
import org.springframework.http.HttpStatus;

/**
 * 400. Carries the names of the offending fields, never their values (docs/design/architecture.md section 14).
 */
public final class ValidationException extends SchedulerException {

    private final List<FieldIssue> fields;

    public ValidationException(String code, String safeMessage, List<FieldIssue> fields) {
        super(code, safeMessage, null);
        this.fields = List.copyOf(fields);
    }

    /** A single invalid field with a fixed, value-free reason. */
    public static ValidationException field(String field, String reason) {
        return new ValidationException(ErrorCodes.VALIDATION_FAILED, "Request validation failed",
                List.of(new FieldIssue(field, reason)));
    }

    /** Invalid cron expression (field {@code cron}). */
    public static ValidationException invalidCron(String reason) {
        return new ValidationException(ErrorCodes.INVALID_CRON, "Invalid cron expression",
                List.of(new FieldIssue("cron", reason)));
    }

    public List<FieldIssue> fields() {
        return fields;
    }

    @Override
    public HttpStatus status() {
        return HttpStatus.BAD_REQUEST;
    }

    /**
     * @param field  request field, query parameter or header name
     * @param reason fixed description such as "must not be null"; never the rejected value
     */
    public record FieldIssue(String field, String reason) {
    }
}
