package com.moveinsync.scheduler.app.error;

import org.springframework.http.HttpStatus;

/**
 * 404. Also used for resources of another tenant, so a response never reveals that they exist
 * (docs/design/architecture.md section 13).
 */
public final class NotFoundException extends SchedulerException {

    public NotFoundException(String code, String safeMessage) {
        super(code, safeMessage, null);
    }

    public static NotFoundException job() {
        return new NotFoundException(ErrorCodes.JOB_NOT_FOUND, "Job not found");
    }

    public static NotFoundException tenant() {
        return new NotFoundException(ErrorCodes.TENANT_NOT_FOUND, "Tenant not found");
    }

    public static NotFoundException deadLetter() {
        return new NotFoundException(ErrorCodes.DEAD_LETTER_NOT_FOUND, "Dead letter not found");
    }

    @Override
    public HttpStatus status() {
        return HttpStatus.NOT_FOUND;
    }
}
