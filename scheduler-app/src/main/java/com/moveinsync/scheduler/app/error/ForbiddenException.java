package com.moveinsync.scheduler.app.error;

import org.springframework.http.HttpStatus;

/**
 * 403: the caller's role may not perform the action. Never used for another tenant's resources,
 * which are reported as {@link NotFoundException}.
 */
public final class ForbiddenException extends SchedulerException {

    public ForbiddenException(String safeMessage) {
        super(ErrorCodes.FORBIDDEN, safeMessage, null);
    }

    @Override
    public HttpStatus status() {
        return HttpStatus.FORBIDDEN;
    }
}
