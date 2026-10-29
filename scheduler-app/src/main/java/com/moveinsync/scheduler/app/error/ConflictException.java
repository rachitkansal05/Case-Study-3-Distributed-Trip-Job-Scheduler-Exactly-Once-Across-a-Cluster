package com.moveinsync.scheduler.app.error;

import org.springframework.http.HttpStatus;

/** 409: the request is valid but clashes with the current state (or a reused idempotency key). */
public final class ConflictException extends SchedulerException {

    public ConflictException(String code, String safeMessage) {
        super(code, safeMessage, null);
    }

    @Override
    public HttpStatus status() {
        return HttpStatus.CONFLICT;
    }
}
