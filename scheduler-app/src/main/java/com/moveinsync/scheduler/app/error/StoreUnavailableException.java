package com.moveinsync.scheduler.app.error;

import org.springframework.http.HttpStatus;

/**
 * 503 {@code SCHEDULER_STORE_UNAVAILABLE}: the database cannot be reached. The scheduler chooses
 * consistency over availability (docs/design/architecture.md section 12), so the client should retry with the same
 * {@code Idempotency-Key}.
 */
public final class StoreUnavailableException extends SchedulerException {

    public StoreUnavailableException(Throwable cause) {
        super(ErrorCodes.SCHEDULER_STORE_UNAVAILABLE, "Scheduler store is unavailable; retry later", cause);
    }

    @Override
    public HttpStatus status() {
        return HttpStatus.SERVICE_UNAVAILABLE;
    }
}
