package com.moveinsync.scheduler.core.time;

/**
 * Thrown when a cron expression cannot be parsed by {@link CronSchedule#parse(String)}. The message
 * names the offending field and at most a short, truncated token; it never echoes the whole
 * expression, so it is safe to return to API clients and to log.
 */
public final class InvalidCronException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    /**
     * @param message safe, short description of the problem
     */
    public InvalidCronException(String message) {
        super(message);
    }
}
