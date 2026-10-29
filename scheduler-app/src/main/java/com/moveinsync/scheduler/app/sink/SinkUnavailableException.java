package com.moveinsync.scheduler.app.sink;

/**
 * The sink's highest accepted epoch could not be read (non-200, malformed answer or I/O error).
 * The startup epoch guard must fail closed on it and never assume {@code 0}.
 */
public class SinkUnavailableException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * @param message message without request or payload values
     * @param cause underlying cause, may be null
     */
    public SinkUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
