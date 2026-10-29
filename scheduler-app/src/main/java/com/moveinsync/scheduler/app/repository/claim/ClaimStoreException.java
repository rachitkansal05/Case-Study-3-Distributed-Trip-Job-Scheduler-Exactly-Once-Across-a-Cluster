package com.moveinsync.scheduler.app.repository.claim;

/**
 * The run store could not be used (database unreachable, statement failed). Nothing that the
 * failing transaction wrote is visible; in particular no token from it was ever handed out.
 */
public class ClaimStoreException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String sqlState;

    public ClaimStoreException(String message, java.sql.SQLException cause) {
        super(message + " (SQLSTATE " + cause.getSQLState() + ")", cause);
        this.sqlState = cause.getSQLState();
    }

    public ClaimStoreException(String message) {
        super(message);
        this.sqlState = null;
    }

    /** SQLSTATE of the cause, or {@code null}. */
    public String sqlState() {
        return sqlState;
    }
}
