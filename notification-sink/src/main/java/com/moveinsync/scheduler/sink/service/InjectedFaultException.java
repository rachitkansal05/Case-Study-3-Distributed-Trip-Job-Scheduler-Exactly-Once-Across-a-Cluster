package com.moveinsync.scheduler.sink.service;

/** A failure injected by {@link FaultInjector}; nothing was written. */
public class InjectedFaultException extends RuntimeException {

    private final int status;

    public InjectedFaultException(int status) {
        super("injected fault", null, false, false);
        this.status = status;
    }

    /** HTTP status to answer with (500 or 503). */
    public int status() {
        return status;
    }
}
