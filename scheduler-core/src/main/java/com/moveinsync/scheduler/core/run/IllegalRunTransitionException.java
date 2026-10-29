package com.moveinsync.scheduler.core.run;

import com.moveinsync.scheduler.core.domain.RunState;
import java.util.Objects;

/** A requested {@link RunState} change that {@link RunStateMachine} does not allow. */
public final class IllegalRunTransitionException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    private final RunState from;
    private final RunState to;

    /**
     * @param from current state
     * @param to requested state
     */
    public IllegalRunTransitionException(RunState from, RunState to) {
        super("illegal run transition: " + Objects.requireNonNull(from, "from") + " -> "
                + Objects.requireNonNull(to, "to"));
        this.from = from;
        this.to = to;
    }

    /** Current state. */
    public RunState from() {
        return from;
    }

    /** Requested state. */
    public RunState to() {
        return to;
    }
}
