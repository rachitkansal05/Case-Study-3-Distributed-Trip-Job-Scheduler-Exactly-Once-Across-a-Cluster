package com.moveinsync.scheduler.core.run;

import static com.moveinsync.scheduler.core.domain.RunState.CANCELLED;
import static com.moveinsync.scheduler.core.domain.RunState.DEAD_LETTERED;
import static com.moveinsync.scheduler.core.domain.RunState.PAUSED;
import static com.moveinsync.scheduler.core.domain.RunState.RETRY_WAIT;
import static com.moveinsync.scheduler.core.domain.RunState.RUNNING;
import static com.moveinsync.scheduler.core.domain.RunState.SCHEDULED;
import static com.moveinsync.scheduler.core.domain.RunState.SKIPPED;
import static com.moveinsync.scheduler.core.domain.RunState.SUCCEEDED;

import com.moveinsync.scheduler.core.domain.RunState;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Legal {@link RunState} transitions of one run (plan section 04). Anything not listed is
 * illegal, including self-transitions.
 *
 * <table>
 *   <caption>Legal transitions</caption>
 *   <tr><th>From</th><th>To</th><th>Why</th></tr>
 *   <tr><td>SCHEDULED</td><td>RUNNING</td><td>claim (slot + fencing token + lease)</td></tr>
 *   <tr><td>SCHEDULED</td><td>SKIPPED</td><td>misfire: SKIP, or absorbed by COALESCE</td></tr>
 *   <tr><td>SCHEDULED</td><td>PAUSED</td><td>tenant paused</td></tr>
 *   <tr><td>SCHEDULED</td><td>CANCELLED</td><td>job disabled before the run started</td></tr>
 *   <tr><td>PAUSED</td><td>SCHEDULED</td><td>resume (misfire policy applied afterwards)</td></tr>
 *   <tr><td>PAUSED</td><td>CANCELLED</td><td>job disabled while paused</td></tr>
 *   <tr><td>RUNNING</td><td>SUCCEEDED</td><td>fenced commit: token matches, lease valid</td></tr>
 *   <tr><td>RUNNING</td><td>RETRY_WAIT</td><td>retryable failure with attempts left, or lease
 *       expired and reaped (lease loss, not counted as an attempt)</td></tr>
 *   <tr><td>RUNNING</td><td>DEAD_LETTERED</td><td>attempts exhausted, permanent error, or
 *       {@code FENCING_CONFLICT} (lease-lost limit)</td></tr>
 *   <tr><td>RETRY_WAIT</td><td>RUNNING</td><td>due again, claimed with a new token</td></tr>
 *   <tr><td>RETRY_WAIT</td><td>CANCELLED</td><td>job disabled while waiting to retry</td></tr>
 *   <tr><td>DEAD_LETTERED</td><td>SCHEDULED</td><td>operator replay, same idempotency key</td></tr>
 * </table>
 *
 * <p>{@code SUCCEEDED}, {@code SKIPPED} and {@code CANCELLED} have no exits. {@code
 * DEAD_LETTERED} is terminal (see {@link RunState#isTerminal()}) but an operator may replay it.
 * A {@code RUNNING} run cannot be cancelled directly: it has already started effects, so it is
 * left to finish, fail or lose its lease.
 */
public final class RunStateMachine {

    private static final Map<RunState, Set<RunState>> ALLOWED = buildTable();

    private RunStateMachine() {}

    /** True iff {@code from -> to} is a legal transition. */
    public static boolean isLegal(RunState from, RunState to) {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        return ALLOWED.get(from).contains(to);
    }

    /**
     * Checks a transition.
     *
     * @throws IllegalRunTransitionException if {@code from -> to} is not legal
     */
    public static void requireTransition(RunState from, RunState to) {
        if (!isLegal(from, to)) {
            throw new IllegalRunTransitionException(from, to);
        }
    }

    /** Unmodifiable set of states reachable from {@code from} in one step (may be empty). */
    public static Set<RunState> allowedFrom(RunState from) {
        Objects.requireNonNull(from, "from");
        return ALLOWED.get(from);
    }

    private static Map<RunState, Set<RunState>> buildTable() {
        Map<RunState, Set<RunState>> table = new EnumMap<>(RunState.class);
        for (RunState state : RunState.values()) {
            table.put(state, EnumSet.noneOf(RunState.class));
        }
        table.get(SCHEDULED).addAll(EnumSet.of(RUNNING, SKIPPED, PAUSED, CANCELLED));
        table.get(PAUSED).addAll(EnumSet.of(SCHEDULED, CANCELLED));
        table.get(RUNNING).addAll(EnumSet.of(SUCCEEDED, RETRY_WAIT, DEAD_LETTERED));
        table.get(RETRY_WAIT).addAll(EnumSet.of(RUNNING, CANCELLED));
        table.get(DEAD_LETTERED).add(SCHEDULED);
        Map<RunState, Set<RunState>> frozen = new EnumMap<>(RunState.class);
        table.forEach((k, v) -> frozen.put(k, Collections.unmodifiableSet(EnumSet.copyOf(v))));
        return Collections.unmodifiableMap(frozen);
    }
}
