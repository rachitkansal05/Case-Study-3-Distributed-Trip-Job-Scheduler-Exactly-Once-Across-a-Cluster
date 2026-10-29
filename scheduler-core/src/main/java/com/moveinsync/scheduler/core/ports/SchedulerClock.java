package com.moveinsync.scheduler.core.ports;

import java.time.Instant;

/**
 * Time source for scheduling decisions (plan sections 07 and 15).
 *
 * <p>Two different clocks, never mixed:
 *
 * <ul>
 *   <li>{@link #now()} is the <b>authoritative</b> time used for due times, misfire lateness and
 *       lease expiry. In production it is the database clock ({@code clock_timestamp()} on the
 *       primary), so node clock skew cannot change any decision. Tests use a controllable fake.
 *   <li>{@link #monotonicNanos()} is the <b>local</b> monotonic clock used only for self-fencing
 *       (deadline {@code claimTime + lease - selfFenceMargin}) and for measuring durations. It is
 *       not comparable across nodes and has no relation to wall time.
 * </ul>
 */
public interface SchedulerClock {

    /** Authoritative current instant (database time in production). */
    Instant now();

    /** Local monotonic time in nanoseconds, as {@link System#nanoTime()}. */
    long monotonicNanos();
}
