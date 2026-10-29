package com.moveinsync.scheduler.core.lease;

import com.moveinsync.scheduler.core.ports.SchedulerClock;
import java.time.Duration;
import java.util.Objects;

/**
 * Local self-fence of one claim (plan section 07, layer 1): a monotonic deadline {@code claimTime
 * + lease - margin}, checked before every outbound effect.
 *
 * <p>Rules:
 *
 * <ul>
 *   <li>Only {@link SchedulerClock#monotonicNanos()} is read, never {@code now()}: the deadline
 *       must not move with wall-clock or database-clock changes.
 *   <li>The claim time is taken <b>before</b> the claim statement is sent ({@link #start}), so
 *       the database lease started no earlier than our local start, and the local deadline is
 *       never later than the real expiry minus {@code margin}.
 *   <li>{@link #mayStartEffect()} is true iff {@code now < deadline}; the comparison uses {@code
 *       now - deadline < 0}, so it is correct across {@code nanoTime} wraparound (as long as the
 *       two values are within 2<sup>63</sup> ns, about 292 years).
 *   <li>A renewal ({@link #renewed(long)}) is measured from the moment the renew request was
 *       <b>sent</b>, and can never move the deadline backwards. Call it only after the database
 *       confirmed the renew (1 row updated); the database rejects renewing an expired lease, so a
 *       process that was frozen past its deadline cannot resurrect it.
 * </ul>
 *
 * <p>This check alone is not enough (a pause can land between check and send); sink fencing and
 * DB fencing cover that. Instances are immutable and thread-safe.
 */
public final class LeaseClock {

    /** Default safety margin subtracted from the lease. */
    public static final Duration DEFAULT_MARGIN = Duration.ofSeconds(2);

    private final SchedulerClock clock;
    private final long leaseNanos;
    private final long marginNanos;
    private final long deadlineNanos;

    private LeaseClock(SchedulerClock clock, long leaseNanos, long marginNanos, long deadlineNanos) {
        this.clock = clock;
        this.leaseNanos = leaseNanos;
        this.marginNanos = marginNanos;
        this.deadlineNanos = deadlineNanos;
    }

    /**
     * Starts the self-fence now. Call this <b>before</b> sending the claim statement.
     *
     * @param clock source of {@code monotonicNanos()}
     * @param lease lease length granted by the claim, {@code > margin}
     * @param margin safety margin, {@code >= 0} (usually {@link #DEFAULT_MARGIN})
     * @throws IllegalArgumentException unless {@code lease > margin >= 0}
     */
    public static LeaseClock start(SchedulerClock clock, Duration lease, Duration margin) {
        Objects.requireNonNull(clock, "clock");
        return startedAt(clock, clock.monotonicNanos(), lease, margin);
    }

    /**
     * Self-fence for a claim whose local start was captured earlier at {@code claimNanos} (taken
     * before the claim statement was sent).
     *
     * @throws IllegalArgumentException unless {@code lease > margin >= 0}
     */
    public static LeaseClock startedAt(SchedulerClock clock, long claimNanos, Duration lease, Duration margin) {
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(margin, "margin");
        if (margin.isNegative()) {
            throw new IllegalArgumentException("margin must be >= 0");
        }
        if (lease.compareTo(margin) <= 0) {
            throw new IllegalArgumentException("lease must be > margin");
        }
        long leaseNanos;
        try {
            leaseNanos = lease.toNanos();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("lease too large", e);
        }
        long marginNanos = margin.toNanos();
        return new LeaseClock(clock, leaseNanos, marginNanos, claimNanos + (leaseNanos - marginNanos));
    }

    /** True iff a new effect may still be started: {@code now < deadline}. */
    public boolean mayStartEffect() {
        return clock.monotonicNanos() - deadlineNanos < 0;
    }

    /** Time left until the deadline, {@link Duration#ZERO} once it has passed. */
    public Duration remaining() {
        long left = deadlineNanos - clock.monotonicNanos();
        return left > 0 ? Duration.ofNanos(left) : Duration.ZERO;
    }

    /**
     * Self-fence after a confirmed renewal: deadline {@code renewStartedNanos + lease - margin},
     * or the current deadline if that is later (renewal never moves it backwards).
     *
     * @param renewStartedNanos {@code monotonicNanos()} captured <b>before</b> the renew request
     *     was sent
     * @throws IllegalArgumentException if {@code renewStartedNanos} lies in the future
     */
    public LeaseClock renewed(long renewStartedNanos) {
        if (renewStartedNanos - clock.monotonicNanos() > 0) {
            throw new IllegalArgumentException("renewStartedNanos must not be in the future");
        }
        long candidate = renewStartedNanos + (leaseNanos - marginNanos);
        long deadline = candidate - deadlineNanos > 0 ? candidate : deadlineNanos;
        return new LeaseClock(clock, leaseNanos, marginNanos, deadline);
    }

    /** Monotonic deadline in {@code monotonicNanos()} units (compare with subtraction only). */
    public long deadlineNanos() {
        return deadlineNanos;
    }

    /** Lease length. */
    public Duration lease() {
        return Duration.ofNanos(leaseNanos);
    }

    /** Safety margin. */
    public Duration margin() {
        return Duration.ofNanos(marginNanos);
    }

    @Override
    public String toString() {
        return "LeaseClock[lease=" + lease() + ", margin=" + margin() + ", remaining=" + remaining() + "]";
    }
}
