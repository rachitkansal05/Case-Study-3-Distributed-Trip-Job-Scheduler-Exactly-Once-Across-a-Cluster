package com.moveinsync.scheduler.core.policy;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with "full jitter" (plan section 09): {@code delay = uniform random in [0,
 * ceiling]}, where {@code ceiling = min(cap, base * 2^attempt)}. Both ends are <b>inclusive</b>
 * and the granularity is one nanosecond. Defaults: base 2 s, cap 5 min, so the ceilings are 4 s,
 * 8 s, 16 s, ... up to 5 min.
 *
 * <p>The ceiling is computed by doubling with saturation at {@code cap}, so it never overflows
 * however large {@code attempt} is (attempt 100 or {@code Integer.MAX_VALUE} simply gives {@code
 * cap}).
 *
 * <p><b>Thread-safety.</b> {@link #withDefaults()} draws from {@link ThreadLocalRandom#current()}
 * on every call, so it is safe to share across threads. An instance built with {@link
 * #ExponentialFullJitterBackoff(Duration, Duration, RandomGenerator)} is exactly as thread-safe
 * as the generator it was given (a seeded {@code SplittableRandom} is fine for single-threaded
 * tests, not for sharing). The class holds no other mutable state.
 */
public final class ExponentialFullJitterBackoff implements BackoffPolicy {

    /** Default base delay. */
    public static final Duration DEFAULT_BASE = Duration.ofSeconds(2);

    /** Default maximum ceiling. */
    public static final Duration DEFAULT_CAP = Duration.ofMinutes(5);

    private final long baseNanos;
    private final long capNanos;
    private final Supplier<RandomGenerator> random;

    /**
     * @param base base delay, {@code > 0}
     * @param cap maximum ceiling, {@code >= base} and representable in nanoseconds
     * @param random source of randomness; see the class comment on thread-safety
     */
    public ExponentialFullJitterBackoff(Duration base, Duration cap, RandomGenerator random) {
        this(base, cap, constant(Objects.requireNonNull(random, "random")));
    }

    private ExponentialFullJitterBackoff(Duration base, Duration cap, Supplier<RandomGenerator> random) {
        Objects.requireNonNull(base, "base");
        Objects.requireNonNull(cap, "cap");
        if (base.isNegative() || base.isZero()) {
            throw new IllegalArgumentException("base must be > 0");
        }
        if (cap.compareTo(base) < 0) {
            throw new IllegalArgumentException("cap must be >= base");
        }
        try {
            this.capNanos = cap.toNanos();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("cap too large", e);
        }
        if (capNanos == Long.MAX_VALUE) {
            throw new IllegalArgumentException("cap too large");
        }
        this.baseNanos = base.toNanos();
        this.random = random;
    }

    /** Base 2 s, cap 5 min, thread-local randomness; safe to share across threads. */
    public static ExponentialFullJitterBackoff withDefaults() {
        return new ExponentialFullJitterBackoff(DEFAULT_BASE, DEFAULT_CAP, ThreadLocalRandom::current);
    }

    /**
     * Upper bound of the delay after attempt {@code attempt} failed: {@code min(cap, base *
     * 2^attempt)}.
     *
     * @throws IllegalArgumentException if {@code attempt < 1}
     */
    public Duration ceilingFor(int attempt) {
        return Duration.ofNanos(ceilingNanos(attempt));
    }

    /** Uniform random in {@code [0, ceilingFor(attempt)]}, both inclusive. */
    @Override
    public Duration delayFor(int attempt) {
        long ceiling = ceilingNanos(attempt);
        return Duration.ofNanos(random.get().nextLong(0, ceiling + 1)); // ceiling < Long.MAX_VALUE
    }

    /** The base delay. */
    public Duration base() {
        return Duration.ofNanos(baseNanos);
    }

    /** The maximum ceiling. */
    public Duration cap() {
        return Duration.ofNanos(capNanos);
    }

    private long ceilingNanos(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }
        long ceiling = baseNanos;
        // Terminates after at most ~63 doublings because base > 0 and we stop at cap.
        for (int i = 0; i < attempt && ceiling < capNanos; i++) {
            ceiling = ceiling > capNanos / 2 ? capNanos : ceiling * 2;
        }
        return Math.min(ceiling, capNanos);
    }

    private static Supplier<RandomGenerator> constant(RandomGenerator random) {
        return () -> random;
    }

    @Override
    public String toString() {
        return "ExponentialFullJitterBackoff[base=" + base() + ", cap=" + cap() + "]";
    }
}
