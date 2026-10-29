package com.moveinsync.scheduler.core.lease;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.core.ports.SchedulerClock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/** Plan section 07, layer 1 self-fencing: deadline = claimTime + lease - margin, monotonic only. */
class LeaseClockTest {

    private static final Duration LEASE = Duration.ofSeconds(10);

    /** Controllable monotonic clock; {@code now()} must never be called by LeaseClock. */
    private static final class FakeClock implements SchedulerClock {
        private long nanos;

        FakeClock(long start) {
            this.nanos = start;
        }

        void advance(Duration d) {
            nanos += d.toNanos(); // wraps like System.nanoTime()
        }

        @Override
        public Instant now() {
            throw new AssertionError("LeaseClock must not read now()");
        }

        @Override
        public long monotonicNanos() {
            return nanos;
        }
    }

    @Test
    void freshClaimMayStartUntilEightSeconds() {
        FakeClock clock = new FakeClock(1_000);
        LeaseClock lc = LeaseClock.start(clock, LEASE, LeaseClock.DEFAULT_MARGIN);
        assertThat(lc.mayStartEffect()).isTrue();
        assertThat(lc.remaining()).isEqualTo(Duration.ofSeconds(8));

        clock.advance(Duration.ofSeconds(5));
        assertThat(lc.mayStartEffect()).isTrue();
        assertThat(lc.remaining()).isEqualTo(Duration.ofSeconds(3));

        clock.advance(Duration.ofSeconds(3).minusNanos(1));
        assertThat(lc.mayStartEffect()).isTrue();
        assertThat(lc.remaining()).isEqualTo(Duration.ofNanos(1));

        clock.advance(Duration.ofNanos(1)); // exactly 8 s
        assertThat(lc.mayStartEffect()).isFalse();
        assertThat(lc.remaining()).isEqualTo(Duration.ZERO);

        clock.advance(Duration.ofSeconds(1));
        assertThat(lc.mayStartEffect()).isFalse();
        assertThat(lc.remaining()).isEqualTo(Duration.ZERO);
    }

    @Test
    void startedAtUsesTheGivenClaimTime() {
        FakeClock clock = new FakeClock(0);
        clock.advance(Duration.ofSeconds(3));
        LeaseClock lc = LeaseClock.startedAt(clock, 0, LEASE, LeaseClock.DEFAULT_MARGIN);
        assertThat(lc.remaining()).isEqualTo(Duration.ofSeconds(5));
        assertThat(lc.deadlineNanos()).isEqualTo(Duration.ofSeconds(8).toNanos());
    }

    @Test
    void renewalExtendsFromTheSendTime() {
        FakeClock clock = new FakeClock(0);
        LeaseClock lc = LeaseClock.start(clock, LEASE, LeaseClock.DEFAULT_MARGIN);
        clock.advance(Duration.ofSeconds(3));
        long renewSent = clock.monotonicNanos();
        clock.advance(Duration.ofMillis(500)); // round trip of the renew statement
        LeaseClock renewed = lc.renewed(renewSent);
        assertThat(renewed.remaining()).isEqualTo(Duration.ofMillis(7_500));
        assertThat(renewed.mayStartEffect()).isTrue();
        assertThat(lc.remaining()).as("original is immutable").isEqualTo(Duration.ofMillis(4_500));

        clock.advance(Duration.ofMillis(7_500));
        assertThat(renewed.mayStartEffect()).isFalse();
    }

    @Test
    void renewalNeverMovesTheDeadlineBackwards() {
        FakeClock clock = new FakeClock(0);
        LeaseClock lc = LeaseClock.start(clock, LEASE, LeaseClock.DEFAULT_MARGIN);
        clock.advance(Duration.ofSeconds(4));
        LeaseClock later = lc.renewed(clock.monotonicNanos());
        LeaseClock older = later.renewed(Duration.ofSeconds(1).toNanos());
        assertThat(older.deadlineNanos()).isEqualTo(later.deadlineNanos());
    }

    @Test
    void renewalFromTheFutureIsRejected() {
        FakeClock clock = new FakeClock(0);
        LeaseClock lc = LeaseClock.start(clock, LEASE, LeaseClock.DEFAULT_MARGIN);
        assertThatThrownBy(() -> lc.renewed(1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void frozenThirtySecondsCannotStartEffectsOrBeResurrectedByStaleRenewal() {
        FakeClock clock = new FakeClock(123_456_789L);
        long claim = clock.monotonicNanos();
        LeaseClock lc = LeaseClock.start(clock, LEASE, LeaseClock.DEFAULT_MARGIN);
        long renewSentBeforeFreeze = clock.monotonicNanos() + Duration.ofSeconds(1).toNanos();
        clock.advance(Duration.ofSeconds(1));

        clock.advance(Duration.ofSeconds(30)); // SIGSTOP: the process wakes 30 s later

        assertThat(lc.mayStartEffect()).isFalse();
        assertThat(lc.remaining()).isEqualTo(Duration.ZERO);

        // A renew whose send time was captured before the freeze can only reach
        // renewStart + lease - margin, which is long past.
        LeaseClock stale = lc.renewed(renewSentBeforeFreeze);
        assertThat(stale.mayStartEffect()).isFalse();
        assertThat(stale.remaining()).isEqualTo(Duration.ZERO);
        assertThat(stale.deadlineNanos() - claim).isEqualTo(Duration.ofSeconds(9).toNanos());
        LeaseClock fromClaim = lc.renewed(claim);
        assertThat(fromClaim.deadlineNanos()).isEqualTo(lc.deadlineNanos());
        assertThat(fromClaim.mayStartEffect()).isFalse();
    }

    @Test
    void survivesNanoTimeWraparound() {
        FakeClock clock = new FakeClock(Long.MAX_VALUE - Duration.ofSeconds(3).toNanos());
        LeaseClock lc = LeaseClock.start(clock, LEASE, LeaseClock.DEFAULT_MARGIN);
        assertThat(lc.deadlineNanos()).as("deadline wrapped").isNegative();
        assertThat(lc.mayStartEffect()).isTrue();
        assertThat(lc.remaining()).isEqualTo(Duration.ofSeconds(8));

        clock.advance(Duration.ofSeconds(5)); // clock itself wraps negative here
        assertThat(clock.monotonicNanos()).isNegative();
        assertThat(lc.mayStartEffect()).isTrue();
        assertThat(lc.remaining()).isEqualTo(Duration.ofSeconds(3));

        long renewSent = clock.monotonicNanos();
        LeaseClock renewed = lc.renewed(renewSent);
        assertThat(renewed.remaining()).isEqualTo(Duration.ofSeconds(8));

        clock.advance(Duration.ofSeconds(3));
        assertThat(lc.mayStartEffect()).isFalse();
        assertThat(lc.remaining()).isEqualTo(Duration.ZERO);
        assertThat(renewed.mayStartEffect()).isTrue();
    }

    @Test
    void zeroMarginIsAllowed() {
        FakeClock clock = new FakeClock(0);
        LeaseClock lc = LeaseClock.start(clock, LEASE, Duration.ZERO);
        assertThat(lc.remaining()).isEqualTo(LEASE);
        assertThat(lc.lease()).isEqualTo(LEASE);
        assertThat(lc.margin()).isEqualTo(Duration.ZERO);
    }

    @Test
    void validation() {
        FakeClock clock = new FakeClock(0);
        assertThatThrownBy(() -> LeaseClock.start(clock, Duration.ofSeconds(2), Duration.ofSeconds(2)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LeaseClock.start(clock, Duration.ofSeconds(1), Duration.ofSeconds(2)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LeaseClock.start(clock, LEASE, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LeaseClock.start(clock, Duration.ofDays(365 * 1000), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LeaseClock.start(null, LEASE, Duration.ZERO))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> LeaseClock.start(clock, null, Duration.ZERO))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> LeaseClock.start(clock, LEASE, null))
                .isInstanceOf(NullPointerException.class);
    }
}
