package com.moveinsync.scheduler.core.policy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Plan section 16, R8 BackoffPolicyTest: bounds and jitter distribution. */
class BackoffPolicyTest {

    private static final Duration BASE = Duration.ofSeconds(2);
    private static final Duration CAP = Duration.ofMinutes(5);

    private static ExponentialFullJitterBackoff seeded(long seed) {
        return new ExponentialFullJitterBackoff(BASE, CAP, new SplittableRandom(seed));
    }

    @Test
    void ceilingsDoubleFromFourSecondsAndCapAtFiveMinutes() {
        ExponentialFullJitterBackoff b = ExponentialFullJitterBackoff.withDefaults();
        assertThat(b.ceilingFor(1)).isEqualTo(Duration.ofSeconds(4));
        assertThat(b.ceilingFor(2)).isEqualTo(Duration.ofSeconds(8));
        assertThat(b.ceilingFor(3)).isEqualTo(Duration.ofSeconds(16));
        assertThat(b.ceilingFor(4)).isEqualTo(Duration.ofSeconds(32));
        assertThat(b.ceilingFor(5)).isEqualTo(Duration.ofSeconds(64));
        assertThat(b.ceilingFor(6)).isEqualTo(Duration.ofSeconds(128));
        assertThat(b.ceilingFor(7)).isEqualTo(Duration.ofSeconds(256));
        assertThat(b.ceilingFor(8)).isEqualTo(CAP);
        assertThat(b.ceilingFor(9)).isEqualTo(CAP);
    }

    @ParameterizedTest
    @ValueSource(ints = {30, 62, 63, 64, 100, 1_000, Integer.MAX_VALUE})
    void hugeAttemptsAreBoundedByCap(int attempt) {
        ExponentialFullJitterBackoff b = seeded(1);
        assertThat(b.ceilingFor(attempt)).isEqualTo(CAP);
        for (int i = 0; i < 100; i++) {
            assertThat(b.delayFor(attempt)).isBetween(Duration.ZERO, CAP);
        }
    }

    @Test
    void everySampleIsWithinZeroAndCeiling() {
        ExponentialFullJitterBackoff b = seeded(42);
        for (int attempt = 1; attempt <= 12; attempt++) {
            Duration ceiling = b.ceilingFor(attempt);
            for (int i = 0; i < 1_000; i++) {
                assertThat(b.delayFor(attempt)).isBetween(Duration.ZERO, ceiling);
            }
        }
        ExponentialFullJitterBackoff defaults = ExponentialFullJitterBackoff.withDefaults();
        for (int i = 0; i < 1_000; i++) {
            assertThat(defaults.delayFor(3)).isBetween(Duration.ZERO, Duration.ofSeconds(16));
        }
    }

    @Test
    void jitterIsSpreadUniformlyOverTheWholeRange() {
        ExponentialFullJitterBackoff b = seeded(7);
        int attempt = 9; // ceiling = cap = 300 s
        long ceiling = b.ceilingFor(attempt).toNanos();
        int samples = 10_000;
        int[] deciles = new int[10];
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        double sum = 0;
        for (int i = 0; i < samples; i++) {
            long d = b.delayFor(attempt).toNanos();
            min = Math.min(min, d);
            max = Math.max(max, d);
            sum += d;
            deciles[(int) Math.min(9, d * 10 / ceiling)]++;
        }
        assertThat((double) min).isLessThan(ceiling * 0.01);
        assertThat((double) max).isGreaterThan(ceiling * 0.99);
        assertThat(sum / samples).isBetween(ceiling * 0.47, ceiling * 0.53);
        for (int count : deciles) {
            // Expected 1,000 per decile; 800..1,200 is more than 6 standard deviations wide.
            assertThat(count).isBetween(800, 1_200);
        }
    }

    @Test
    void deterministicWithFixedSeed() {
        List<Duration> first = new ArrayList<>();
        List<Duration> second = new ArrayList<>();
        ExponentialFullJitterBackoff a = seeded(123);
        ExponentialFullJitterBackoff b = seeded(123);
        for (int attempt = 1; attempt <= 20; attempt++) {
            first.add(a.delayFor(attempt));
            second.add(b.delayFor(attempt));
        }
        assertThat(first).isEqualTo(second);
        assertThat(first).doesNotHaveDuplicates();
    }

    @Test
    void customBaseAndCap() {
        ExponentialFullJitterBackoff b =
                new ExponentialFullJitterBackoff(Duration.ofMillis(100), Duration.ofSeconds(1), new SplittableRandom(1));
        assertThat(b.ceilingFor(1)).isEqualTo(Duration.ofMillis(200));
        assertThat(b.ceilingFor(3)).isEqualTo(Duration.ofMillis(800));
        assertThat(b.ceilingFor(4)).isEqualTo(Duration.ofSeconds(1));
        ExponentialFullJitterBackoff flat =
                new ExponentialFullJitterBackoff(Duration.ofSeconds(1), Duration.ofSeconds(1), new SplittableRandom(1));
        assertThat(flat.ceilingFor(1)).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void validation() {
        ExponentialFullJitterBackoff b = seeded(1);
        assertThatThrownBy(() -> b.delayFor(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> b.ceilingFor(-1)).isInstanceOf(IllegalArgumentException.class);
        SplittableRandom r = new SplittableRandom(1);
        assertThatThrownBy(() -> new ExponentialFullJitterBackoff(Duration.ZERO, CAP, r))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExponentialFullJitterBackoff(Duration.ofSeconds(-1), CAP, r))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExponentialFullJitterBackoff(CAP, BASE, r))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExponentialFullJitterBackoff(BASE, Duration.ofDays(365 * 1000), r))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ExponentialFullJitterBackoff(null, CAP, r))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ExponentialFullJitterBackoff(BASE, CAP, null))
                .isInstanceOf(NullPointerException.class);
    }
}
