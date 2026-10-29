package com.moveinsync.scheduler.core.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Plan section 16, R4 FencingTokenOrderTest: pair order across epochs, timelines above 65,536. */
class FencingTokenTest {

    @Test
    void ordersByEpochFirstThenSeq() {
        FencingToken a = new FencingToken(1, 900);
        FencingToken b = new FencingToken(2, 1);
        FencingToken c = new FencingToken(2, 2);

        assertThat(a).isLessThan(b);
        assertThat(b).isLessThan(c);
        assertThat(b.isNewerThan(a)).as("higher epoch wins even with a lower seq").isTrue();
        assertThat(a.isNewerThan(b)).isFalse();
        assertThat(c.compareTo(new FencingToken(2, 2))).isZero();
        assertThat(c.isNewerThan(new FencingToken(2, 2))).isFalse();
    }

    @Test
    void sortingIsTotal() {
        List<FencingToken> tokens = new ArrayList<>(List.of(
                new FencingToken(5, 1), new FencingToken(1, 100), new FencingToken(5, 0),
                new FencingToken(Long.MAX_VALUE, 0), new FencingToken(1, Long.MAX_VALUE)));
        Collections.shuffle(tokens);
        Collections.sort(tokens);
        assertThat(tokens).containsExactly(
                new FencingToken(1, 100), new FencingToken(1, Long.MAX_VALUE),
                new FencingToken(5, 0), new FencingToken(5, 1), new FencingToken(Long.MAX_VALUE, 0));
    }

    @Test
    void timelinesAbove65536StillIncreaseTheEpoch() {
        // A 16-bit packing would wrap here; the pair representation must not.
        long t65535 = FencingToken.epochOf(0, 65_535);
        long t65536 = FencingToken.epochOf(0, 65_536);
        long t65537 = FencingToken.epochOf(0, 65_537);
        long tMax = FencingToken.epochOf(0, FencingToken.MAX_TIMELINE);

        assertThat(t65535).isLessThan(t65536);
        assertThat(t65536).isLessThan(t65537);
        assertThat(t65537).isLessThan(tMax);
        assertThat(new FencingToken(t65537, 1)).isGreaterThan(new FencingToken(t65536, 1_000_000_000L));
        assertThat(new FencingToken(FencingToken.epochOf(2, 70_000), 5))
                .isGreaterThan(new FencingToken(FencingToken.epochOf(2, 69_999), Long.MAX_VALUE));
    }

    @Test
    void newClusterGenerationBeatsEveryTimelineOfThePreviousOne() {
        long oldest = FencingToken.epochOf(0, FencingToken.MAX_TIMELINE);
        long rebuilt = FencingToken.epochOf(1, 1); // a fresh initdb restarts at timeline 1
        assertThat(rebuilt).isGreaterThan(oldest);
        assertThat(rebuilt).isEqualTo((1L << 32) + 1);
        assertThat(FencingToken.epochOf(FencingToken.MAX_CLUSTER_GENERATION, FencingToken.MAX_TIMELINE))
                .as("max epoch stays a non-negative long").isPositive();
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1, 0x1_0000_0000L})
    void timelineOutOfRangeFailsClosed(long timeline) {
        assertThatThrownBy(() -> FencingToken.epochOf(0, timeline)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void negativeGenerationRejected() {
        assertThatThrownBy(() -> FencingToken.epochOf(-1, 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void headerRoundTrip() {
        FencingToken token = new FencingToken(FencingToken.epochOf(3, 70_000), 123_456_789L);
        assertThat(token.toHeader()).isEqualTo(((3L << 32) + 70_000) + ".123456789");
        assertThat(FencingToken.parse(token.toHeader())).isEqualTo(token);
        assertThat(FencingToken.parse("0.0")).isEqualTo(new FencingToken(0, 0));
        assertThat(FencingToken.parse(Long.MAX_VALUE + "." + Long.MAX_VALUE))
                .isEqualTo(new FencingToken(Long.MAX_VALUE, Long.MAX_VALUE));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", ".", "1", "1.", ".1", "1.2.3", "-1.2", "1.-2", "+1.2", " 1.2", "1.2 ",
        "a.b", "1,2", "99999999999999999999.1", "1.99999999999999999999"})
    void malformedHeaderRejected(String header) {
        assertThatThrownBy(() -> FencingToken.parse(header)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void negativePartsRejected() {
        assertThatThrownBy(() -> new FencingToken(-1, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FencingToken(0, -1)).isInstanceOf(IllegalArgumentException.class);
    }
}
