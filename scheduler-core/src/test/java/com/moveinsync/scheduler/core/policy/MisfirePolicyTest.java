package com.moveinsync.scheduler.core.policy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Plan section 16, R6 MisfirePolicyTest: 3 missed hourly runs → FIRE_NOW 3, SKIP 0, COALESCE 1 (count 3). */
class MisfirePolicyTest {

    private static final Duration THRESHOLD = Duration.ofSeconds(60);
    private static final Instant T0 = Instant.parse("2026-03-01T00:00:00Z");
    private static final List<Instant> HOURLY = List.of(T0, T0.plusSeconds(3600), T0.plusSeconds(7200));
    private static final Instant NOW = T0.plus(Duration.ofHours(3).plusMinutes(5));

    @Test
    void ofReturnsMatchingSingleton() {
        assertThat(MisfirePolicy.of(MisfirePolicyType.FIRE_NOW)).isSameAs(FireNow.INSTANCE);
        assertThat(MisfirePolicy.of(MisfirePolicyType.SKIP)).isSameAs(Skip.INSTANCE);
        assertThat(MisfirePolicy.of(MisfirePolicyType.COALESCE)).isSameAs(Coalesce.INSTANCE);
        for (MisfirePolicyType type : MisfirePolicyType.values()) {
            assertThat(MisfirePolicy.of(type).type()).isEqualTo(type);
        }
        assertThatThrownBy(() -> MisfirePolicy.of(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void fireNowFiresAllThreeMissedRuns() {
        MisfireDecision d = FireNow.INSTANCE.decide(HOURLY, NOW, THRESHOLD);
        assertThat(d.toFire()).containsExactlyElementsOf(HOURLY);
        assertThat(d.toSkip()).isEmpty();
        assertThat(d.coalescedCount()).isEqualTo(1);
        assertThat(d.coalescedInto()).isEmpty();
        HOURLY.forEach(at -> assertThat(d.coalescedCountFor(at)).isEqualTo(1));
    }

    @Test
    void skipSkipsAllThreeMissedRuns() {
        MisfireDecision d = Skip.INSTANCE.decide(HOURLY, NOW, THRESHOLD);
        assertThat(d.toFire()).isEmpty();
        assertThat(d.toSkip()).containsExactlyElementsOf(HOURLY);
        assertThat(d.coalescedCount()).isEqualTo(1);
    }

    @Test
    void coalesceFiresNewestOnceWithCountThree() {
        MisfireDecision d = Coalesce.INSTANCE.decide(HOURLY, NOW, THRESHOLD);
        assertThat(d.toFire()).containsExactly(HOURLY.get(2));
        assertThat(d.toSkip()).containsExactly(HOURLY.get(0), HOURLY.get(1));
        assertThat(d.coalescedCount()).isEqualTo(3);
        assertThat(d.coalescedInto()).contains(HOURLY.get(2));
        assertThat(d.isCoalesced()).isTrue();
        assertThat(d.coalescedCountFor(HOURLY.get(2))).isEqualTo(3);
        assertThatThrownBy(() -> d.coalescedCountFor(HOURLY.get(0)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @EnumSource(MisfirePolicyType.class)
    void withinThresholdIsNotAMisfire(MisfirePolicyType type) {
        Instant at = NOW.minusSeconds(59);
        MisfireDecision d = MisfirePolicy.of(type).decide(List.of(at), NOW, THRESHOLD);
        assertThat(d.toFire()).containsExactly(at);
        assertThat(d.toSkip()).isEmpty();
        assertThat(d.coalescedCount()).isEqualTo(1);
    }

    @ParameterizedTest
    @EnumSource(MisfirePolicyType.class)
    void exactlyAtThresholdIsNotAMisfire(MisfirePolicyType type) {
        Instant at = NOW.minus(THRESHOLD);
        assertThat(MisfirePolicy.isMisfire(at, NOW, THRESHOLD)).isFalse();
        assertThat(MisfirePolicy.isMisfire(at.minusNanos(1), NOW, THRESHOLD)).isTrue();
        assertThat(MisfirePolicy.of(type).decide(List.of(at), NOW, THRESHOLD).toFire()).containsExactly(at);
    }

    @Test
    void oneNanoPastThresholdIsAMisfire() {
        Instant at = NOW.minus(THRESHOLD).minusNanos(1);
        assertThat(Skip.INSTANCE.decide(List.of(at), NOW, THRESHOLD).toSkip()).containsExactly(at);
    }

    @Test
    void zeroThresholdMisfiresAnythingLate() {
        assertThat(MisfirePolicy.isMisfire(NOW, NOW, Duration.ZERO)).isFalse();
        assertThat(MisfirePolicy.isMisfire(NOW.minusNanos(1), NOW, Duration.ZERO)).isTrue();
    }

    @Test
    void mixedMisfiredAndOnTime() {
        Instant late1 = NOW.minusSeconds(600);
        Instant late2 = NOW.minusSeconds(300);
        Instant onTime1 = NOW.minusSeconds(30);
        Instant onTime2 = NOW;
        List<Instant> due = List.of(late1, late2, onTime1, onTime2);

        MisfireDecision fireNow = FireNow.INSTANCE.decide(due, NOW, THRESHOLD);
        assertThat(fireNow.toFire()).containsExactlyElementsOf(due);
        assertThat(fireNow.toSkip()).isEmpty();

        MisfireDecision skip = Skip.INSTANCE.decide(due, NOW, THRESHOLD);
        assertThat(skip.toFire()).containsExactly(onTime1, onTime2);
        assertThat(skip.toSkip()).containsExactly(late1, late2);

        MisfireDecision coalesce = Coalesce.INSTANCE.decide(due, NOW, THRESHOLD);
        assertThat(coalesce.toFire()).containsExactly(late2, onTime1, onTime2);
        assertThat(coalesce.toSkip()).containsExactly(late1);
        assertThat(coalesce.coalescedCount()).isEqualTo(2);
        assertThat(coalesce.coalescedCountFor(late2)).isEqualTo(2);
        assertThat(coalesce.coalescedCountFor(onTime1)).isEqualTo(1);
        assertThat(coalesce.coalescedCountFor(onTime2)).isEqualTo(1);
    }

    @Test
    void coalesceWithSingleMisfireHasCountOne() {
        Instant late = NOW.minusSeconds(600);
        MisfireDecision d = Coalesce.INSTANCE.decide(List.of(late), NOW, THRESHOLD);
        assertThat(d.toFire()).containsExactly(late);
        assertThat(d.toSkip()).isEmpty();
        assertThat(d.coalescedCount()).isEqualTo(1);
        assertThat(d.coalescedInto()).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(MisfirePolicyType.class)
    void emptyListGivesEmptyDecision(MisfirePolicyType type) {
        MisfireDecision d = MisfirePolicy.of(type).decide(List.of(), NOW, THRESHOLD);
        assertThat(d.toFire()).isEmpty();
        assertThat(d.toSkip()).isEmpty();
        assertThat(d.coalescedCount()).isEqualTo(1);
    }

    @ParameterizedTest
    @EnumSource(MisfirePolicyType.class)
    void everyOccurrenceEndsUpInExactlyOneList(MisfirePolicyType type) {
        List<Instant> due = new ArrayList<>();
        for (int i = 10; i >= 0; i--) {
            due.add(NOW.minusSeconds(i * 20L));
        }
        MisfireDecision d = MisfirePolicy.of(type).decide(due, NOW, THRESHOLD);
        List<Instant> all = new ArrayList<>(d.toFire());
        all.addAll(d.toSkip());
        assertThat(all).containsExactlyInAnyOrderElementsOf(due);
    }

    @ParameterizedTest
    @EnumSource(MisfirePolicyType.class)
    void rejectsInvalidInput(MisfirePolicyType type) {
        MisfirePolicy p = MisfirePolicy.of(type);
        assertThatThrownBy(() -> p.decide(List.of(T0.plusSeconds(1), T0), NOW, THRESHOLD))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ascending");
        assertThatThrownBy(() -> p.decide(List.of(T0, T0), NOW, THRESHOLD))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ascending");
        assertThatThrownBy(() -> p.decide(List.of(NOW.plusNanos(1)), NOW, THRESHOLD))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("after now");
        assertThatThrownBy(() -> p.decide(List.of(T0), NOW, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> p.decide(null, NOW, THRESHOLD)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> p.decide(List.of(T0), null, THRESHOLD)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> p.decide(List.of(T0), NOW, null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> p.decide(Arrays.asList(T0, null), NOW, THRESHOLD))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void decisionCopiesInputDefensively() {
        List<Instant> fire = new ArrayList<>(List.of(T0));
        MisfireDecision d = new MisfireDecision(fire, List.of(), 1);
        fire.add(T0.plusSeconds(1));
        assertThat(d.toFire()).containsExactly(T0);
        assertThatThrownBy(() -> d.toFire().add(T0)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void decisionValidation() {
        Instant a = T0;
        Instant b = T0.plusSeconds(1);
        assertThatThrownBy(() -> new MisfireDecision(List.of(a), List.of(a), 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("disjoint");
        assertThatThrownBy(() -> new MisfireDecision(List.of(b, a), List.of(), 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MisfireDecision(List.of(), List.of(b, a), 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MisfireDecision(List.of(a), List.of(), 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MisfireDecision(List.of(), List.of(a), 2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MisfireDecision(List.of(b), List.of(a), 3))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new MisfireDecision(List.of(b), List.of(a), 2).coalescedInto()).contains(b);
    }
}
