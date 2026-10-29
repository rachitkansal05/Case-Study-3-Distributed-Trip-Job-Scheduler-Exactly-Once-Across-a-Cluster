package com.moveinsync.scheduler.core.time;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class NextFireCalculatorTest {

    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");
    private static final ZoneId LONDON = ZoneId.of("Europe/London");
    private static final ZoneId KOLKATA = ZoneId.of("Asia/Kolkata");

    private final NextFireCalculator calc = new NextFireCalculator();

    private static Instant i(String iso) {
        return Instant.parse(iso);
    }

    private List<Instant> instants(String cron, ZoneId zone, String after, int n) {
        return calc.next(CronSchedule.parse(cron), zone, i(after), n).stream()
                .map(NextFireCalculator.FireTime::instant)
                .toList();
    }

    @Test
    void newYorkGapDailyJobFiresAtGapEnd() {
        NextFireCalculator.FireTime fire = calc.nextAfter(
                CronSchedule.parse("30 2 * * *"), NEW_YORK, i("2026-03-08T05:00:00Z")).orElseThrow();
        assertThat(fire.instant()).isEqualTo(i("2026-03-08T07:00:00Z"));
        assertThat(fire.localTime()).isEqualTo(LocalDateTime.parse("2026-03-08T03:00"));
        assertThat(fire.offset()).isEqualTo(ZoneOffset.ofHours(-4));
    }

    @Test
    void newYorkOverlapDailyJobFiresOnceAtEarlierOffset() {
        List<Instant> fires = instants("30 1 * * *", NEW_YORK, "2026-11-01T00:00:00Z", 2);
        assertThat(fires).containsExactly(i("2026-11-01T05:30:00Z"), i("2026-11-02T06:30:00Z"));
    }

    @Test
    void quarterHourlyCollapsesGapIntoOneFire() {
        assertThat(instants("*/15 * * * *", NEW_YORK, "2026-03-08T06:30:00Z", 4)).containsExactly(
                i("2026-03-08T06:45:00Z"), // 01:45 EST
                i("2026-03-08T07:00:00Z"), // 02:00..02:45 skipped and 03:00 EDT: one fire
                i("2026-03-08T07:15:00Z"),
                i("2026-03-08T07:30:00Z"));
    }

    @Test
    void quarterHourlyFiresFourTimesInRepeatedHour() {
        assertThat(instants("*/15 * * * *", NEW_YORK, "2026-11-01T04:30:00Z", 7)).containsExactly(
                i("2026-11-01T04:45:00Z"),
                i("2026-11-01T05:00:00Z"),
                i("2026-11-01T05:15:00Z"),
                i("2026-11-01T05:30:00Z"),
                i("2026-11-01T05:45:00Z"),
                i("2026-11-01T07:00:00Z"), // 02:00 EST; nothing in the second 01:xx pass
                i("2026-11-01T07:15:00Z"));
    }

    @Test
    void afterInsideSecondPassOfOverlapSkipsAlreadyResolvedTimes() {
        CronSchedule cron = CronSchedule.parse("*/15 * * * *");
        // 06:10Z is 01:10 EST (second pass); 01:15..01:45 resolve to EDT instants before it.
        assertThat(calc.nextAfter(cron, NEW_YORK, i("2026-11-01T06:10:00Z")).orElseThrow().instant())
                .isEqualTo(i("2026-11-01T07:00:00Z"));
        assertThat(calc.nextAfter(cron, NEW_YORK, i("2026-11-01T06:00:00Z")).orElseThrow().instant())
                .isEqualTo(i("2026-11-01T07:00:00Z"));
        // End of the first pass (01:50 EDT).
        assertThat(calc.nextAfter(cron, NEW_YORK, i("2026-11-01T05:50:00Z")).orElseThrow().instant())
                .isEqualTo(i("2026-11-01T07:00:00Z"));
    }

    @Test
    void londonTransitions() {
        assertThat(instants("30 1 * * *", LONDON, "2026-03-28T12:00:00Z", 2))
                .containsExactly(i("2026-03-29T01:00:00Z"), i("2026-03-30T00:30:00Z"));
        assertThat(instants("30 1 * * *", LONDON, "2026-10-24T12:00:00Z", 2))
                .containsExactly(i("2026-10-25T00:30:00Z"), i("2026-10-26T01:30:00Z"));
    }

    @Test
    void kolkataEveryDayAt1630Z() {
        List<Instant> fires = instants("0 22 * * *", KOLKATA, "2026-01-01T00:00:00Z", 365);
        assertThat(fires).hasSize(365);
        Instant expected = i("2026-01-01T16:30:00Z");
        for (Instant fire : fires) {
            assertThat(fire).isEqualTo(expected);
            expected = expected.plusSeconds(86_400);
        }
    }

    @Test
    void feb29FromTwentyTwentySix() {
        assertThat(instants("0 0 29 2 *", ZoneOffset.UTC, "2026-01-01T00:00:00Z", 2))
                .containsExactly(i("2028-02-29T00:00:00Z"), i("2032-02-29T00:00:00Z"));
    }

    @Test
    void vixieOrSemantics() {
        assertThat(instants("0 0 13 * 5", ZoneOffset.UTC, "2026-10-01T00:00:00Z", 5)).containsExactly(
                i("2026-10-02T00:00:00Z"), i("2026-10-09T00:00:00Z"), i("2026-10-13T00:00:00Z"),
                i("2026-10-16T00:00:00Z"), i("2026-10-23T00:00:00Z"));
    }

    @Test
    void impossibleExpressionGivesEmpty() {
        CronSchedule cron = CronSchedule.parse("0 0 31 2 *");
        assertThat(calc.nextAfter(cron, NEW_YORK, i("2026-01-01T00:00:00Z"))).isEmpty();
        assertThat(calc.next(cron, NEW_YORK, i("2026-01-01T00:00:00Z"), 10)).isEmpty();
    }

    @Test
    void afterExactlyAtFireInstantIsExcluded() {
        CronSchedule cron = CronSchedule.parse("30 2 * * *");
        assertThat(calc.nextAfter(cron, NEW_YORK, i("2026-03-08T07:00:00Z")).orElseThrow().instant())
                .isEqualTo(i("2026-03-09T06:30:00Z"));
        assertThat(calc.nextAfter(cron, NEW_YORK, i("2026-06-01T06:30:00Z")).orElseThrow().instant())
                .isEqualTo(i("2026-06-02T06:30:00Z"));
        assertThat(calc.nextAfter(cron, NEW_YORK, i("2026-06-01T06:29:59.999Z")).orElseThrow().instant())
                .isEqualTo(i("2026-06-01T06:30:00Z"));
    }

    @Test
    void resultsStrictlyIncreasingAndFireTimeInvariantHolds() {
        for (String cron : new String[] {"*/15 * * * *", "*/7 0-3 * * *", "30 1 * * *", "0 2 * * SUN"}) {
            for (ZoneId zone : List.of(NEW_YORK, LONDON)) {
                Instant after = i("2026-03-01T00:00:00Z");
                List<NextFireCalculator.FireTime> fires = calc.next(CronSchedule.parse(cron), zone, after, 1000);
                Instant previous = after;
                for (NextFireCalculator.FireTime fire : fires) {
                    assertThat(fire.instant()).isAfter(previous);
                    assertThat(fire.localTime().toInstant(fire.offset())).isEqualTo(fire.instant());
                    assertThat(fire.offset()).isEqualTo(zone.getRules().getOffset(fire.instant()));
                    previous = fire.instant();
                }
            }
        }
    }

    @Test
    void countIsValidated() {
        CronSchedule cron = CronSchedule.parse("* * * * *");
        Instant now = i("2026-01-01T00:00:00Z");
        assertThat(calc.next(cron, NEW_YORK, now, 0)).isEmpty();
        assertThat(calc.next(cron, NEW_YORK, now, NextFireCalculator.MAX_COUNT)).hasSize(1000);
        assertThatThrownBy(() -> calc.next(cron, NEW_YORK, now, -1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calc.next(cron, NEW_YORK, now, 1001)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fireTimeRejectsInconsistentFields() {
        assertThatThrownBy(() -> new NextFireCalculator.FireTime(i("2026-03-08T07:00:00Z"),
                LocalDateTime.parse("2026-03-08T02:30"), ZoneOffset.ofHours(-4)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NextFireCalculator.FireTime(null, LocalDateTime.now(), ZoneOffset.UTC))
                .isInstanceOf(NullPointerException.class);
    }
}
