package com.moveinsync.scheduler.core.time;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Next fire times of daily jobs across both 2026 DST transitions in New York (plan R2). */
class NextFireTimesTest {

    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");

    private final NextFireCalculator calc = new NextFireCalculator();

    private List<Instant> next(String cron, String after, int n) {
        return calc.next(CronSchedule.parse(cron), NEW_YORK, Instant.parse(after), n).stream()
                .map(NextFireCalculator.FireTime::instant)
                .toList();
    }

    @Test
    void dailyAt0230AcrossSpringForward() {
        assertThat(next("30 2 * * *", "2026-03-06T12:00:00Z", 5)).containsExactly(
                Instant.parse("2026-03-07T07:30:00Z"), // 02:30 EST
                Instant.parse("2026-03-08T07:00:00Z"), // 02:30 skipped: gap end 03:00 EDT
                Instant.parse("2026-03-09T06:30:00Z"), // 02:30 EDT
                Instant.parse("2026-03-10T06:30:00Z"),
                Instant.parse("2026-03-11T06:30:00Z"));
    }

    @Test
    void dailyAt0130AcrossFallBack() {
        assertThat(next("30 1 * * *", "2026-10-30T12:00:00Z", 4)).containsExactly(
                Instant.parse("2026-10-31T05:30:00Z"), // 01:30 EDT
                Instant.parse("2026-11-01T05:30:00Z"), // first 01:30 (EDT) only
                Instant.parse("2026-11-02T06:30:00Z"), // 01:30 EST
                Instant.parse("2026-11-03T06:30:00Z"));
    }

    @Test
    void dailyAt0900WholeYearHasOneFirePerDay() {
        List<Instant> fires = next("0 9 * * *", "2025-12-31T23:59:59Z", 365);
        assertThat(fires).hasSize(365);
        LocalDate day = LocalDate.of(2026, 1, 1);
        for (Instant fire : fires) {
            assertThat(fire.atZone(NEW_YORK).toLocalDate()).isEqualTo(day);
            assertThat(fire.atZone(NEW_YORK).toLocalTime().toString()).isEqualTo("09:00");
            day = day.plusDays(1);
        }
        assertThat(fires).contains(
                Instant.parse("2026-03-07T14:00:00Z"),
                Instant.parse("2026-03-08T13:00:00Z"),
                Instant.parse("2026-10-31T13:00:00Z"),
                Instant.parse("2026-11-01T14:00:00Z"));
    }
}
