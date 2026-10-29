package com.moveinsync.scheduler.core.time;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

class WallClockResolverTest {

    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");
    private static final ZoneId LONDON = ZoneId.of("Europe/London");
    private static final ZoneId KOLKATA = ZoneId.of("Asia/Kolkata");

    private static LocalDateTime t(String iso) {
        return LocalDateTime.parse(iso);
    }

    @Test
    void newYorkGapResolvesToGapEndNotJavaDefault() {
        WallClockResolver.Resolution r = WallClockResolver.resolveDetailed(t("2026-03-08T02:30"), NEW_YORK);
        assertThat(r.instant()).isEqualTo(Instant.parse("2026-03-08T07:00:00Z"));
        assertThat(r.offset()).isEqualTo(ZoneOffset.ofHours(-4));
        assertThat(r.kind()).isEqualTo(WallClockResolver.Kind.GAP);
        assertThat(r.wallTime()).isEqualTo(t("2026-03-08T03:00"));
        // Java's default would shift to 03:30 EDT = 07:30Z; we deliberately do not.
        assertThat(ZonedDateTime.of(t("2026-03-08T02:30"), NEW_YORK).toInstant())
                .isEqualTo(Instant.parse("2026-03-08T07:30:00Z"))
                .isNotEqualTo(r.instant());
        assertThat(WallClockResolver.resolve(t("2026-03-08T02:00"), NEW_YORK))
                .isEqualTo(Instant.parse("2026-03-08T07:00:00Z"));
        assertThat(WallClockResolver.resolve(t("2026-03-08T02:59"), NEW_YORK))
                .isEqualTo(Instant.parse("2026-03-08T07:00:00Z"));
    }

    @Test
    void newYorkOverlapResolvesToEarlierOffset() {
        WallClockResolver.Resolution r = WallClockResolver.resolveDetailed(t("2026-11-01T01:30"), NEW_YORK);
        assertThat(r.instant()).isEqualTo(Instant.parse("2026-11-01T05:30:00Z"));
        assertThat(r.offset()).isEqualTo(ZoneOffset.ofHours(-4));
        assertThat(r.kind()).isEqualTo(WallClockResolver.Kind.OVERLAP);
        assertThat(r.wallTime()).isEqualTo(t("2026-11-01T01:30"));
        assertThat(WallClockResolver.resolve(t("2026-11-01T02:00"), NEW_YORK))
                .isEqualTo(Instant.parse("2026-11-01T07:00:00Z"));
    }

    @Test
    void normalTimesUseTheSingleOffset() {
        WallClockResolver.Resolution r = WallClockResolver.resolveDetailed(t("2026-07-01T09:00"), NEW_YORK);
        assertThat(r.kind()).isEqualTo(WallClockResolver.Kind.NORMAL);
        assertThat(r.instant()).isEqualTo(Instant.parse("2026-07-01T13:00:00Z"));
        assertThat(WallClockResolver.resolve(t("2026-01-15T09:00"), NEW_YORK))
                .isEqualTo(Instant.parse("2026-01-15T14:00:00Z"));
    }

    @Test
    void londonLastSundaysOfMarchAndOctober() {
        WallClockResolver.Resolution gap = WallClockResolver.resolveDetailed(t("2026-03-29T01:30"), LONDON);
        assertThat(gap.kind()).isEqualTo(WallClockResolver.Kind.GAP);
        assertThat(gap.instant()).isEqualTo(Instant.parse("2026-03-29T01:00:00Z"));
        assertThat(gap.wallTime()).isEqualTo(t("2026-03-29T02:00"));

        WallClockResolver.Resolution overlap = WallClockResolver.resolveDetailed(t("2026-10-25T01:30"), LONDON);
        assertThat(overlap.kind()).isEqualTo(WallClockResolver.Kind.OVERLAP);
        assertThat(overlap.instant()).isEqualTo(Instant.parse("2026-10-25T00:30:00Z"));
        assertThat(overlap.offset()).isEqualTo(ZoneOffset.ofHours(1));
    }

    @Test
    void kolkataHasNoDst() {
        for (String day : new String[] {"2026-01-10", "2026-03-08", "2026-07-01", "2026-11-01"}) {
            WallClockResolver.Resolution r = WallClockResolver.resolveDetailed(t(day + "T22:00"), KOLKATA);
            assertThat(r.kind()).isEqualTo(WallClockResolver.Kind.NORMAL);
            assertThat(r.instant()).isEqualTo(Instant.parse(day + "T16:30:00Z"));
        }
    }

    @Test
    void resolutionIsMonotonicAcrossTransitions() {
        for (String from : new String[] {"2026-03-08T00:00", "2026-11-01T00:00"}) {
            LocalDateTime local = t(from);
            Instant previous = WallClockResolver.resolve(local, NEW_YORK);
            for (int i = 0; i < 4 * 60; i++) {
                local = local.plusMinutes(1);
                Instant current = WallClockResolver.resolve(local, NEW_YORK);
                assertThat(current).isAfterOrEqualTo(previous);
                previous = current;
            }
        }
    }
}
