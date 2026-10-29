package com.moveinsync.scheduler.core.time;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CronScheduleTest {

    private static LocalDateTime t(String iso) {
        return LocalDateTime.parse(iso);
    }

    private static List<LocalDateTime> nextN(CronSchedule cron, LocalDateTime from, int n) {
        List<LocalDateTime> out = new ArrayList<>();
        LocalDateTime cursor = from;
        for (int i = 0; i < n; i++) {
            Optional<LocalDateTime> next = cron.nextLocalAfter(cursor);
            if (next.isEmpty()) {
                break;
            }
            out.add(next.get());
            cursor = next.get();
        }
        return out;
    }

    @Test
    void parsesWildcardsNumbersListsRangesAndSteps() {
        CronSchedule cron = CronSchedule.parse("0,30 9-17/4 * * *");
        assertThat(cron.matchesTime(9, 0)).isTrue();
        assertThat(cron.matchesTime(13, 30)).isTrue();
        assertThat(cron.matchesTime(17, 0)).isTrue();
        assertThat(cron.matchesTime(10, 0)).isFalse();
        assertThat(cron.matchesTime(9, 15)).isFalse();

        CronSchedule everyQuarter = CronSchedule.parse("*/15 * * * *");
        assertThat(everyQuarter.matchesTime(0, 45)).isTrue();
        assertThat(everyQuarter.matchesTime(0, 50)).isFalse();
    }

    @Test
    void startSlashStepRunsToFieldMax() {
        CronSchedule cron = CronSchedule.parse("50/5 * * * *");
        assertThat(cron.matchesTime(3, 50)).isTrue();
        assertThat(cron.matchesTime(3, 55)).isTrue();
        assertThat(cron.matchesTime(3, 0)).isFalse();
        assertThat(cron.matchesTime(3, 45)).isFalse();
    }

    @Test
    void monthAndDayNamesAreCaseInsensitive() {
        CronSchedule cron = CronSchedule.parse("0 8 * jan-Mar MON-fri");
        assertThat(cron.matches(t("2026-01-05T08:00"))).isTrue(); // Monday
        assertThat(cron.matches(t("2026-01-04T08:00"))).isFalse(); // Sunday
        assertThat(cron.matches(t("2026-04-06T08:00"))).isFalse(); // April
        assertThat(CronSchedule.parse("0 0 1 DEC *").matchesDate(LocalDate.of(2026, 12, 1))).isTrue();
    }

    @Test
    void sundayIsZeroAndSeven() {
        LocalDate sunday = LocalDate.of(2026, 11, 1);
        assertThat(CronSchedule.parse("0 0 * * 0").matchesDate(sunday)).isTrue();
        assertThat(CronSchedule.parse("0 0 * * 7").matchesDate(sunday)).isTrue();
        assertThat(CronSchedule.parse("0 0 * * SUN").matchesDate(sunday)).isTrue();
        assertThat(CronSchedule.parse("0 0 * * 5-7").matchesDate(sunday)).isTrue();
        assertThat(CronSchedule.parse("0 0 * * 5-7").matchesDate(sunday.minusDays(1))).isTrue();
        assertThat(CronSchedule.parse("0 0 * * 1-6").matchesDate(sunday)).isFalse();
    }

    @Test
    void secondsAreIgnoredByMatches() {
        assertThat(CronSchedule.parse("30 2 * * *").matches(t("2026-05-05T02:30:59.5"))).isTrue();
    }

    @Test
    void vixieOrWhenBothDayFieldsRestricted() {
        CronSchedule cron = CronSchedule.parse("0 0 13 * 5");
        // October 2026: the 13th is a Tuesday; Fridays are 2, 9, 16, 23, 30.
        assertThat(nextN(cron, t("2026-09-30T23:59"), 7)).containsExactly(
                t("2026-10-02T00:00"), t("2026-10-09T00:00"), t("2026-10-13T00:00"),
                t("2026-10-16T00:00"), t("2026-10-23T00:00"), t("2026-10-30T00:00"),
                t("2026-11-06T00:00"));
    }

    @Test
    void andWhenOneDayFieldStartsWithStar() {
        assertThat(CronSchedule.parse("0 0 13 * *").matchesDate(LocalDate.of(2026, 10, 16))).isFalse();
        assertThat(CronSchedule.parse("0 0 * * 5").matchesDate(LocalDate.of(2026, 10, 13))).isFalse();
        // "*/1" counts as unrestricted, so the day-of-week field alone decides.
        CronSchedule starStep = CronSchedule.parse("0 0 */1 * 5");
        assertThat(starStep.matchesDate(LocalDate.of(2026, 10, 13))).isFalse();
        assertThat(starStep.matchesDate(LocalDate.of(2026, 10, 16))).isTrue();
    }

    @Test
    void nextLocalAfterIsStrictAndIgnoresSeconds() {
        CronSchedule cron = CronSchedule.parse("30 2 * * *");
        assertThat(cron.nextLocalAfter(t("2026-05-05T02:30"))).contains(t("2026-05-06T02:30"));
        assertThat(cron.nextLocalAfter(t("2026-05-05T02:29:59"))).contains(t("2026-05-05T02:30"));
        assertThat(cron.nextLocalAfter(t("2026-12-31T23:59"))).contains(t("2027-01-01T02:30"));
    }

    @Test
    void feb29FoundAcrossLeapCycles() {
        CronSchedule cron = CronSchedule.parse("0 0 29 2 *");
        assertThat(cron.nextLocalAfter(t("2026-01-01T00:00"))).contains(t("2028-02-29T00:00"));
        assertThat(cron.nextLocalAfter(t("2028-02-29T00:00"))).contains(t("2032-02-29T00:00"));
        // 2100 is not a leap year: 8-year wait.
        assertThat(cron.nextLocalAfter(t("2096-03-01T00:00"))).contains(t("2104-02-29T00:00"));
    }

    @Test
    void impossibleExpressionsReturnEmpty() {
        assertThat(CronSchedule.parse("0 0 30 2 *").nextLocalAfter(t("2026-01-01T00:00"))).isEmpty();
        assertThat(CronSchedule.parse("0 0 31 2 *").nextLocalAfter(t("2026-01-01T00:00"))).isEmpty();
        assertThat(CronSchedule.parse("0 0 31 4,6,9,11 *").nextLocalAfter(t("2026-01-01T00:00"))).isEmpty();
    }

    @Test
    void everyMinuteAndSparseSearchesAreCorrect() {
        assertThat(nextN(CronSchedule.parse("* * * * *"), t("2026-01-01T23:58"), 3))
                .containsExactly(t("2026-01-01T23:59"), t("2026-01-02T00:00"), t("2026-01-02T00:01"));
        assertThat(CronSchedule.parse("59 23 31 12 *").nextLocalAfter(t("2027-01-01T00:00")))
                .contains(t("2027-12-31T23:59"));
    }

    @Test
    void expressionIsNormalisedAndDrivesEquality() {
        CronSchedule a = CronSchedule.parse("  0   2 * *\t*  ");
        CronSchedule b = CronSchedule.parse("0 2 * * *");
        assertThat(a.expression()).isEqualTo("0 2 * * *");
        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a.toString()).isEqualTo("0 2 * * *");
        assertThat(a).isNotEqualTo(CronSchedule.parse("0 3 * * *"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "", "   ", "* * * *", "* * * * * *", "61 * * * *", "60 * * * *", "* 24 * * *",
        "* * 0 * *", "* * 32 * *", "* * * 0 *", "* * * 13 *", "* * * * 8", "*/0 * * * *",
        "5-1 * * * *", "a * * * *", "1,,2 * * * *", "1, * * * *", "1- * * * *", "-1 * * * *",
        "1/ * * * *", "*/2/3 * * * *", "* * * FOO *", "* * * * FRIDAY", "* * * JAN-DEC/x *",
        "100 * * * *", "* * * * MON-SUN", "? * * * *", "L * * * *", "* * * * 1#2"
    })
    void rejectsMalformedExpressions(String expression) {
        assertThatThrownBy(() -> CronSchedule.parse(expression)).isInstanceOf(InvalidCronException.class);
    }

    @Test
    void errorMessagesNameFieldAndToken() {
        assertThatThrownBy(() -> CronSchedule.parse("61 * * * *"))
                .isInstanceOf(InvalidCronException.class)
                .hasMessage("invalid minute field: 61");
        assertThatThrownBy(() -> CronSchedule.parse("* * * * 8"))
                .hasMessage("invalid day-of-week field: 8");
        String longToken = "x".repeat(150);
        assertThatThrownBy(() -> CronSchedule.parse(longToken + " * * * *"))
                .hasMessageNotContaining(longToken)
                .hasMessageStartingWith("invalid minute field: ");
        assertThatThrownBy(() -> CronSchedule.parse("1 ".repeat(150)))
                .isInstanceOf(InvalidCronException.class)
                .hasMessageContaining("longer than");
        assertThatThrownBy(() -> CronSchedule.parse(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void matchesTimeValidatesArguments() {
        CronSchedule cron = CronSchedule.parse("* * * * *");
        assertThatThrownBy(() -> cron.matchesTime(24, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> cron.matchesTime(0, 60)).isInstanceOf(IllegalArgumentException.class);
    }
}
