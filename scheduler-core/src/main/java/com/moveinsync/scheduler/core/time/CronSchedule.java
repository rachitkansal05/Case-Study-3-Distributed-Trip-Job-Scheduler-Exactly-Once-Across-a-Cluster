package com.moveinsync.scheduler.core.time;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable five-field cron expression evaluated against <b>local wall time</b> (plan section 08).
 * The zone is not part of the schedule; {@link NextFireCalculator} combines a schedule with a zone.
 *
 * <p>Format: {@code minute hour day-of-month month day-of-week}, fields separated by whitespace,
 * exactly five fields.
 *
 * <table>
 *   <caption>Field ranges</caption>
 *   <tr><th>Field</th><th>Values</th></tr>
 *   <tr><td>minute</td><td>0-59</td></tr>
 *   <tr><td>hour</td><td>0-23</td></tr>
 *   <tr><td>day-of-month</td><td>1-31</td></tr>
 *   <tr><td>month</td><td>1-12 or {@code JAN}-{@code DEC}</td></tr>
 *   <tr><td>day-of-week</td><td>0-7 or {@code SUN}-{@code SAT}; 0 and 7 are both Sunday</td></tr>
 * </table>
 *
 * <p>Each field is a comma-separated list of elements; an element is {@code *}, a value {@code a},
 * a range {@code a-b} (reversed ranges are rejected), optionally followed by a step {@code /n}
 * ({@code n >= 1}). {@code a/n} means {@code a-max/n}. Names are case-insensitive and may appear
 * in ranges ({@code MON-FRI}). Anything else (empty, wrong field count, out-of-range, step 0,
 * garbage) is rejected with {@link InvalidCronException}.
 *
 * <p><b>Day matching (Vixie cron semantics).</b> If both day-of-month and day-of-week are
 * restricted (neither field text starts with {@code *}), a date matches when <em>either</em>
 * matches: {@code 0 0 13 * 5} fires on every 13th and on every Friday. Otherwise both must match,
 * which in practice means the restricted one decides (so {@code *}{@code /2} in day-of-month still
 * counts as unrestricted for this rule, like Vixie cron).
 *
 * <p>Seconds and nanoseconds of the arguments are ignored: {@link #matches(LocalDateTime)} looks at
 * the minute only, and {@link #nextLocalAfter(LocalDateTime)} returns times with zero seconds.
 *
 * <p>Equality, hash code and {@link #toString()} use the normalised expression text (trimmed,
 * fields joined by one space); {@code 0 0 * * MON} and {@code 0 0 * * 1} are therefore different
 * objects even though they fire at the same times.
 */
public final class CronSchedule {

    /**
     * How far {@link #nextLocalAfter(LocalDateTime)} searches before reporting "never". 28 years is
     * a full Gregorian weekday/leap cycle outside century years and covers the longest legitimate
     * wait (Feb 29 across a skipped century leap year: 8 years), so an empty result means the
     * expression can never match (e.g. {@code 0 0 30 2 *}).
     */
    public static final int SEARCH_HORIZON_YEARS = 28;

    /** Longest accepted expression, in characters (after trimming). */
    public static final int MAX_EXPRESSION_LENGTH = 200;

    private static final int MAX_TOKEN_IN_MESSAGE = 16;

    private static final String[] MONTH_NAMES = {
        "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"
    };
    private static final String[] DAY_NAMES = {"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"};

    private enum Field {
        MINUTE("minute", 0, 59),
        HOUR("hour", 0, 23),
        DAY_OF_MONTH("day-of-month", 1, 31),
        MONTH("month", 1, 12),
        DAY_OF_WEEK("day-of-week", 0, 7);

        final String label;
        final int min;
        final int max;

        Field(String label, int min, int max) {
            this.label = label;
            this.min = min;
            this.max = max;
        }
    }

    private final String expression;
    private final long minutes;
    private final long hours;
    private final long daysOfMonth;
    private final long months;
    private final long daysOfWeek;
    private final boolean dayOfMonthStar;
    private final boolean dayOfWeekStar;

    private CronSchedule(String expression, String[] fields) {
        this.expression = expression;
        this.minutes = parseField(fields[0], Field.MINUTE);
        this.hours = parseField(fields[1], Field.HOUR);
        this.daysOfMonth = parseField(fields[2], Field.DAY_OF_MONTH);
        this.months = parseField(fields[3], Field.MONTH);
        this.daysOfWeek = parseField(fields[4], Field.DAY_OF_WEEK);
        this.dayOfMonthStar = fields[2].startsWith("*");
        this.dayOfWeekStar = fields[4].startsWith("*");
    }

    /**
     * Parses a five-field cron expression.
     *
     * @param expression the expression; surrounding whitespace is ignored
     * @return the immutable schedule
     * @throws NullPointerException if {@code expression} is null
     * @throws InvalidCronException if the expression is malformed
     */
    public static CronSchedule parse(String expression) {
        Objects.requireNonNull(expression, "expression");
        String trimmed = expression.strip();
        if (trimmed.isEmpty()) {
            throw new InvalidCronException("cron expression is empty");
        }
        if (trimmed.length() > MAX_EXPRESSION_LENGTH) {
            throw new InvalidCronException(
                    "cron expression longer than " + MAX_EXPRESSION_LENGTH + " characters");
        }
        String[] fields = trimmed.split("\\s+");
        if (fields.length != 5) {
            throw new InvalidCronException(
                    "cron expression must have exactly 5 fields, got " + fields.length);
        }
        return new CronSchedule(String.join(" ", fields), fields);
    }

    /** The normalised expression text (trimmed, fields separated by one space). */
    public String expression() {
        return expression;
    }

    /** True if the minute of {@code t} matches; seconds and nanoseconds are ignored. */
    public boolean matches(LocalDateTime t) {
        Objects.requireNonNull(t, "t");
        return matchesDate(t.toLocalDate()) && matchesTime(t.getHour(), t.getMinute());
    }

    /** True if the month and day (day-of-month / day-of-week, Vixie semantics) match. */
    public boolean matchesDate(LocalDate d) {
        Objects.requireNonNull(d, "d");
        return bit(months, d.getMonthValue()) && dayMatches(d);
    }

    /**
     * True if the hour and minute fields match.
     *
     * @throws IllegalArgumentException if {@code hour} is not 0-23 or {@code minute} is not 0-59
     */
    public boolean matchesTime(int hour, int minute) {
        if (hour < 0 || hour > 23) {
            throw new IllegalArgumentException("hour must be 0-23: " + hour);
        }
        if (minute < 0 || minute > 59) {
            throw new IllegalArgumentException("minute must be 0-59: " + minute);
        }
        return bit(hours, hour) && bit(minutes, minute);
    }

    /**
     * The first matching local date-time strictly after {@code after} (at minute precision: the
     * search starts at the minute following {@code after} truncated to minutes).
     *
     * <p>Skips whole non-matching months and days and uses bit scans for hours and minutes, so a
     * call costs at most a few thousand cheap steps even for a sparse expression.
     *
     * @return the next match, or empty if none exists within {@link #SEARCH_HORIZON_YEARS} years
     *     (the expression can never match, e.g. {@code 0 0 31 2 *})
     */
    public Optional<LocalDateTime> nextLocalAfter(LocalDateTime after) {
        Objects.requireNonNull(after, "after");
        LocalDateTime start = after.truncatedTo(ChronoUnit.MINUTES).plusMinutes(1);
        LocalDate limit = start.toLocalDate().plusYears(SEARCH_HORIZON_YEARS);
        LocalDate date = start.toLocalDate();
        int hour = start.getHour();
        int minute = start.getMinute();
        while (!date.isAfter(limit)) {
            if (!bit(months, date.getMonthValue())) {
                date = date.withDayOfMonth(1).plusMonths(1);
                hour = 0;
                minute = 0;
                continue;
            }
            if (dayMatches(date)) {
                int minuteOfDay = nextTimeOnDay(hour, minute);
                if (minuteOfDay >= 0) {
                    return Optional.of(date.atTime(minuteOfDay / 60, minuteOfDay % 60));
                }
            }
            date = date.plusDays(1);
            hour = 0;
            minute = 0;
        }
        return Optional.empty();
    }

    /** First matching minute-of-day at or after {@code hour:minute}, or -1. */
    private int nextTimeOnDay(int hour, int minute) {
        int h = nextBit(hours, hour);
        while (h >= 0) {
            int m = nextBit(minutes, h == hour ? minute : 0);
            if (m >= 0) {
                return h * 60 + m;
            }
            h = nextBit(hours, h + 1);
        }
        return -1;
    }

    private boolean dayMatches(LocalDate d) {
        boolean domHit = bit(daysOfMonth, d.getDayOfMonth());
        boolean dowHit = bit(daysOfWeek, d.getDayOfWeek().getValue() % 7);
        if (!dayOfMonthStar && !dayOfWeekStar) {
            return domHit || dowHit;
        }
        return domHit && dowHit;
    }

    private static boolean bit(long mask, int index) {
        return (mask & (1L << index)) != 0;
    }

    private static int nextBit(long mask, int from) {
        if (from >= Long.SIZE) {
            return -1;
        }
        long masked = mask & (-1L << from);
        return masked == 0 ? -1 : Long.numberOfTrailingZeros(masked);
    }

    private static long parseField(String text, Field field) {
        long mask = 0;
        for (String element : text.split(",", -1)) {
            mask |= parseElement(element, field);
        }
        return mask;
    }

    private static long parseElement(String element, Field field) {
        if (element.isEmpty()) {
            throw invalid(field, element);
        }
        int slash = element.indexOf('/');
        String base = slash < 0 ? element : element.substring(0, slash);
        int step = 1;
        if (slash >= 0) {
            step = parseNumber(element.substring(slash + 1), field, element, null);
            if (step < 1) {
                throw invalid(field, element);
            }
        }
        int lo;
        int hi;
        if (base.equals("*")) {
            lo = field.min;
            hi = field.max;
        } else {
            int dash = base.indexOf('-');
            if (dash >= 0) {
                lo = parseValue(base.substring(0, dash), field, element);
                hi = parseValue(base.substring(dash + 1), field, element);
                if (lo > hi) {
                    throw invalid(field, element);
                }
            } else {
                lo = parseValue(base, field, element);
                hi = slash >= 0 ? field.max : lo;
            }
        }
        long mask = 0;
        for (int v = lo; v <= hi; v += step) {
            int index = field == Field.DAY_OF_WEEK && v == 7 ? 0 : v;
            mask |= 1L << index;
        }
        return mask;
    }

    private static int parseValue(String token, Field field, String element) {
        String[] names = switch (field) {
            case MONTH -> MONTH_NAMES;
            case DAY_OF_WEEK -> DAY_NAMES;
            default -> null;
        };
        int value = parseNumber(token, field, element, names);
        if (value < field.min || value > field.max) {
            throw invalid(field, element);
        }
        return value;
    }

    private static int parseNumber(String token, Field field, String element, String[] names) {
        if (names != null && token.length() == 3 && Character.isLetter(token.charAt(0))) {
            String upper = token.toUpperCase(Locale.ROOT);
            for (int i = 0; i < names.length; i++) {
                if (names[i].equals(upper)) {
                    return field == Field.MONTH ? i + 1 : i;
                }
            }
            throw invalid(field, element);
        }
        if (token.isEmpty() || token.length() > 2) {
            throw invalid(field, element);
        }
        int value = 0;
        for (int i = 0; i < token.length(); i++) {
            char c = token.charAt(i);
            if (c < '0' || c > '9') {
                throw invalid(field, element);
            }
            value = value * 10 + (c - '0');
        }
        return value;
    }

    private static InvalidCronException invalid(Field field, String token) {
        String shown = token.length() > MAX_TOKEN_IN_MESSAGE
                ? token.substring(0, MAX_TOKEN_IN_MESSAGE) + "..."
                : token;
        return new InvalidCronException("invalid " + field.label + " field: " + shown);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CronSchedule other && expression.equals(other.expression);
    }

    @Override
    public int hashCode() {
        return expression.hashCode();
    }

    /** The normalised expression text. */
    @Override
    public String toString() {
        return expression;
    }
}
