package com.moveinsync.scheduler.app.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Cron validation and next fire times, evaluated in the tenant's wall-clock zone with the DST
 * rules of PLAN.md section 08.
 *
 * <p>The API layer has <b>no production implementation</b>: an adapter over Core's
 * {@code CronSchedule} / {@code NextFireCalculator} is wired in at merge time. Until then
 * {@link ServiceDefaultsConfig} registers a fallback that throws
 * {@link UnsupportedOperationException}, so the context still starts.
 */
public interface FireTimeService {

    /**
     * Checks a cron expression.
     *
     * @throws com.moveinsync.scheduler.app.error.ValidationException with code
     *         {@code INVALID_CRON} (see {@code ValidationException.invalidCron}) when it is invalid
     */
    void validateCron(String cron);

    /**
     * The next {@code count} fire instants strictly after {@code after}, in order. Fewer (or none)
     * are returned when the schedule has no further occurrences.
     */
    List<FireTimeView> nextFireTimes(String cron, ZoneId zone, Instant after, int count);

    /**
     * One occurrence.
     *
     * @param instant   UTC instant
     * @param localTime wall-clock time in the tenant's zone
     * @param offset    the zone's offset at that instant
     */
    record FireTimeView(Instant instant, LocalDateTime localTime, ZoneOffset offset) {

        /** The view of {@code instant} in {@code zone}. */
        public static FireTimeView of(Instant instant, ZoneId zone) {
            var zoned = instant.atZone(zone);
            return new FireTimeView(instant, zoned.toLocalDateTime(), zoned.getOffset());
        }
    }
}
