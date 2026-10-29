package com.moveinsync.scheduler.core.time;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Computes the fire instants of a {@link CronSchedule} in a zone (plan section 08). Cron matching
 * is done on local wall time; each matching local time is mapped to one instant by {@link
 * WallClockResolver} (gap: end of gap; overlap: earlier offset).
 *
 * <p>Guarantees:
 *
 * <ul>
 *   <li>Results are strictly increasing and strictly after {@code after} (an {@code after} equal
 *       to a fire instant does not return that instant).
 *   <li>Several matching local times inside one DST gap (e.g. {@code *}{@code /15} at
 *       02:00..02:45 on NY 2026-03-08), plus 03:00 if it also matches, collapse into
 *       <em>one</em> fire at the gap end (07:00Z).
 *   <li>In an overlap, each distinct local time fires once, at its earlier offset: {@code
 *       *}{@code /15} gives 4 runs in the repeated 01:xx hour, not 8. If {@code after} lies in the
 *       second pass of the overlap, local times already resolved to earlier instants are skipped.
 * </ul>
 *
 * <p>Stateless and thread-safe; one instance may be shared.
 */
public final class NextFireCalculator {

    /** Largest {@code count} accepted by {@link #next(CronSchedule, ZoneId, Instant, int)}. */
    public static final int MAX_COUNT = 1000;

    /**
     * Resolved instants never lag the local scan by more than the largest zone offset swing, so
     * once the scan is this far past the start without passing {@code after}, something is wrong;
     * the bound guarantees the loop terminates.
     */
    private static final long SCAN_GUARD_DAYS = 3;

    /**
     * One fire time.
     *
     * <p>{@code localTime} is the wall time actually shown on the clock at {@code instant} in the
     * zone, i.e. {@code instant} viewed at {@code offset}. For a normal or overlap time it equals
     * the cron-matched local time; for a gap it is the end of the gap (03:00 for a matched 02:30 on
     * NY 2026-03-08), not the skipped time. The invariant {@code
     * localTime.toInstant(offset).equals(instant)} always holds and is enforced.
     *
     * @param instant the fire instant
     * @param localTime the wall time at {@code instant} in the zone
     * @param offset the zone offset in force at {@code instant}
     */
    public record FireTime(Instant instant, LocalDateTime localTime, ZoneOffset offset) {

        public FireTime {
            Objects.requireNonNull(instant, "instant");
            Objects.requireNonNull(localTime, "localTime");
            Objects.requireNonNull(offset, "offset");
            if (!localTime.toInstant(offset).equals(instant)) {
                throw new IllegalArgumentException("localTime at offset must equal instant");
            }
        }
    }

    /** Creates a calculator; it holds no state. */
    public NextFireCalculator() {
    }

    /**
     * The next {@code count} fire times strictly after {@code after}.
     *
     * @param count 0 to {@link #MAX_COUNT}
     * @return strictly increasing fire times; fewer than {@code count} only if the schedule runs
     *     out (impossible expressions give an empty list)
     * @throws IllegalArgumentException if {@code count} is out of range
     */
    public List<FireTime> next(CronSchedule cron, ZoneId zone, Instant after, int count) {
        Objects.requireNonNull(cron, "cron");
        Objects.requireNonNull(zone, "zone");
        Objects.requireNonNull(after, "after");
        if (count < 0 || count > MAX_COUNT) {
            throw new IllegalArgumentException("count must be 0-" + MAX_COUNT + ": " + count);
        }
        List<FireTime> result = new ArrayList<>(Math.min(count, 64));
        Instant cursor = after;
        while (result.size() < count) {
            Optional<FireTime> next = nextAfter(cron, zone, cursor);
            if (next.isEmpty()) {
                break;
            }
            result.add(next.get());
            cursor = next.get().instant();
        }
        return List.copyOf(result);
    }

    /**
     * The first fire time strictly after {@code after}.
     *
     * @return the fire time, or empty if the schedule can never fire (see {@link
     *     CronSchedule#nextLocalAfter(LocalDateTime)})
     */
    public Optional<FireTime> nextAfter(CronSchedule cron, ZoneId zone, Instant after) {
        Objects.requireNonNull(cron, "cron");
        Objects.requireNonNull(zone, "zone");
        Objects.requireNonNull(after, "after");
        // Resolution is monotonic in local time, so no local time before local(after) can resolve
        // to an instant after `after`; scanning forward from local(after) is complete.
        LocalDateTime start = LocalDateTime.ofInstant(after, zone);
        LocalDateTime guard = start.plusDays(SCAN_GUARD_DAYS);
        LocalDateTime candidate = start;
        while (true) {
            Optional<LocalDateTime> next = cron.nextLocalAfter(candidate);
            if (next.isEmpty()) {
                return Optional.empty();
            }
            candidate = next.get();
            WallClockResolver.Resolution resolved = WallClockResolver.resolveDetailed(candidate, zone);
            if (resolved.instant().isAfter(after)) {
                return Optional.of(new FireTime(resolved.instant(), resolved.wallTime(), resolved.offset()));
            }
            if (candidate.isAfter(guard)) {
                return Optional.empty();
            }
        }
    }
}
