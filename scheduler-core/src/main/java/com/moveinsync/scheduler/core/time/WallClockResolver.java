package com.moveinsync.scheduler.core.time;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.List;
import java.util.Objects;

/**
 * Maps a local wall time in a zone to exactly one {@link Instant} (plan section 08). DST policy:
 *
 * <ul>
 *   <li><b>Normal</b>: the single valid offset.
 *   <li><b>Gap</b> (spring forward; the local time never appears on the clock): the instant the gap
 *       ends, i.e. the transition instant. NY 2026-03-08 02:30 resolves to 03:00 EDT = 07:00Z. This
 *       is deliberately <em>not</em> Java's default (shift by the gap length to 03:30): a nightly
 *       job should not move later than needed, and every skipped time collapses to the same
 *       instant, which keeps the run idempotency key unique.
 *   <li><b>Overlap</b> (fall back; the local time appears twice): the <em>earlier</em> instant,
 *       i.e. the offset in force before the transition. NY 2026-11-01 01:30 resolves to 01:30 EDT =
 *       05:30Z; the second 01:30 (EST) is never produced.
 * </ul>
 *
 * <p>The mapping is monotonic non-decreasing in local time, which {@link NextFireCalculator}
 * relies on. Stateless and thread-safe.
 */
public final class WallClockResolver {

    /** Which DST case applied. */
    public enum Kind {
        /** The local time has exactly one offset. */
        NORMAL,
        /** The local time was skipped; resolved to the end of the gap. */
        GAP,
        /** The local time occurs twice; resolved to the earlier occurrence. */
        OVERLAP
    }

    /**
     * Detailed result of a resolution.
     *
     * @param instant the resolved instant
     * @param offset the offset in force at {@code instant} (after the transition for a gap)
     * @param kind which DST case applied
     */
    public record Resolution(Instant instant, ZoneOffset offset, Kind kind) {

        public Resolution {
            Objects.requireNonNull(instant, "instant");
            Objects.requireNonNull(offset, "offset");
            Objects.requireNonNull(kind, "kind");
        }

        /** Wall time shown on the clock at {@link #instant()}; equals the input except for gaps. */
        public LocalDateTime wallTime() {
            return LocalDateTime.ofInstant(instant, offset);
        }
    }

    private WallClockResolver() {
    }

    /** Resolves {@code local} in {@code zone} to one instant using the policy above. */
    public static Instant resolve(LocalDateTime local, ZoneId zone) {
        return resolveDetailed(local, zone).instant();
    }

    /** Like {@link #resolve(LocalDateTime, ZoneId)}, also reporting the offset and the case. */
    public static Resolution resolveDetailed(LocalDateTime local, ZoneId zone) {
        Objects.requireNonNull(local, "local");
        Objects.requireNonNull(zone, "zone");
        ZoneRules rules = zone.getRules();
        List<ZoneOffset> offsets = rules.getValidOffsets(local);
        if (offsets.size() == 1) {
            ZoneOffset offset = offsets.get(0);
            return new Resolution(local.toInstant(offset), offset, Kind.NORMAL);
        }
        ZoneOffsetTransition transition = rules.getTransition(local);
        if (offsets.isEmpty()) {
            return new Resolution(transition.getInstant(), transition.getOffsetAfter(), Kind.GAP);
        }
        // Overlap: the offset before the transition is the larger one, giving the earlier instant.
        ZoneOffset earlier = transition.getOffsetBefore();
        return new Resolution(local.toInstant(earlier), earlier, Kind.OVERLAP);
    }
}
