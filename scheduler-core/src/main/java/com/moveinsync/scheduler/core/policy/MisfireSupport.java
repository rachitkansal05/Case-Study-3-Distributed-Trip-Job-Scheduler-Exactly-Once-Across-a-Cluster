package com.moveinsync.scheduler.core.policy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Shared validation and partitioning for the {@link MisfirePolicy} implementations. */
final class MisfireSupport {

    private MisfireSupport() {}

    /** Due occurrences split into the misfired prefix and the on-time suffix (both ascending). */
    record Split(List<Instant> misfired, List<Instant> onTime) {}

    static void requireThreshold(Duration threshold) {
        Objects.requireNonNull(threshold, "threshold");
        if (threshold.isNegative()) {
            throw new IllegalArgumentException("threshold must be >= 0");
        }
    }

    /**
     * Validates the input and splits it. Because the list is ascending and lateness decreases
     * along it, the misfired occurrences form a prefix.
     */
    static Split split(List<Instant> dueOccurrences, Instant now, Duration threshold) {
        Objects.requireNonNull(dueOccurrences, "dueOccurrences");
        Objects.requireNonNull(now, "now");
        requireThreshold(threshold);
        List<Instant> due = List.copyOf(dueOccurrences); // rejects null elements
        int firstOnTime = due.size();
        for (int i = 0; i < due.size(); i++) {
            Instant at = due.get(i);
            if (i > 0 && !at.isAfter(due.get(i - 1))) {
                throw new IllegalArgumentException("dueOccurrences must be strictly ascending");
            }
            if (at.isAfter(now)) {
                throw new IllegalArgumentException("dueOccurrences must not be after now");
            }
            if (firstOnTime == due.size() && !MisfirePolicy.isMisfire(at, now, threshold)) {
                firstOnTime = i;
            }
        }
        return new Split(due.subList(0, firstOnTime), due.subList(firstOnTime, due.size()));
    }
}
