package com.moveinsync.scheduler.core.policy;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Result of {@link MisfirePolicy#decide}: which occurrences fire and which become {@code SKIPPED}.
 *
 * <p>Rules enforced here:
 *
 * <ul>
 *   <li>{@code toFire} and {@code toSkip} are strictly ascending, null-free, disjoint and stored
 *       as immutable copies.
 *   <li>{@code coalescedCount >= 1} is how many occurrences the coalesced fire stands for; it is
 *       {@code 1} when nothing was coalesced. When it is greater than 1, the coalesced fire is the
 *       <i>first</i> element of {@code toFire} (the newest misfired occurrence is older than every
 *       on-time one) and at least {@code coalescedCount - 1} occurrences were skipped.
 *   <li>Whether an individually fired occurrence was late is not recorded here; use {@link
 *       MisfirePolicy#isMisfire} for that.
 * </ul>
 *
 * @param toFire occurrences that fire, ascending
 * @param toSkip occurrences that become {@code SKIPPED}, ascending
 * @param coalescedCount occurrences represented by the coalesced fire, {@code 1} if none
 */
public record MisfireDecision(List<Instant> toFire, List<Instant> toSkip, int coalescedCount) {

    public MisfireDecision {
        toFire = List.copyOf(Objects.requireNonNull(toFire, "toFire"));
        toSkip = List.copyOf(Objects.requireNonNull(toSkip, "toSkip"));
        requireStrictlyAscending(toFire, "toFire");
        requireStrictlyAscending(toSkip, "toSkip");
        if (coalescedCount < 1) {
            throw new IllegalArgumentException("coalescedCount must be >= 1");
        }
        if (coalescedCount > 1) {
            if (toFire.isEmpty()) {
                throw new IllegalArgumentException("coalescedCount > 1 needs a fired occurrence");
            }
            if (toSkip.size() < coalescedCount - 1) {
                throw new IllegalArgumentException("coalescedCount exceeds skipped occurrences + 1");
            }
        }
        Set<Instant> fired = new HashSet<>(toFire);
        for (Instant skipped : toSkip) {
            if (fired.contains(skipped)) {
                throw new IllegalArgumentException("toFire and toSkip must be disjoint");
            }
        }
    }

    /** A decision that fires every given occurrence individually and skips nothing. */
    public static MisfireDecision fireAll(List<Instant> occurrences) {
        return new MisfireDecision(occurrences, List.of(), 1);
    }

    /** The occurrence that carries the coalesced count, present only if {@code coalescedCount > 1}. */
    public Optional<Instant> coalescedInto() {
        return coalescedCount > 1 ? Optional.of(toFire.get(0)) : Optional.empty();
    }

    /** True if more than one occurrence was folded into a single fire. */
    public boolean isCoalesced() {
        return coalescedCount > 1;
    }

    /**
     * Coalesced count to store on the run fired for {@code fired}: {@link #coalescedCount()} for
     * the coalesced occurrence, {@code 1} for every other fired occurrence.
     *
     * @throws IllegalArgumentException if {@code fired} is not in {@link #toFire()}
     */
    public int coalescedCountFor(Instant fired) {
        Objects.requireNonNull(fired, "fired");
        if (!toFire.contains(fired)) {
            throw new IllegalArgumentException("occurrence is not in toFire");
        }
        return isCoalesced() && toFire.get(0).equals(fired) ? coalescedCount : 1;
    }

    private static void requireStrictlyAscending(List<Instant> list, String name) {
        for (int i = 1; i < list.size(); i++) {
            if (!list.get(i).isAfter(list.get(i - 1))) {
                throw new IllegalArgumentException(name + " must be strictly ascending");
            }
        }
    }
}
