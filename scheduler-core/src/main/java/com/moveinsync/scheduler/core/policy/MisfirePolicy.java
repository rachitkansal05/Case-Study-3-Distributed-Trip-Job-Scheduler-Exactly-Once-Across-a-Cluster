package com.moveinsync.scheduler.core.policy;

import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Misfire policy of one job (plan section 09, requirement R6), applied when the poller looks at a
 * job's due runs.
 *
 * <p><b>Misfire rule.</b> An occurrence scheduled at {@code s} has <i>misfired</i> at
 * authoritative time {@code now} iff {@code now - s > threshold} (strictly greater; a run exactly
 * {@code threshold} late is still on time). See {@link #isMisfire(Instant, Instant, Duration)}.
 *
 * <p><b>Decision rules</b> for {@link #decide(List, Instant, Duration)}:
 *
 * <ul>
 *   <li>Occurrences that have not misfired always fire, individually, each with coalesced count 1.
 *   <li>{@link FireNow}: every misfired occurrence fires too, in order, each with its own key.
 *   <li>{@link Skip}: every misfired occurrence is skipped ({@code SKIPPED}).
 *   <li>{@link Coalesce}: the <i>newest</i> misfired occurrence fires once, carrying {@code
 *       coalescedCount = number of misfired occurrences}; the other misfired ones are skipped.
 * </ul>
 *
 * <p>Implementations are stateless singletons and thread-safe.
 */
public sealed interface MisfirePolicy permits FireNow, Skip, Coalesce {

    /** The column value this policy implements. */
    MisfirePolicyType type();

    /**
     * Decides which of one job's due occurrences fire and which are skipped.
     *
     * @param dueOccurrences scheduled times of the job's due runs, strictly ascending, none after
     *     {@code now} (due means {@code scheduledAt <= now}); may be empty
     * @param now authoritative current time ({@code SchedulerClock.now()})
     * @param threshold misfire threshold ({@code job.misfire_threshold}), {@code >= 0}
     * @return the decision; every input occurrence is in exactly one of {@code toFire} and {@code
     *     toSkip}
     * @throws IllegalArgumentException if the list is not strictly ascending, contains a future
     *     occurrence, or {@code threshold} is negative
     */
    MisfireDecision decide(List<Instant> dueOccurrences, Instant now, Duration threshold);

    /** Returns the singleton policy for {@code type}. */
    static MisfirePolicy of(MisfirePolicyType type) {
        Objects.requireNonNull(type, "type");
        return switch (type) {
            case FIRE_NOW -> FireNow.INSTANCE;
            case SKIP -> Skip.INSTANCE;
            case COALESCE -> Coalesce.INSTANCE;
        };
    }

    /**
     * True iff an occurrence scheduled at {@code scheduledAt} has misfired at {@code now}: {@code
     * now - scheduledAt > threshold}, strictly.
     *
     * @throws IllegalArgumentException if {@code threshold} is negative
     */
    static boolean isMisfire(Instant scheduledAt, Instant now, Duration threshold) {
        Objects.requireNonNull(scheduledAt, "scheduledAt");
        Objects.requireNonNull(now, "now");
        MisfireSupport.requireThreshold(threshold);
        return scheduledAt.plus(threshold).isBefore(now);
    }
}
