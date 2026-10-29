package com.moveinsync.scheduler.core.policy;

import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * {@code SKIP}: misfired occurrences become {@code SKIPPED}; on-time occurrences (and the next
 * future occurrence, once due) fire normally. Good for alerts that are useless when late.
 */
public final class Skip implements MisfirePolicy {

    /** The singleton instance. */
    public static final Skip INSTANCE = new Skip();

    private Skip() {}

    @Override
    public MisfirePolicyType type() {
        return MisfirePolicyType.SKIP;
    }

    @Override
    public MisfireDecision decide(List<Instant> dueOccurrences, Instant now, Duration threshold) {
        MisfireSupport.Split split = MisfireSupport.split(dueOccurrences, now, threshold);
        return new MisfireDecision(split.onTime(), split.misfired(), 1);
    }

    @Override
    public String toString() {
        return "Skip";
    }
}
