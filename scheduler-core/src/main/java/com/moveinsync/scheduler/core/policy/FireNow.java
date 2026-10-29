package com.moveinsync.scheduler.core.policy;

import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * {@code FIRE_NOW}: every due occurrence fires, misfired or not, in order, each with its own
 * idempotency key and coalesced count 1. Good for work where each period must be processed (for
 * example a daily billing close).
 */
public final class FireNow implements MisfirePolicy {

    /** The singleton instance. */
    public static final FireNow INSTANCE = new FireNow();

    private FireNow() {}

    @Override
    public MisfirePolicyType type() {
        return MisfirePolicyType.FIRE_NOW;
    }

    @Override
    public MisfireDecision decide(List<Instant> dueOccurrences, Instant now, Duration threshold) {
        MisfireSupport.split(dueOccurrences, now, threshold); // validation only
        return MisfireDecision.fireAll(dueOccurrences);
    }

    @Override
    public String toString() {
        return "FireNow";
    }
}
