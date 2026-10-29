package com.moveinsync.scheduler.core.policy;

import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code COALESCE}: of the {@code n} misfired occurrences, the <i>newest</i> fires once with
 * {@code coalesced_count = n} and the other {@code n - 1} become {@code SKIPPED}. On-time
 * occurrences still fire individually with count 1. With a single misfired occurrence nothing is
 * absorbed and it fires with count 1. Good for work where only the latest state matters (roster
 * regeneration).
 */
public final class Coalesce implements MisfirePolicy {

    /** The singleton instance. */
    public static final Coalesce INSTANCE = new Coalesce();

    private Coalesce() {}

    @Override
    public MisfirePolicyType type() {
        return MisfirePolicyType.COALESCE;
    }

    @Override
    public MisfireDecision decide(List<Instant> dueOccurrences, Instant now, Duration threshold) {
        MisfireSupport.Split split = MisfireSupport.split(dueOccurrences, now, threshold);
        List<Instant> misfired = split.misfired();
        if (misfired.isEmpty()) {
            return MisfireDecision.fireAll(split.onTime());
        }
        List<Instant> toFire = new ArrayList<>(split.onTime().size() + 1);
        toFire.add(misfired.get(misfired.size() - 1));
        toFire.addAll(split.onTime());
        return new MisfireDecision(toFire, misfired.subList(0, misfired.size() - 1), misfired.size());
    }

    @Override
    public String toString() {
        return "Coalesce";
    }
}
