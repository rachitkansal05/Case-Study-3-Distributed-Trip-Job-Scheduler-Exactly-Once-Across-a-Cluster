package com.moveinsync.scheduler.sink.domain;

import com.moveinsync.scheduler.core.domain.FencingToken;
import java.util.Objects;

/**
 * What the sink does with an incoming token, given the highest token already accepted for the run
 * (plan section 07, "Sink fencing"). Tokens compare as a pair, epoch first, then sequence.
 */
public enum FenceDecision {
    /** Incoming token is lower than the fence: reject with 409, deliver nothing. */
    REJECT_STALE,
    /** Incoming token equals the fence: a retry by the current holder; accept, fence unchanged. */
    ACCEPT_EQUAL,
    /** Incoming token is higher than the fence: accept and raise the fence to it. */
    ACCEPT_RAISE;

    /**
     * Decides for {@code incoming} against {@code fence}.
     *
     * @param fence highest token accepted so far for the run
     * @param incoming token of the request
     */
    public static FenceDecision decide(FencingToken fence, FencingToken incoming) {
        int cmp = Objects.requireNonNull(incoming, "incoming").compareTo(Objects.requireNonNull(fence, "fence"));
        if (cmp < 0) {
            return REJECT_STALE;
        }
        return cmp == 0 ? ACCEPT_EQUAL : ACCEPT_RAISE;
    }

    /** True unless the request is rejected. */
    public boolean accepted() {
        return this != REJECT_STALE;
    }
}
