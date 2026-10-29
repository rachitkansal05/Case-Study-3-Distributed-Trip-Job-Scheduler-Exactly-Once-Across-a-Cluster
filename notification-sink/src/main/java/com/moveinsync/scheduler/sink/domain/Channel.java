package com.moveinsync.scheduler.sink.domain;

import java.util.Optional;

/** Delivery channel of the mock provider ({@code sink.delivery.channel}). */
public enum Channel {
    SMS,
    PUSH;

    /** Exact, case-sensitive lookup; empty for anything else. */
    public static Optional<Channel> of(String value) {
        if (value == null) {
            return Optional.empty();
        }
        for (Channel c : values()) {
            if (c.name().equals(value)) {
                return Optional.of(c);
            }
        }
        return Optional.empty();
    }
}
