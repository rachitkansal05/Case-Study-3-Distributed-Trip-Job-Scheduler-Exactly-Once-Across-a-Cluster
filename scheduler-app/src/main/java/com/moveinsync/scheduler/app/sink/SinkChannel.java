package com.moveinsync.scheduler.app.sink;

import com.moveinsync.scheduler.core.domain.HandlerType;
import java.util.Objects;

/**
 * Delivery channel sent as the sink body's {@code channel} (docs/sink-api.md §1). The handler to
 * channel mapping lives only in {@link #forHandler(HandlerType)}.
 */
public enum SinkChannel {
    /** Text message to an employee. */
    SMS,
    /** Push notification to the operations app. */
    PUSH;

    /** Channel used for every effect of the given handler. */
    public static SinkChannel forHandler(HandlerType handler) {
        return switch (Objects.requireNonNull(handler, "handler")) {
            case PICKUP_ALERT, NO_SHOW_CANCEL -> SMS;
            case ROSTER_GEN, BILLING_CLOSE -> PUSH;
        };
    }
}
