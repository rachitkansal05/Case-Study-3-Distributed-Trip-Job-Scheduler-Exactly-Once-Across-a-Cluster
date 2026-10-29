package com.moveinsync.scheduler.sink.domain;

import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import java.util.Objects;

/**
 * Verdict for one delivery request.
 *
 * @param outcome what happened
 * @param delivery the delivery row for the key; {@code null} exactly when the outcome is
 *     {@link DeliveryOutcome#STALE_TOKEN}
 */
public record DeliveryResult(DeliveryOutcome outcome, Delivery delivery) {

    public DeliveryResult {
        Objects.requireNonNull(outcome, "outcome");
        if ((outcome == DeliveryOutcome.STALE_TOKEN) != (delivery == null)) {
            throw new IllegalArgumentException("delivery must be present exactly when the request was accepted");
        }
    }

    public static DeliveryResult stale() {
        return new DeliveryResult(DeliveryOutcome.STALE_TOKEN, null);
    }
}
