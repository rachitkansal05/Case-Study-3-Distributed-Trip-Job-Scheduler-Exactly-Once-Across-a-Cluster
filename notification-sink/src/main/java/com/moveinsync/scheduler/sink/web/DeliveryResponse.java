package com.moveinsync.scheduler.sink.web;

import com.moveinsync.scheduler.sink.domain.DeliveryResult;
import java.time.Instant;

/**
 * Body of a successful delivery response (201 or 200). For {@code ALREADY_DELIVERED} the fields
 * describe the original delivery of the key, not the current request.
 *
 * @param outcome {@code DELIVERED} or {@code ALREADY_DELIVERED}
 * @param runId run that delivered the key
 * @param fencingToken token of the delivering request, {@code "<epoch>.<seq>"}
 * @param deliveredAt sink DB time of the delivery
 */
public record DeliveryResponse(String outcome, long runId, String fencingToken, Instant deliveredAt) {

    static DeliveryResponse of(DeliveryResult result) {
        return new DeliveryResponse(result.outcome().name(), result.delivery().runId(),
                result.delivery().token().toHeader(), result.delivery().deliveredAt());
    }
}
