package com.moveinsync.scheduler.sink.domain;

import com.moveinsync.scheduler.core.domain.FencingToken;
import java.util.Objects;

/**
 * A validated delivery request. Holds only the SHA-256 of the payload, never the payload.
 *
 * @param runId run the effect belongs to (fencing is per run)
 * @param idemKey value of the {@code Idempotency-Key} header
 * @param token value of the {@code Fencing-Token} header
 * @param channel delivery channel
 * @param payloadSha256 lower-case hex SHA-256 of the payload
 */
public record DeliveryCommand(long runId, String idemKey, FencingToken token, Channel channel, String payloadSha256) {

    public DeliveryCommand {
        Objects.requireNonNull(idemKey, "idemKey");
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(payloadSha256, "payloadSha256");
    }
}
