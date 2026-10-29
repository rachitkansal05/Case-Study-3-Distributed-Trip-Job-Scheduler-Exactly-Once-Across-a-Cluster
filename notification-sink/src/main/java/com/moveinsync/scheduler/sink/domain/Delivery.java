package com.moveinsync.scheduler.sink.domain;

import com.moveinsync.scheduler.core.domain.FencingToken;
import java.time.Instant;

/**
 * One row of {@code sink.delivery}: the single effect recorded for an idempotency key.
 *
 * @param runId run that delivered it
 * @param token token the delivering request carried
 * @param deliveredAt DB time of the delivery
 */
public record Delivery(long runId, FencingToken token, Instant deliveredAt) {
}
