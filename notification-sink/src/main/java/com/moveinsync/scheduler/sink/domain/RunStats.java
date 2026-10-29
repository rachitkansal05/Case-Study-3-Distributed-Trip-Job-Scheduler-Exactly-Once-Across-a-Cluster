package com.moveinsync.scheduler.sink.domain;

/**
 * Delivery counters of one run. Always {@code attempts = delivered + alreadyDelivered +
 * staleRejected} and {@code deliveredDistinct = delivered}.
 *
 * @param runId the run
 * @param deliveredDistinct rows in {@code sink.delivery} for the run (distinct keys delivered)
 * @param attempts rows in {@code sink.attempt_log} for the run (validated requests)
 * @param delivered attempts with outcome {@code DELIVERED}
 * @param alreadyDelivered attempts with outcome {@code ALREADY_DELIVERED}
 * @param staleRejected attempts with outcome {@code STALE_TOKEN}
 */
public record RunStats(long runId, long deliveredDistinct, long attempts, long delivered, long alreadyDelivered,
                       long staleRejected) {
}
