package com.moveinsync.scheduler.sink.domain;

/**
 * Delivery counters across all runs.
 *
 * @param deliveredDistinct rows in {@code sink.delivery} (distinct keys delivered)
 * @param attempts rows in {@code sink.attempt_log}
 * @param delivered attempts with outcome {@code DELIVERED}
 * @param alreadyDelivered attempts with outcome {@code ALREADY_DELIVERED}
 * @param staleRejected attempts with outcome {@code STALE_TOKEN}
 * @param runs runs with a fence row (runs that had at least one accepted request)
 * @param maxDeliveriesPerKey largest number of delivery rows for one key: 1 whenever anything was
 *     delivered (the exactly-once proof), 0 when nothing was
 */
public record TotalStats(long deliveredDistinct, long attempts, long delivered, long alreadyDelivered,
                         long staleRejected, long runs, long maxDeliveriesPerKey) {
}
