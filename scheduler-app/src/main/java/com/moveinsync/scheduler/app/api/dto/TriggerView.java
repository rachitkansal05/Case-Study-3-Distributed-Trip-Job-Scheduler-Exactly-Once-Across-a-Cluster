package com.moveinsync.scheduler.app.api.dto;

import com.moveinsync.scheduler.core.domain.RunState;
import java.time.Instant;
import java.util.UUID;

/**
 * Result of a manual trigger.
 *
 * @param duplicate true when a run for this {@code Idempotency-Key} already existed and was returned
 */
public record TriggerView(long runId, UUID jobId, RunState state, Instant dueAt, boolean duplicate) {
}
