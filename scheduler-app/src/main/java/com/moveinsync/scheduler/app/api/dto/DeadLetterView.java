package com.moveinsync.scheduler.app.api.dto;

import com.moveinsync.scheduler.core.domain.RunState;
import java.time.Instant;
import java.util.UUID;

/**
 * One dead letter: error code and counts only, never payloads or messages.
 *
 * @param runState the run's current state ({@code DEAD_LETTERED}, or later states after a replay)
 */
public record DeadLetterView(long runId, UUID jobId, String tenantId, String errorCode, int attempts,
                             Instant deadAt, Instant replayedAt, RunState runState) {
}
