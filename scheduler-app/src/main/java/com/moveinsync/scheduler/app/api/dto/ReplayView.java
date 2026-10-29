package com.moveinsync.scheduler.app.api.dto;

import com.moveinsync.scheduler.core.domain.RunState;
import java.time.Instant;
import java.util.UUID;

/**
 * Result of a replay: the same run, {@code SCHEDULED} and due now, with its original
 * {@code idemKey}.
 */
public record ReplayView(long runId, UUID jobId, RunState state, Instant dueAt, String idemKey,
                         Instant replayedAt) {
}
