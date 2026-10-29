package com.moveinsync.scheduler.app.repository.job;

import com.moveinsync.scheduler.core.domain.RunState;
import java.time.Instant;
import java.util.UUID;

/**
 * Operational view of one {@code job_run}: never the payload, the checkpoint or the error message,
 * only the error code.
 *
 * @param lagMillis {@code started_at - scheduled_at} once started, else null
 * @param errorCode leading {@code [A-Z0-9_]+} token of {@code last_error}, if any
 */
public record RunRow(
        long runId,
        UUID jobId,
        String tenantId,
        RunState state,
        int attempt,
        int leaseLostCount,
        Instant scheduledAt,
        Instant dueAt,
        Instant startedAt,
        Instant finishedAt,
        Long lagMillis,
        String errorCode,
        int coalescedCount,
        boolean manual) {
}
