package com.moveinsync.scheduler.app.api.dto;

import com.moveinsync.scheduler.app.repository.job.RunRow;
import com.moveinsync.scheduler.core.domain.RunState;
import java.time.Instant;

/**
 * One run in the history: state, attempts, lag and error code. Never a payload or error message.
 *
 * @param lagMillis start lag ({@code started_at - scheduled_at}); null until started
 */
public record RunView(
        long runId,
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

    public static RunView of(RunRow r) {
        return new RunView(r.runId(), r.state(), r.attempt(), r.leaseLostCount(), r.scheduledAt(), r.dueAt(),
                r.startedAt(), r.finishedAt(), r.lagMillis(), r.errorCode(), r.coalescedCount(), r.manual());
    }
}
