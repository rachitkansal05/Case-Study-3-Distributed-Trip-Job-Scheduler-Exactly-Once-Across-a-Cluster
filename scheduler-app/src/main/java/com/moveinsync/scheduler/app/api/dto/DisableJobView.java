package com.moveinsync.scheduler.app.api.dto;

import java.util.UUID;

/** Result of {@code DELETE /api/v1/jobs/{id}}: the job is disabled and unstarted runs cancelled. */
public record DisableJobView(UUID jobId, boolean enabled, int cancelledRuns) {
}
