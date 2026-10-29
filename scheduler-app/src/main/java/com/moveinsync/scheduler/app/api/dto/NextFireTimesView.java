package com.moveinsync.scheduler.app.api.dto;

import com.moveinsync.scheduler.app.service.FireTimeService.FireTimeView;
import java.util.List;
import java.util.UUID;

/** Next fire instants of a job, each with the tenant-local time and offset. Empty when disabled. */
public record NextFireTimesView(UUID jobId, String timeZone, List<FireTimeView> fireTimes) {
}
