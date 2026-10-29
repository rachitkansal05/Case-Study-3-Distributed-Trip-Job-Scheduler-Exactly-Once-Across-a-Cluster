package com.moveinsync.scheduler.app.api.dto;

/** Response of pause/resume: the tenant's new {@code paused} flag. */
public record TenantPauseView(String tenantId, boolean paused) {
}
