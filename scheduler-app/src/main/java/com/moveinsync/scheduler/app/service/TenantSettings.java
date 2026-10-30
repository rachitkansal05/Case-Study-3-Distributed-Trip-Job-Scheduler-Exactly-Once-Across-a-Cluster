package com.moveinsync.scheduler.app.service;

import java.time.ZoneId;

/**
 * Cacheable tenant configuration: only the time zone. Caps and the pause flag are deliberately
 * not here, because they must never be cached (docs/design/architecture.md section 14).
 */
public record TenantSettings(String tenantId, ZoneId zone) {
}
