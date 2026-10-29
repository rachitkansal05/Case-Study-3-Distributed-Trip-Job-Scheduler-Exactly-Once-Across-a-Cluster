package com.moveinsync.scheduler.app.api.dto;

/**
 * A tenant with its cap numbers.
 *
 * @param configuredCap the cap the admin set
 * @param effectiveCap  slot rows; running runs never exceed it
 * @param draining      true while a lowered cap is still draining (effective &gt; configured)
 */
public record TenantView(String id, String timeZone, boolean paused, int configuredCap, int effectiveCap,
                         boolean draining) {
}
