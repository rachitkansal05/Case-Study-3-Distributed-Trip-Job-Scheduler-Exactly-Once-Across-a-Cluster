package com.moveinsync.scheduler.app.api.dto;

/**
 * Response of {@code PUT /api/v1/tenants/{t}/cap}.
 *
 * @param configuredCap the new cap
 * @param effectiveCap  slot rows after the change; higher than configured while slots in use drain
 * @param draining      {@code effectiveCap > configuredCap}
 */
public record CapView(String tenantId, int configuredCap, int effectiveCap, boolean draining) {

    public static CapView of(String tenantId, int configured, int effective) {
        return new CapView(tenantId, configured, effective, effective > configured);
    }
}
