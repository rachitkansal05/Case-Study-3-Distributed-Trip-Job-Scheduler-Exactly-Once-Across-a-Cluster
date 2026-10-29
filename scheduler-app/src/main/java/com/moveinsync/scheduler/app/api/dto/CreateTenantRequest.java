package com.moveinsync.scheduler.app.api.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * Body of {@code POST /api/v1/tenants} (OPERATOR).
 *
 * @param id       tenant id: letters, digits, {@code . _ -}; 1-64 characters
 * @param timeZone IANA region id, e.g. {@code Asia/Kolkata}
 * @param cap      concurrency cap (slot rows); defaults to {@code scheduler.tenant.default-cap}
 */
public record CreateTenantRequest(
        @NotNull @Pattern(regexp = "[A-Za-z0-9][A-Za-z0-9._-]{0,63}") String id,
        @NotBlank @Size(max = 64) String timeZone,
        @Min(0) @Max(CapRequest.MAX_CAP) Integer cap) {
}
