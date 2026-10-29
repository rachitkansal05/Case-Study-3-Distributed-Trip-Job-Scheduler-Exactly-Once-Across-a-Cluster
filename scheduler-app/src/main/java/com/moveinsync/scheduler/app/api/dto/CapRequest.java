package com.moveinsync.scheduler.app.api.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/** Body of {@code PUT /api/v1/tenants/{t}/cap}. */
public record CapRequest(@NotNull @Min(0) @Max(MAX_CAP) Integer cap) {

    /** Upper bound for a cap (slot numbers are SMALLINT). */
    public static final int MAX_CAP = 10_000;
}
