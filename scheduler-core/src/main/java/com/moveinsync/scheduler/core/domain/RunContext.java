package com.moveinsync.scheduler.core.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * What a handler receives for one attempt (plan section 07, "Execute"). Every outbound effect must
 * send {@code Idempotency-Key} ({@link #idemKey()} or {@link #itemKey(String)}) and {@code
 * Fencing-Token} ({@link FencingToken#toHeader()}).
 *
 * @param runId {@code job_run.id}
 * @param jobId {@code job_run.job_id}
 * @param tenantId owning tenant
 * @param attempt 1-based attempt number
 * @param idemKey run idempotency key, stable across attempts and replays
 * @param fencingToken token of the current claim
 * @param checkpointJson last committed checkpoint as JSON, or {@code null} to start from the
 *     beginning
 * @param deadline hard stop of this attempt ({@code max_run_time} after the claim); past it the
 *     attempt fails with a timeout. Self-fencing against the lease uses the separate monotonic
 *     lease clock, not this value.
 */
public record RunContext(
        long runId,
        UUID jobId,
        TenantId tenantId,
        int attempt,
        String idemKey,
        FencingToken fencingToken,
        String checkpointJson,
        Instant deadline) {

    public RunContext {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(idemKey, "idemKey");
        Objects.requireNonNull(fencingToken, "fencingToken");
        Objects.requireNonNull(deadline, "deadline");
    }

    /** Per-item idempotency key for chunked handlers: {@code {idemKey}:{itemId}}. */
    public String itemKey(String itemId) {
        Objects.requireNonNull(itemId, "itemId");
        return idemKey + ":" + itemId;
    }
}
