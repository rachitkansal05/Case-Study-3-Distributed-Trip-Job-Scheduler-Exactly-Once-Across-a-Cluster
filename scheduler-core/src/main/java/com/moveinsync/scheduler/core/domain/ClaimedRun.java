package com.moveinsync.scheduler.core.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * A run that this node has claimed and whose claim transaction has committed. From this point the
 * run is {@code RUNNING}, holds tenant slot {@link #slotNo()} and may act with {@link #token()}
 * until its lease expires.
 *
 * @param runId {@code job_run.id}
 * @param jobId {@code job_run.job_id}
 * @param tenantId owning tenant
 * @param partitionId {@code 0..127}
 * @param jobVersion job definition version the run was materialised from (cache key)
 * @param handler handler to execute
 * @param priority 1 = most urgent
 * @param slotNo tenant slot held while running
 * @param token fencing token of this claim
 * @param idemKey idempotency key of the run ({@code jobId:scheduledAtUTC} or {@code
 *     jobId:manual:requestId}); stable across attempts and replays
 * @param scheduledAt occurrence instant
 * @param attempt attempt number after this claim (1-based)
 * @param maxRunTime hard stop for this attempt
 * @param claimedAt DB time at which the claim took effect
 * @param leaseUntil DB time at which the lease expires unless renewed
 * @param checkpointJson last committed checkpoint as JSON, or {@code null}; resume from here
 * @param coalescedCount number of occurrences this run stands for (COALESCE), normally 1
 * @param recovered true if taken over by the orphan sweep; such runs go to the front of the
 *     dispatch queue and may use the reserved worker
 */
public record ClaimedRun(
        long runId,
        UUID jobId,
        TenantId tenantId,
        int partitionId,
        long jobVersion,
        HandlerType handler,
        int priority,
        int slotNo,
        FencingToken token,
        String idemKey,
        Instant scheduledAt,
        int attempt,
        Duration maxRunTime,
        Instant claimedAt,
        Instant leaseUntil,
        String checkpointJson,
        int coalescedCount,
        boolean recovered) {

    public ClaimedRun {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(token, "token");
        Objects.requireNonNull(idemKey, "idemKey");
        Objects.requireNonNull(scheduledAt, "scheduledAt");
        Objects.requireNonNull(maxRunTime, "maxRunTime");
        Objects.requireNonNull(claimedAt, "claimedAt");
        Objects.requireNonNull(leaseUntil, "leaseUntil");
        if (slotNo < 0) {
            throw new IllegalArgumentException("slotNo must be >= 0");
        }
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1 after a claim");
        }
    }
}
