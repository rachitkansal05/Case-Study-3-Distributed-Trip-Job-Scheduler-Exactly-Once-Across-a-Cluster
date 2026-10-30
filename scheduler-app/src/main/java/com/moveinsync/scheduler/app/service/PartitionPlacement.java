package com.moveinsync.scheduler.app.service;

import java.util.UUID;

/**
 * Chooses {@code job.partition_id} once, when a job is created. The fallback in
 * {@link ServiceDefaultsConfig} uses {@code Math.floorMod(jobId.hashCode(), 128)}; at merge time
 * it is replaced by Core's murmur3 {@code PartitionHasher} (docs/design/architecture.md section 06).
 */
public interface PartitionPlacement {

    /** Partition in {@code 0 .. partitions-1}. */
    int partitionFor(UUID jobId);
}
