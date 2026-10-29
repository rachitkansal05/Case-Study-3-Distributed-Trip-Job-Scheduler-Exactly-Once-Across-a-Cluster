package com.moveinsync.scheduler.core.ports;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Partition leases (table {@code partition_lease}, 128 rows; plan section 06). Leases only spread
 * polling load: correctness never depends on them, because claims are fenced per run.
 */
public interface PartitionLeaseStore {

    /**
     * Snapshot of one lease row.
     *
     * @param partitionId {@code 0..127}
     * @param ownerNode current owner, or {@code null} if released / never owned
     * @param epoch incremented on every acquire or renew
     * @param leaseUntil DB time the lease expires, or {@code null}
     */
    record PartitionLease(int partitionId, String ownerNode, long epoch, Instant leaseUntil) {
    }

    /**
     * Acquires or renews a partition with one conditional row update:
     *
     * <pre>{@code
     * UPDATE partition_lease
     *    SET owner_node = :me, epoch = epoch + 1, lease_until = clock_timestamp() + :leaseDuration
     *  WHERE partition_id = :p
     *    AND (owner_node = :me OR owner_node IS NULL OR lease_until < clock_timestamp())
     * }</pre>
     *
     * @return the updated lease if this node now owns the partition; empty if another node holds a
     *     valid lease
     */
    Optional<PartitionLease> tryAcquireOrRenew(int partitionId, String nodeId, Duration leaseDuration);

    /**
     * Releases a partition this node no longer wants ({@code owner_node = NULL, lease_until =
     * NULL} where {@code owner_node = :me}), so the desired owner can take it at once.
     *
     * @return {@code true} if this node held it
     */
    boolean release(int partitionId, String nodeId);

    /** Partitions whose lease is held by {@code nodeId} and not expired (DB time). */
    Set<Integer> ownedBy(String nodeId);

    /** All 128 lease rows, ordered by partition id. */
    List<PartitionLease> findAll();
}
