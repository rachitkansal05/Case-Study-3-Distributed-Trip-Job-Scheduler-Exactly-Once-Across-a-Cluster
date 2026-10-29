/**
 * Partition hashing ({@code murmur3(jobId) mod 128}) and rendezvous assignment
 * ({@code PartitionAssigner}), plan section 06. Owned by the Core team.
 *
 * <ul>
 *   <li>{@link com.moveinsync.scheduler.core.cluster.PartitionHasher}: job id to fixed partition,
 *       with pinned cross-team vectors.
 *   <li>{@link com.moveinsync.scheduler.core.cluster.PartitionAssigner}: desired owner per partition
 *       over the live set (highest random weight, exact minimal movement).
 *   <li>{@link com.moveinsync.scheduler.core.cluster.PartitionPlanner}: pure per-tick decisions of
 *       the partition loop (acquire, renew, release, keep), including treating an expired lease's
 *       owner as dead for that partition at once (plan section 21, issue 1).
 * </ul>
 */
package com.moveinsync.scheduler.core.cluster;
