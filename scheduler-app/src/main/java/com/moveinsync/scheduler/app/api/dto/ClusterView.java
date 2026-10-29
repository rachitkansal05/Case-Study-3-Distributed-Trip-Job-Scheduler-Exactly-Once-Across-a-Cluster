package com.moveinsync.scheduler.app.api.dto;

import com.moveinsync.scheduler.app.repository.cluster.ClusterViewRepository.NodeRow;
import com.moveinsync.scheduler.app.repository.cluster.ClusterViewRepository.PartitionRow;
import java.time.Instant;
import java.util.List;

/**
 * Cluster state for operators: live nodes, every partition's owner and lease epoch.
 *
 * @param observedAt        database time of the snapshot
 * @param ownedPartitions   partitions with a valid lease (should be 128 within a few seconds)
 * @param unownedPartitions partitions without a valid lease
 */
public record ClusterView(Instant observedAt, List<NodeRow> liveNodes, int ownedPartitions,
                          int unownedPartitions, List<PartitionRow> partitions) {
}
