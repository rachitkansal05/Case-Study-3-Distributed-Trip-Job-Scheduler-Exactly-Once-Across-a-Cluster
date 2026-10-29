package com.moveinsync.scheduler.app.service;

import com.moveinsync.scheduler.app.api.dto.ClusterView;
import com.moveinsync.scheduler.app.config.SchedulerProperties;
import com.moveinsync.scheduler.app.repository.cluster.ClusterViewRepository;
import com.moveinsync.scheduler.app.repository.cluster.ClusterViewRepository.PartitionRow;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Read-only cluster overview (OPERATOR). */
@Service
public class ClusterService {

    private final ClusterViewRepository cluster;
    private final SchedulerProperties properties;

    public ClusterService(ClusterViewRepository cluster, SchedulerProperties properties) {
        this.cluster = cluster;
        this.properties = properties;
    }

    /** Liveness uses {@code scheduler.membership.dead-after}, on the database clock. */
    @Transactional(readOnly = true)
    public ClusterView overview() {
        var nodes = cluster.liveNodes(properties.membership().deadAfter());
        var partitions = cluster.partitions();
        int owned = (int) partitions.stream().filter(PartitionRow::leaseValid).count();
        return new ClusterView(cluster.databaseNow(), nodes, owned, partitions.size() - owned, partitions);
    }
}
