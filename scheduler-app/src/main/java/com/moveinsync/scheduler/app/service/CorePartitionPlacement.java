package com.moveinsync.scheduler.app.service;

import com.moveinsync.scheduler.app.config.SchedulerProperties;
import com.moveinsync.scheduler.core.cluster.PartitionHasher;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** {@link PartitionPlacement} using Core's murmur3 {@link PartitionHasher} (PLAN.md section 06). */
@Component
public class CorePartitionPlacement implements PartitionPlacement {

    private final int partitions;

    @Autowired
    public CorePartitionPlacement(SchedulerProperties properties) {
        this(properties.partitions());
    }

    CorePartitionPlacement(int partitions) {
        this.partitions = partitions;
    }

    @Override
    public int partitionFor(UUID jobId) {
        return PartitionHasher.partitionOf(jobId, partitions);
    }
}
