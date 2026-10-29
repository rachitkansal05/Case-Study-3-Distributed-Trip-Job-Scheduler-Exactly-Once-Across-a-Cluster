package com.moveinsync.scheduler.app.api;

import com.moveinsync.scheduler.app.api.dto.ClusterView;
import com.moveinsync.scheduler.app.service.ClusterService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** {@code GET /api/v1/cluster} (OPERATOR): live nodes, partition owners, lease epochs. */
@RestController
public class ClusterController {

    private final ClusterService cluster;

    public ClusterController(ClusterService cluster) {
        this.cluster = cluster;
    }

    @GetMapping("/api/v1/cluster")
    public ClusterView cluster() {
        return cluster.overview();
    }
}
