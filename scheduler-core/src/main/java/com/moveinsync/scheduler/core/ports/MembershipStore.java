package com.moveinsync.scheduler.core.ports;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Cluster membership (table {@code scheduler_node}; plan section 06). Each node writes only its
 * own row, so nodes do not contend. Liveness is judged by DB time.
 */
public interface MembershipStore {

    /**
     * One member.
     *
     * @param nodeId node identifier ({@code scheduler.node-id})
     * @param heartbeatAt last heartbeat, DB time
     * @param startedAt when the node first registered, DB time
     */
    record NodeInfo(String nodeId, Instant heartbeatAt, Instant startedAt) {
    }

    /**
     * Upserts this node's row with {@code heartbeat_at = clock_timestamp()} (and {@code started_at}
     * on first insert). Called every heartbeat interval (default 2 s).
     */
    void heartbeat(String nodeId);

    /**
     * Nodes whose heartbeat is younger than {@code deadAfter} by DB time (default 6 s), ordered by
     * node id.
     */
    List<NodeInfo> liveNodes(Duration deadAfter);

    /** Removes this node's row on graceful shutdown so others rebalance at once. */
    void deregister(String nodeId);
}
