package com.moveinsync.scheduler.app.repository.cluster;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Read-only view of {@code scheduler_node} and {@code partition_lease} for
 * {@code GET /api/v1/cluster} (OPERATOR). Cluster data belongs to no tenant. The Engine's
 * {@code JdbcMembershipStore} and {@code JdbcPartitionLeaseStore} remain the only writers; this
 * class never writes. Liveness and lease validity are judged on the database clock.
 */
@Repository
public class ClusterViewRepository {

    private final JdbcClient jdbc;

    public ClusterViewRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Instant databaseNow() {
        return jdbc.sql("SELECT clock_timestamp()").query(Timestamp.class).single().toInstant();
    }

    /** Nodes whose heartbeat is younger than {@code deadAfter}, with how many valid partition leases each holds. */
    public List<NodeRow> liveNodes(Duration deadAfter) {
        return jdbc.sql("""
                        SELECT n.node_id, n.heartbeat_at, n.started_at,
                               (SELECT count(*) FROM partition_lease p
                                 WHERE p.owner_node = n.node_id AND p.lease_until >= clock_timestamp()) AS owned
                          FROM scheduler_node n
                         WHERE n.heartbeat_at > clock_timestamp() - :deadAfterMs * interval '1 millisecond'
                         ORDER BY n.node_id""")
                .param("deadAfterMs", deadAfter.toMillis())
                .query((rs, i) -> new NodeRow(rs.getString("node_id"), rs.getTimestamp("heartbeat_at").toInstant(),
                        rs.getTimestamp("started_at").toInstant(), rs.getInt("owned")))
                .list();
    }

    public List<PartitionRow> partitions() {
        return jdbc.sql("""
                        SELECT partition_id, owner_node, epoch, lease_until,
                               owner_node IS NOT NULL AND lease_until >= clock_timestamp() AS valid
                          FROM partition_lease
                         ORDER BY partition_id""")
                .query((rs, i) -> {
                    Timestamp until = rs.getTimestamp("lease_until");
                    return new PartitionRow(rs.getInt("partition_id"), rs.getString("owner_node"), rs.getLong("epoch"),
                            until == null ? null : until.toInstant(), rs.getBoolean("valid"));
                })
                .list();
    }

    public record NodeRow(String nodeId, Instant heartbeatAt, Instant startedAt, int partitionsOwned) {
    }

    public record PartitionRow(int partitionId, String ownerNode, long epoch, Instant leaseUntil, boolean leaseValid) {
    }
}
