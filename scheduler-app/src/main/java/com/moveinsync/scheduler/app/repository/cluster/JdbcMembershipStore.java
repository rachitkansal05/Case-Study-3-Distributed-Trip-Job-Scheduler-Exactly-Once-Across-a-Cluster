package com.moveinsync.scheduler.app.repository.cluster;

import com.moveinsync.scheduler.core.ports.MembershipStore;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Membership in {@code scheduler_node} (plan section 06). Each node writes only its own row, so
 * heartbeats never contend. Liveness is judged by DB time only.
 */
public final class JdbcMembershipStore implements MembershipStore {

    /* Heartbeat on the DB clock; started_at is set once, on first registration. */
    private static final String HEARTBEAT = """
            INSERT INTO scheduler_node (node_id, heartbeat_at, started_at)
            VALUES (:n, clock_timestamp(), clock_timestamp())
            ON CONFLICT (node_id) DO UPDATE SET heartbeat_at = clock_timestamp()
            """;

    /* Live = heartbeat younger than deadAfter by DB time (clock_timestamp(), not node time). */
    private static final String LIVE = """
            SELECT node_id, heartbeat_at, started_at FROM scheduler_node
             WHERE heartbeat_at > clock_timestamp() - :deadMs * interval '1 millisecond'
             ORDER BY node_id
            """;

    private static final String DEREGISTER = "DELETE FROM scheduler_node WHERE node_id = :n";

    private final JdbcClient jdbc;

    public JdbcMembershipStore(DataSource dataSource) {
        this.jdbc = JdbcClient.create(Objects.requireNonNull(dataSource, "dataSource"));
    }

    @Override
    public void heartbeat(String nodeId) {
        jdbc.sql(HEARTBEAT).param("n", nodeId).update();
    }

    @Override
    public List<NodeInfo> liveNodes(Duration deadAfter) {
        return jdbc.sql(LIVE).param("deadMs", deadAfter.toMillis())
                .query((rs, i) -> new NodeInfo(rs.getString("node_id"),
                        rs.getObject("heartbeat_at", OffsetDateTime.class).toInstant(),
                        rs.getObject("started_at", OffsetDateTime.class).toInstant()))
                .list();
    }

    @Override
    public void deregister(String nodeId) {
        jdbc.sql(DEREGISTER).param("n", nodeId).update();
    }
}
