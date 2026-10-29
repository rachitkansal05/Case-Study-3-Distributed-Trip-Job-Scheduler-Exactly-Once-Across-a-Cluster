package com.moveinsync.scheduler.app.repository.cluster;

import com.moveinsync.scheduler.core.ports.PartitionLeaseStore;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import javax.sql.DataSource;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Partition leases in {@code partition_lease} (plan section 06). One conditional UPDATE per
 * partition, so contention is per row and spread over 128 rows. Leases only spread polling load;
 * correctness never depends on them (every run is fenced on its own).
 */
public final class JdbcPartitionLeaseStore implements PartitionLeaseStore {

    /*
     * Acquire or renew. Invariant: at most one node holds a valid lease per partition. The WHERE
     * clause is evaluated on the newest row version under the row lock, so of two concurrent
     * acquirers exactly one sees the row as free. A lease is free when it is mine, never owned /
     * released (owner_node IS NULL; the 128 seed rows start that way), or expired by DB time
     * (clock_timestamp(), never now(), which is the transaction start time).
     */
    private static final String ACQUIRE_OR_RENEW = """
            UPDATE partition_lease
               SET owner_node = :me, epoch = epoch + 1,
                   lease_until = clock_timestamp() + :leaseMs * interval '1 millisecond'
             WHERE partition_id = :p
               AND (owner_node = :me OR owner_node IS NULL OR lease_until < clock_timestamp())
            RETURNING partition_id, owner_node, epoch, lease_until
            """;

    /* Release only my own row, so a slow node can never release a partition someone else took. */
    private static final String RELEASE = """
            UPDATE partition_lease SET owner_node = NULL, lease_until = NULL
             WHERE partition_id = :p AND owner_node = :me
            """;

    /* Valid (unexpired by DB time) leases of one node. */
    private static final String OWNED_BY = """
            SELECT partition_id FROM partition_lease
             WHERE owner_node = :me AND lease_until >= clock_timestamp()
            """;

    private static final String FIND_ALL = """
            SELECT partition_id, owner_node, epoch, lease_until FROM partition_lease ORDER BY partition_id
            """;

    private final JdbcClient jdbc;

    public JdbcPartitionLeaseStore(DataSource dataSource) {
        this.jdbc = JdbcClient.create(Objects.requireNonNull(dataSource, "dataSource"));
    }

    @Override
    public Optional<PartitionLease> tryAcquireOrRenew(int partitionId, String nodeId, Duration leaseDuration) {
        return jdbc.sql(ACQUIRE_OR_RENEW)
                .param("me", nodeId)
                .param("p", partitionId)
                .param("leaseMs", leaseDuration.toMillis())
                .query(JdbcPartitionLeaseStore::map)
                .optional();
    }

    @Override
    public boolean release(int partitionId, String nodeId) {
        return jdbc.sql(RELEASE).param("p", partitionId).param("me", nodeId).update() == 1;
    }

    @Override
    public Set<Integer> ownedBy(String nodeId) {
        return new TreeSet<>(jdbc.sql(OWNED_BY).param("me", nodeId).query(Integer.class).list());
    }

    @Override
    public List<PartitionLease> findAll() {
        return jdbc.sql(FIND_ALL).query(JdbcPartitionLeaseStore::map).list();
    }

    private static PartitionLease map(ResultSet rs, int row) throws SQLException {
        OffsetDateTime until = rs.getObject("lease_until", OffsetDateTime.class);
        return new PartitionLease(rs.getInt("partition_id"), rs.getString("owner_node"), rs.getLong("epoch"),
                until == null ? null : until.toInstant());
    }
}
