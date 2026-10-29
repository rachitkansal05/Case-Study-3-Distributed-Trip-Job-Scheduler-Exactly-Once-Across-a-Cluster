package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.core.domain.FencingToken;
import java.time.Instant;
import java.time.OffsetDateTime;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Test-only snapshot of one {@code job_run} row plus the DB clock at the moment it was read
 * ({@code clock_timestamp()} in the same statement), for the failure-injection ITs.
 */
record ItRunRow(String state, String ownerNode, FencingToken token, Instant leaseUntil, Integer nextItem,
                Instant dbNow) {

    static ItRunRow read(JdbcClient jdbc, long runId) {
        return jdbc.sql("""
                SELECT state, owner_node, fencing_epoch, fencing_seq, lease_until,
                       (checkpoint ->> 'nextItem')::int AS next_item, clock_timestamp() AS db_now
                  FROM job_run WHERE id = ?""")
                .param(runId)
                .query((rs, n) -> {
                    long epoch = rs.getLong("fencing_epoch");
                    boolean hasToken = !rs.wasNull();
                    long seq = rs.getLong("fencing_seq");
                    OffsetDateTime lease = rs.getObject("lease_until", OffsetDateTime.class);
                    int next = rs.getInt("next_item");
                    Integer nextItem = rs.wasNull() ? null : next;
                    return new ItRunRow(rs.getString("state"), rs.getString("owner_node"),
                            hasToken ? new FencingToken(epoch, seq) : null,
                            lease == null ? null : lease.toInstant(), nextItem,
                            rs.getObject("db_now", OffsetDateTime.class).toInstant());
                })
                .single();
    }

    /** DB time now. */
    static Instant dbNow(JdbcClient jdbc) {
        return jdbc.sql("SELECT clock_timestamp()").query(OffsetDateTime.class).single().toInstant();
    }

    boolean running() {
        return "RUNNING".equals(state);
    }
}
