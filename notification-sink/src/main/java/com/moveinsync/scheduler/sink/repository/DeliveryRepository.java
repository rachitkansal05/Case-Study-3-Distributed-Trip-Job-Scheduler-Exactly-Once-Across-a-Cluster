package com.moveinsync.scheduler.sink.repository;

import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.sink.domain.Delivery;
import com.moveinsync.scheduler.sink.domain.DeliveryCommand;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** {@code sink.delivery}: at most one row per idempotency key (primary key). */
@Repository
public class DeliveryRepository {

    private static final String COLUMNS = "run_id, fencing_epoch, fencing_seq, delivered_at";

    private final JdbcClient jdbc;

    public DeliveryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts the delivery unless the key is already delivered.
     *
     * @return the new row, or empty if a row for the key already existed (nothing written)
     */
    public Optional<Delivery> insertIfAbsent(DeliveryCommand cmd) {
        return jdbc.sql("""
                INSERT INTO sink.delivery
                       (idem_key, run_id, fencing_epoch, fencing_seq, channel, payload_sha256, delivered_at)
                VALUES (:key, :run, :epoch, :seq, :channel, :hash, clock_timestamp())
                ON CONFLICT (idem_key) DO NOTHING
                RETURNING\s""" + COLUMNS)
                .param("key", cmd.idemKey())
                .param("run", cmd.runId())
                .param("epoch", cmd.token().epoch())
                .param("seq", cmd.token().seq())
                .param("channel", cmd.channel().name())
                .param("hash", cmd.payloadSha256())
                .query(DeliveryRepository::map)
                .optional();
    }

    /** The delivery recorded for {@code idemKey}, if any. */
    public Optional<Delivery> find(String idemKey) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM sink.delivery WHERE idem_key = :key")
                .param("key", idemKey)
                .query(DeliveryRepository::map)
                .optional();
    }

    private static Delivery map(ResultSet rs, int rowNum) throws SQLException {
        return new Delivery(
                rs.getLong("run_id"),
                new FencingToken(rs.getLong("fencing_epoch"), rs.getLong("fencing_seq")),
                rs.getTimestamp("delivered_at").toInstant());
    }
}
