package com.moveinsync.scheduler.app.repository.claim;

import com.moveinsync.scheduler.core.domain.FencingToken;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reads the fencing epoch {@code cluster_generation * 2^32 + timeline} on a given connection, so a
 * claim takes it inside its own transaction (plan section 07, "Fencing token across failover").
 *
 * <p>The timeline is the first 8 hex digits of {@code pg_walfile_name(pg_current_wal_lsn())}. It
 * rises on every promotion, so tokens minted after a failover beat every older token. On a standby
 * {@code pg_current_wal_lsn()} raises an error, which fails the claim; a NULL or non-positive
 * timeline is rejected by {@link FencingToken#epochOf}. Either way the claim starts nothing.
 */
public final class FencingEpochReader {

    /* Invariant: the epoch never goes backwards across failover (F5); read in the claim's txn. */
    static final String EPOCH_SQL = """
            SELECT m.cluster_generation, pg_walfile_name(pg_current_wal_lsn()) AS walfile
              FROM cluster_meta m
            """;

    /** Thrown when the epoch cannot be determined; callers must fail closed. */
    public static final class EpochUnavailableException extends ClaimStoreException {
        private static final long serialVersionUID = 1L;

        EpochUnavailableException(String message) {
            super(message);
        }
    }

    /**
     * Current epoch.
     *
     * @throws EpochUnavailableException if the row or timeline is missing or out of range
     * @throws SQLException if the query fails (e.g. on a standby)
     */
    public long read(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(EPOCH_SQL);
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                throw new EpochUnavailableException("cluster_meta row missing");
            }
            return epochOf(rs.getInt("cluster_generation"), rs.getString("walfile"));
        }
    }

    /** {@code epochOf(generation, timeline(walfile))}, failing closed on a bad WAL file name. */
    static long epochOf(int generation, String walfile) {
        if (walfile == null || walfile.length() < 8) {
            throw new EpochUnavailableException("timeline unavailable");
        }
        long timeline;
        try {
            timeline = Long.parseLong(walfile.substring(0, 8), 16);
        } catch (NumberFormatException e) {
            throw new EpochUnavailableException("timeline unreadable");
        }
        try {
            return FencingToken.epochOf(generation, timeline);
        } catch (IllegalArgumentException e) {
            throw new EpochUnavailableException("epoch out of range: " + e.getMessage());
        }
    }
}
