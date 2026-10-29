package com.moveinsync.scheduler.loadgen;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * One {@code job_run} row as read back after a benchmark: all instants are DB-clock epoch
 * microseconds, {@link #NONE} when the column is NULL.
 */
public record RunSample(long id, String tenant, int partition, long scheduledUs, long startedUs, long finishedUs,
                        String state, int attempt, int leaseLostCount) {

    public static final long NONE = Long.MIN_VALUE;
    public static final Set<String> TERMINAL = Set.of("SUCCEEDED", "SKIPPED", "DEAD_LETTERED", "CANCELLED");

    private static final String SELECT = """
            SELECT id, tenant_id, partition_id,
                   (extract(epoch FROM scheduled_at) * 1000000)::bigint AS s_us,
                   (extract(epoch FROM started_at) * 1000000)::bigint AS st_us,
                   (extract(epoch FROM finished_at) * 1000000)::bigint AS f_us,
                   state, attempt, lease_lost_count
              FROM job_run
             WHERE """;

    private static final String PROGRESS = """
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE state IN ('SUCCEEDED', 'SKIPPED', 'DEAD_LETTERED', 'CANCELLED')) AS terminal,
                   count(*) FILTER (WHERE state = 'RUNNING') AS running,
                   count(*) FILTER (WHERE started_at IS NOT NULL) AS started
              FROM job_run
             WHERE """;

    public boolean started() {
        return startedUs != NONE;
    }

    public boolean finished() {
        return finishedUs != NONE;
    }

    public boolean terminal() {
        return TERMINAL.contains(state);
    }

    /** {@code started_at - scheduled_at} in microseconds; only meaningful when {@link #started()}. */
    public long lagUs() {
        return startedUs - scheduledUs;
    }

    /** Runs matching {@code where} (SQL after WHERE, with {@code ?} parameters). */
    public static List<RunSample> load(Connection c, String where, Object... params) throws SQLException {
        List<RunSample> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(SELECT + " " + where + " ORDER BY id")) {
            bind(c, ps, params);
            ps.setFetchSize(10_000);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new RunSample(rs.getLong(1), rs.getString(2), rs.getInt(3), rs.getLong(4),
                            nullable(rs, 5), nullable(rs, 6), rs.getString(7), rs.getInt(8), rs.getInt(9)));
                }
            }
        }
        return out;
    }

    /** {total, terminal, running, started} of the runs matching {@code where}. */
    public static long[] progress(Connection c, String where, Object... params) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(PROGRESS + " " + where)) {
            bind(c, ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return new long[] {rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getLong(4)};
            }
        }
    }

    /** Raw lags as CSV: run id, tenant, partition, scheduled (epoch us), lag ms, state, attempt, lease losses. */
    public static void writeCsv(Path file, List<RunSample> samples) throws IOException {
        Files.createDirectories(file.toAbsolutePath().getParent());
        try (Writer w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            w.write("run_id,tenant,partition,scheduled_at_us,lag_ms,state,attempt,lease_lost_count\n");
            for (RunSample s : samples) {
                w.write(s.id + "," + s.tenant + "," + s.partition + "," + s.scheduledUs + ","
                        + (s.started() ? MarkdownTable.f1(s.lagUs() / 1000.0) : "") + "," + s.state + ","
                        + s.attempt + "," + s.leaseLostCount + "\n");
            }
        }
    }

    private static void bind(Connection c, PreparedStatement ps, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            Object p = params[i];
            if (p instanceof String[] arr) {
                ps.setArray(i + 1, c.createArrayOf("text", arr));
            } else {
                ps.setObject(i + 1, p);
            }
        }
    }

    private static long nullable(ResultSet rs, int col) throws SQLException {
        long v = rs.getLong(col);
        return rs.wasNull() ? NONE : v;
    }
}
