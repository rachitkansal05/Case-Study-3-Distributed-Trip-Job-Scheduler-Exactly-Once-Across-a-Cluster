package com.moveinsync.scheduler.sink.repository;

import com.moveinsync.scheduler.sink.domain.RunStats;
import com.moveinsync.scheduler.sink.domain.TotalStats;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Read-only counters over {@code sink.delivery}, {@code sink.attempt_log} and {@code sink.run_fence}. */
@Repository
public class StatsRepository {

    private static final String OUTCOME_COUNTS = """
            count(*)                                              AS attempts,
            count(*) FILTER (WHERE outcome = 'DELIVERED')         AS delivered,
            count(*) FILTER (WHERE outcome = 'ALREADY_DELIVERED') AS already_delivered,
            count(*) FILTER (WHERE outcome = 'STALE_TOKEN')       AS stale_rejected""";

    private final JdbcClient jdbc;

    public StatsRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Counters of one run; all zero for an unknown run. */
    public RunStats forRun(long runId) {
        return jdbc.sql("SELECT (SELECT count(*) FROM sink.delivery WHERE run_id = :run) AS delivered_distinct, "
                        + OUTCOME_COUNTS + " FROM sink.attempt_log WHERE run_id = :run")
                .param("run", runId)
                .query((rs, n) -> new RunStats(runId,
                        rs.getLong("delivered_distinct"),
                        rs.getLong("attempts"),
                        rs.getLong("delivered"),
                        rs.getLong("already_delivered"),
                        rs.getLong("stale_rejected")))
                .single();
    }

    /** Counters across all runs. Scans the tables: meant for tests and demos, not hot paths. */
    public TotalStats totals() {
        return jdbc.sql("SELECT (SELECT count(*) FROM sink.delivery) AS delivered_distinct, "
                        + "(SELECT count(*) FROM sink.run_fence) AS runs, "
                        + "(SELECT coalesce(max(c), 0) FROM (SELECT count(*) AS c FROM sink.delivery "
                        + "GROUP BY idem_key) per_key) AS max_per_key, "
                        + OUTCOME_COUNTS + " FROM sink.attempt_log")
                .query((rs, n) -> new TotalStats(
                        rs.getLong("delivered_distinct"),
                        rs.getLong("attempts"),
                        rs.getLong("delivered"),
                        rs.getLong("already_delivered"),
                        rs.getLong("stale_rejected"),
                        rs.getLong("runs"),
                        rs.getLong("max_per_key")))
                .single();
    }
}
