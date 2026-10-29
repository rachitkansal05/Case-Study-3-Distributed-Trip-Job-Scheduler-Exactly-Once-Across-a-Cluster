package com.moveinsync.scheduler.sink.repository;

import com.moveinsync.scheduler.core.domain.FencingToken;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** {@code sink.run_fence}: highest accepted token per run. */
@Repository
public class RunFenceRepository {

    private final JdbcClient jdbc;

    public RunFenceRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Creates the fence row with {@code initial} if the run has none, then locks the row
     * ({@code FOR UPDATE}) until the surrounding transaction ends and returns its current value.
     * Concurrent requests for the same run are serialised here. Must run inside a transaction.
     */
    public FencingToken lockOrCreate(long runId, FencingToken initial) {
        jdbc.sql("""
                INSERT INTO sink.run_fence (run_id, max_epoch, max_seq)
                VALUES (:run, :epoch, :seq)
                ON CONFLICT (run_id) DO NOTHING""")
                .param("run", runId)
                .param("epoch", initial.epoch())
                .param("seq", initial.seq())
                .update();
        return jdbc.sql("SELECT max_epoch, max_seq FROM sink.run_fence WHERE run_id = :run FOR UPDATE")
                .param("run", runId)
                .query((rs, n) -> new FencingToken(rs.getLong("max_epoch"), rs.getLong("max_seq")))
                .single();
    }

    /** Sets the fence to {@code token}. The caller holds the row lock and has checked it is higher. */
    public void raise(long runId, FencingToken token) {
        jdbc.sql("UPDATE sink.run_fence SET max_epoch = :epoch, max_seq = :seq WHERE run_id = :run")
                .param("run", runId)
                .param("epoch", token.epoch())
                .param("seq", token.seq())
                .update();
    }

    /** Highest {@code max_epoch} ever accepted, or 0 when no run has been fenced yet. */
    public long highestEpoch() {
        return jdbc.sql("SELECT coalesce(max(max_epoch), 0) FROM sink.run_fence").query(Long.class).single();
    }
}
