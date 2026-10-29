package com.moveinsync.scheduler.sink.repository;

import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.sink.domain.DeliveryCommand;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** {@code sink.attempt_log}: one row per validated delivery request, whatever the outcome. */
@Repository
public class AttemptLogRepository {

    private final JdbcClient jdbc;

    public AttemptLogRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void record(DeliveryCommand cmd, DeliveryOutcome outcome) {
        jdbc.sql("""
                INSERT INTO sink.attempt_log (run_id, idem_key, fencing_epoch, fencing_seq, outcome, at)
                VALUES (:run, :key, :epoch, :seq, :outcome, clock_timestamp())""")
                .param("run", cmd.runId())
                .param("key", cmd.idemKey())
                .param("epoch", cmd.token().epoch())
                .param("seq", cmd.token().seq())
                .param("outcome", outcome.name())
                .update();
    }
}
