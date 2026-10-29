package com.moveinsync.scheduler.app.repository.claim;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Performance guard for the claim: with a 50,000-run backlog for one tenant, the candidate query
 * must walk {@code run_claim_order} in claim order and stop after k rows instead of sorting the
 * whole backlog (found by TenantFairnessIT: claim transactions took 100-340 ms before V201).
 */
class ClaimQueryPlanIT {

    private static EngineTestDb db;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("claim_query_plan_it");
        Fixtures fx = new Fixtures(db.jdbc());
        fx.tenant("plan-a", 32);
        for (int j = 0; j < 50; j++) {
            UUID job = fx.job("plan-a").insert();
            fx.dueRuns(job, 1_000);
        }
        db.jdbc().sql("ANALYZE job_run").update();
    }

    @AfterAll
    static void close() {
        db.close();
    }

    @Test
    void candidateQueryUsesClaimOrderIndexAndStopsEarly() throws Exception {
        Integer[] partitions = IntStream.range(0, 43).boxed().toArray(Integer[]::new);
        StringBuilder plan = new StringBuilder();
        long execMs;
        try (Connection c = db.admin().getConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("EXPLAIN (ANALYZE, FORMAT TEXT) "
                    + JdbcClaimProtocol.CANDIDATES)) {
                ps.setArray(1, c.createArrayOf("int4", partitions));
                ps.setString(2, "plan-a");
                ps.setArray(3, c.createArrayOf("int8", new Object[0]));
                ps.setInt(4, 32);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        plan.append(rs.getString(1)).append('\n');
                    }
                }
            }
            c.rollback();
        }
        String text = plan.toString();
        execMs = Math.round(Double.parseDouble(text.replaceAll("(?s).*Execution Time: ([0-9.]+) ms.*", "$1")));
        assertThat(text).as(text).contains("run_claim_order").doesNotContain("Sort Method");
        assertThat(execMs).as(text).isLessThan(50);
    }
}
