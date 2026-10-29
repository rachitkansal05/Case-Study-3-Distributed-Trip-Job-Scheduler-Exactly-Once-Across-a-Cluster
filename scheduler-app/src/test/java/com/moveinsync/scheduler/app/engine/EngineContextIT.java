package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.moveinsync.scheduler.app.support.AbstractEmbeddedPostgresIT;
import com.moveinsync.scheduler.app.support.EmbeddedPostgresSupport;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

/**
 * The engine boots inside the real Spring context when {@code scheduler.engine.enabled=true}:
 * the node starts, owns all 128 partitions, and its metrics appear on {@code /actuator/prometheus}.
 * The epoch guard is off because no sink runs here. {@code @DirtiesContext} closes this context
 * (and stops the node gracefully) so it never keeps running under other test classes, and the
 * context uses its own database so the running engine never touches other tests' rows.
 */
@AutoConfigureObservability
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
    "scheduler.engine.enabled=true",
    "scheduler.engine.epoch-guard.enabled=false",
    "scheduler.node-id=engine-context-node"})
class EngineContextIT extends AbstractEmbeddedPostgresIT {

    private static final String OWN_DATABASE = "engine_context_it";

    @DynamicPropertySource
    static void ownDatabase(DynamicPropertyRegistry registry) {
        EmbeddedPostgresSupport.createDatabase(OWN_DATABASE);
        registry.add(OWN_DATABASE_URL_PROPERTY, () -> EmbeddedPostgresSupport.jdbcUrl(OWN_DATABASE));
    }

    @Autowired
    private SchedulerNode node;

    @Autowired
    private TestRestTemplate http;

    @Autowired
    private org.springframework.jdbc.core.simple.JdbcClient jdbc;

    @Test
    void runsOnItsOwnDatabase() {
        assertThat(jdbc.sql("SELECT current_database()").query(String.class).single()).isEqualTo(OWN_DATABASE);
    }

    @Test
    void nodeStartsAndExposesMetrics() {
        assertThat(node.state()).isEqualTo(SchedulerNode.State.RUNNING);
        await().atMost(Duration.ofSeconds(15)).until(() -> node.ownedPartitions().size() == 128);
        String prometheus = http.getForObject("/actuator/prometheus", String.class);
        assertThat(prometheus)
                .contains("scheduler_partitions_owned{")
                .contains("scheduler_runs{")
                .contains("scheduler_queue_depth")
                .contains("scheduler_lease_lost_total{")
                .contains("scheduler_run_outcome_total{")
                .contains("scheduler_stale_commit_rejected_total")
                .contains("scheduler_lease_renew_failures_total")
                .contains("scheduler_orphan_takeovers_total")
                .contains("scheduler_claim_batch_seconds");
    }
}
