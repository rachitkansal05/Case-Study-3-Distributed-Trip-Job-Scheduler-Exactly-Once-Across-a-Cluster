package com.moveinsync.scheduler.app.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.app.handler.BillingCloseHandler;
import com.moveinsync.scheduler.app.handler.EffectContext;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.app.handler.JobHandler;
import com.moveinsync.scheduler.app.handler.PickupAlertHandler;
import com.moveinsync.scheduler.app.handler.RosterGenerationHandler;
import com.moveinsync.scheduler.app.service.CacheConfig;
import com.moveinsync.scheduler.app.support.EmbeddedPostgresSupport;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.ports.EffectSink;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

/**
 * A real node (engine enabled, fake sink) runs a small workload, then {@code /actuator/prometheus}
 * must export every metric of the PLAN.md section 14 table with its tags, every {@code [scheduler]}
 * name of {@code ops/metric-names.txt} (the list the dashboard, alerts and runbook are checked
 * against), and the Caffeine caches; the {@code scheduler_runs{state}} gauges must equal
 * {@code SELECT state, count(*) FROM job_run}.
 *
 * <p>Workload: tenant {@code metrics-a} (cap 1) gets four slow runs, so later claims are
 * cap-rejected; tenant {@code metrics-b} gets quick successful runs and one run with an invalid
 * payload, which is dead-lettered at once.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@AutoConfigureObservability
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Import(MetricsIT.Config.class)
@TestPropertySource(properties = {
    "scheduler.engine.enabled=true",
    "scheduler.engine.epoch-guard.enabled=false",
    "scheduler.engine.metrics.refresh-interval=300ms",
    "scheduler.node-id=metrics-node"})
class MetricsIT {

    private static final String OWN_DATABASE = "observability_metrics_it";
    private static final String CAPPED = "metrics-a";
    private static final String BUSY = "metrics-b";

    @DynamicPropertySource
    static void ownDatabase(DynamicPropertyRegistry registry) {
        EmbeddedPostgresSupport.createDatabase(OWN_DATABASE);
        registry.add("spring.datasource.url", () -> EmbeddedPostgresSupport.jdbcUrl(OWN_DATABASE));
        registry.add("spring.datasource.username", () -> EmbeddedPostgresSupport.USER);
        registry.add("spring.datasource.password", () -> "");
    }

    @Autowired
    private TestRestTemplate http;

    @Autowired
    private JdbcClient jdbc;

    @BeforeAll
    void runWorkload() {
        Fixtures fx = new Fixtures(jdbc);
        fx.tenant(CAPPED, 1);
        fx.tenant(BUSY, 4);
        UUID slow = fx.job(CAPPED).handler(HandlerType.NO_SHOW_CANCEL).payload("{}").insert();
        fx.dueRuns(slow, 4);
        UUID quick = fx.job(BUSY).insert();
        fx.dueRuns(quick, 10);
        UUID poison = fx.job(BUSY).payload("{}").insert();
        fx.dueRuns(poison, 1);

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200)).until(() ->
                fx.count("SELECT count(*) FROM job_run WHERE state IN ('SCHEDULED', 'RUNNING', 'RETRY_WAIT')") == 0);
        assertThat(fx.count("SELECT count(*) FROM job_run")).as("own database: only this workload").isEqualTo(15);
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE state = 'SUCCEEDED'")).isEqualTo(14);
        assertThat(fx.count("SELECT count(*) FROM job_run WHERE state = 'DEAD_LETTERED'")).isEqualTo(1);
    }

    @Test
    void exportsEveryPlanMetricWithItsTags() {
        PrometheusScrape scrape = await().atMost(Duration.ofSeconds(15)).until(this::scrape,
                s -> s.value("scheduler_partitions_owned", Map.of("node", "metrics-node")) == 128
                        && !s.series("scheduler_tenant_cap").isEmpty());

        assertThat(scrape.labelValues("scheduler_fire_lag_seconds_bucket", "tenant_tier")).containsExactly("standard");
        assertThat(scrape.labelValues("scheduler_fire_lag_seconds_bucket", "handler"))
                .contains("ROSTER_GEN", "NO_SHOW_CANCEL");
        assertThat(scrape.labelValues("scheduler_fire_lag_seconds_bucket", "le"))
                .contains("0.1", "0.25", "0.5", "1.0", "2.0", "5.0", "10.0", "30.0", "+Inf");
        assertThat(scrape.value("scheduler_fire_lag_seconds_count", Map.of("handler", "ROSTER_GEN"))).isEqualTo(11);

        assertThat(scrape.labelValues("scheduler_runs", "state"))
                .containsExactlyInAnyOrder("running", "retrying", "dead_lettered");
        assertThat(scrape.series("scheduler_queue_depth")).hasSize(1);
        assertThat(scrape.labelValues("scheduler_run_outcome_total", "outcome"))
                .containsExactlyInAnyOrder("success", "retryable", "permanent", "timeout");
        assertThat(scrape.value("scheduler_run_outcome_total", Map.of("outcome", "success"))).isEqualTo(14);
        assertThat(scrape.value("scheduler_run_outcome_total", Map.of("outcome", "permanent"))).isEqualTo(1);
        assertThat(scrape.series("scheduler_stale_commit_rejected_total")).hasSize(1);
        assertThat(scrape.labelValues("scheduler_tenant_cap_rejections_total", "tenant")).contains(CAPPED);
        assertThat(scrape.labelValues("scheduler_partitions_owned", "node")).containsExactly("metrics-node");
        assertThat(scrape.series("scheduler_lease_renew_failures_total")).hasSize(1);
        assertThat(scrape.series("scheduler_claim_batch_seconds_bucket")).isNotEmpty();
        assertThat(scrape.value("scheduler_claim_batch_seconds_count", Map.of())).isPositive();
        assertThat(scrape.value("scheduler_tenant_cap", Map.of("tenant", CAPPED, "kind", "configured"))).isEqualTo(1);
        assertThat(scrape.value("scheduler_tenant_cap", Map.of("tenant", CAPPED, "kind", "effective"))).isEqualTo(1);
        assertThat(scrape.value("scheduler_tenant_cap", Map.of("tenant", BUSY, "kind", "configured"))).isEqualTo(4);
        assertThat(scrape.labelValues("scheduler_lease_lost_total", "reason"))
                .containsExactlyInAnyOrder("stale_409", "fenced_write_0_rows", "self_fence");
        assertThat(scrape.series("scheduler_orphan_takeovers_total")).hasSize(1);
        assertThat(scrape.labelValues("scheduler_orphan_takeover_time_seconds_bucket", "le")).contains("11.0");

        // the common tags every dashboard filter relies on
        for (String name : List.of("scheduler_runs", "scheduler_lease_renew_failures_total",
                "scheduler_claim_batch_seconds_count", "scheduler_fire_lag_seconds_count")) {
            assertThat(scrape.labelValues(name, "node")).as("node tag on %s", name).containsExactly("metrics-node");
            assertThat(scrape.labelValues(name, "application")).as(name).containsExactly("scheduler");
        }
    }

    @Test
    void exportsEveryNameTheOpsFilesMayUse() {
        System.gc(); // jvm_gc_pause_* appears after the first collection
        Set<String> expected = OpsFiles.metricNames().get(OpsFiles.SCHEDULER);
        assertThat(expected).hasSizeGreaterThan(30);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(scrape().names()).containsAll(expected));
    }

    @Test
    void bindsBothCaffeineCaches() {
        PrometheusScrape scrape = scrape();
        for (String metric : List.of("cache_gets_total", "cache_puts_total", "cache_evictions_total", "cache_size")) {
            assertThat(scrape.labelValues(metric, "cache")).as(metric)
                    .contains(CacheConfig.TENANT_SETTINGS, CacheConfig.NEXT_FIRE_TIMES);
        }
        assertThat(scrape.labelValues("cache_gets_total", "result")).contains("hit", "miss");
    }

    @Test
    void runGaugesMatchTheDatabase() {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Map<String, Long> db = new HashMap<>();
            jdbc.sql("SELECT state, count(*) FROM job_run GROUP BY state")
                    .query((rs, i) -> db.put(rs.getString(1), rs.getLong(2))).list();
            PrometheusScrape scrape = scrape();
            assertThat(scrape.value("scheduler_runs", Map.of("state", "running")))
                    .isEqualTo(db.getOrDefault("RUNNING", 0L).doubleValue());
            assertThat(scrape.value("scheduler_runs", Map.of("state", "retrying")))
                    .isEqualTo(db.getOrDefault("RETRY_WAIT", 0L).doubleValue());
            assertThat(scrape.value("scheduler_runs", Map.of("state", "dead_lettered")))
                    .isEqualTo(db.getOrDefault("DEAD_LETTERED", 0L).doubleValue())
                    .isEqualTo(1);
            long dueNotStarted = jdbc.sql("""
                    SELECT count(*) FROM job_run
                     WHERE state IN ('SCHEDULED', 'RETRY_WAIT') AND due_at <= clock_timestamp()""")
                    .query(Long.class).single();
            assertThat(scrape.value("scheduler_queue_depth", Map.of())).isEqualTo((double) dueNotStarted);
        });
    }

    private PrometheusScrape scrape() {
        return PrometheusScrape.parse(http.getForObject("/actuator/prometheus", String.class));
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Config {

        @Bean
        @Primary
        EffectSink fakeEffectSink() {
            return new FakeEffectSink();
        }

        @Bean
        @Primary
        HandlerRegistry metricsHandlers(ObjectMapper mapper) {
            return new HandlerRegistry(List.of(new PickupAlertHandler(mapper), new RosterGenerationHandler(mapper),
                    new BillingCloseHandler(mapper), new SlowHandler(mapper)));
        }
    }

    /** NO_SHOW_CANCEL stand-in that holds its slot for a while, so the capped tenant's claims are rejected. */
    static final class SlowHandler extends JobHandler<String> {

        SlowHandler(ObjectMapper mapper) {
            super(mapper);
        }

        @Override
        public HandlerType type() {
            return HandlerType.NO_SHOW_CANCEL;
        }

        @Override
        protected String parsePayload(String payloadJson) {
            return payloadJson;
        }

        @Override
        protected void execute(EffectContext ctx, String payload) throws InterruptedException {
            Thread.sleep(700);
            ctx.send(ctx.run().itemKey("only"), "{\"text\":\"slow\"}");
        }
    }
}
