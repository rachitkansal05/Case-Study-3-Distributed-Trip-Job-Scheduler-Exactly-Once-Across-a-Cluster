package com.moveinsync.scheduler.app.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * {@code ops/grafana/dashboards/scheduler.json} is valid, provisioned against the right
 * datasource, has the {@code node} and {@code tenant} variables and the PLAN.md section 14 panels
 * in order, and every PromQL expression uses only metric names from {@code ops/metric-names.txt}
 * (which {@code MetricsIT} proves are really exported).
 */
class GrafanaDashboardTest {

    private static JsonNode dashboard;
    private static List<JsonNode> panels;

    @BeforeAll
    static void load() throws Exception {
        dashboard = new ObjectMapper().readTree(OpsFiles.read("ops/grafana/dashboards/scheduler.json"));
        panels = new ArrayList<>();
        dashboard.get("panels").forEach(p -> {
            panels.add(p);
            if (p.has("panels")) {
                p.get("panels").forEach(panels::add); // collapsed rows
            }
        });
    }

    @Test
    void isAProvisionableGrafanaDashboard() {
        assertThat(dashboard.get("uid").asText()).isEqualTo("scheduler-overview");
        assertThat(dashboard.get("schemaVersion").asInt()).isGreaterThanOrEqualTo(36);
        assertThat(dashboard.get("title").asText()).isNotBlank();
        Set<Integer> ids = new HashSet<>();
        for (JsonNode p : panels) {
            assertThat(ids.add(p.get("id").asInt())).as("unique panel id %s", p.get("id")).isTrue();
            JsonNode grid = p.get("gridPos");
            assertThat(grid.get("x").asInt() + grid.get("w").asInt()).as("panel %s width", p.get("title")).isLessThanOrEqualTo(24);
        }
    }

    @Test
    void everyPanelUsesTheProvisionedPrometheusDatasource() {
        @SuppressWarnings("unchecked")
        Map<String, Object> provisioning = new Yaml().load(OpsFiles.read("ops/grafana/provisioning/datasources/datasource.yml"));
        @SuppressWarnings("unchecked")
        Map<String, Object> prometheus = ((List<Map<String, Object>>) provisioning.get("datasources")).get(0);
        String uid = (String) prometheus.get("uid");
        assertThat(prometheus.get("type")).isEqualTo("prometheus");

        for (JsonNode p : panels) {
            if ("row".equals(p.get("type").asText())) {
                continue;
            }
            assertThat(p.at("/datasource/uid").asText()).as("panel %s", p.get("title")).isEqualTo(uid);
            assertThat(p.get("targets")).as("panel %s has queries", p.get("title")).isNotEmpty();
            p.get("targets").forEach(t -> {
                if (t.has("datasource")) {
                    assertThat(t.at("/datasource/uid").asText()).isEqualTo(uid);
                }
            });
        }
        dashboard.at("/templating/list").forEach(v -> assertThat(v.at("/datasource/uid").asText()).isEqualTo(uid));
    }

    @Test
    void hasNodeAndTenantVariables() {
        List<String> names = new ArrayList<>();
        dashboard.at("/templating/list").forEach(v -> names.add(v.get("name").asText()));
        assertThat(names).contains("node", "tenant");
    }

    @Test
    void panelsFollowThePlanOrder() {
        List<String> titles = panels.stream().map(p -> p.get("title").asText().toLowerCase()).toList();
        List<String> expectedInOrder = List.of(
                "fire-lag p99", "queue depth", "running", "retrying", "dead-lettered", "error rate", "stale commits",
                "fire lag p50", "fire lag distribution",
                "runs by state", "outcomes rate", "error rate",
                "claim batch latency", "partitions owned per node",
                "lease renew failures", "lease lost by reason", "orphan takeovers", "orphan takeover time p99",
                "tenant cap: configured vs effective", "tenant cap rejections",
                "sink deliveries by outcome", "sink delivery latency",
                "heap used", "hikaricp connections");
        int from = 0;
        for (String expected : expectedInOrder) {
            int found = -1;
            for (int i = from; i < titles.size(); i++) {
                if (titles.get(i).startsWith(expected)) {
                    found = i;
                    break;
                }
            }
            assertThat(found).as("panel starting with '%s' after position %d in %s", expected, from, titles)
                    .isGreaterThanOrEqualTo(0);
            from = found + 1;
        }
    }

    @Test
    void everyExpressionReferencesOnlyKnownMetrics() {
        Set<String> known = OpsFiles.allMetricNames();
        List<String> expressions = new ArrayList<>();
        for (JsonNode p : panels) {
            if (p.has("targets")) {
                p.get("targets").forEach(t -> expressions.add(t.get("expr").asText()));
            }
        }
        dashboard.at("/templating/list").forEach(v -> {
            JsonNode q = v.get("query");
            expressions.add(q.isTextual() ? q.asText() : q.get("query").asText());
        });
        assertThat(expressions).hasSizeGreaterThan(30);
        Set<String> used = new HashSet<>();
        for (String expr : expressions) {
            Set<String> names = OpsFiles.metricNamesIn(expr);
            assertThat(names).as("metric names in %s", expr).isNotEmpty().isSubsetOf(known);
            used.addAll(names);
        }
        // every plan metric appears on the dashboard
        assertThat(used).contains(
                "scheduler_fire_lag_seconds_bucket", "scheduler_runs", "scheduler_queue_depth",
                "scheduler_run_outcome_total", "scheduler_stale_commit_rejected_total",
                "scheduler_tenant_cap_rejections_total", "scheduler_partitions_owned",
                "scheduler_lease_renew_failures_total", "scheduler_claim_batch_seconds_bucket", "scheduler_tenant_cap",
                "scheduler_lease_lost_total", "scheduler_orphan_takeovers_total",
                "scheduler_orphan_takeover_time_seconds_bucket", "sink_deliveries_total",
                "sink_delivery_latency_seconds_bucket");
    }

    @Test
    void scannerIgnoresFunctionsLabelsRangesAndVariables() {
        assertThat(OpsFiles.metricNamesIn(
                "histogram_quantile(0.99, sum by (le, handler) (rate(scheduler_fire_lag_seconds_bucket{node=~\"$node\"}"
                        + "[$__rate_interval]))) > 2 and on() max(scheduler_queue_depth offset 5m) / clamp_min(x_total, 1e-9)"))
                .containsExactly("scheduler_fire_lag_seconds_bucket", "scheduler_queue_depth", "x_total");
        assertThat(OpsFiles.metricNamesIn("label_values(scheduler_tenant_cap, tenant)"))
                .containsExactly("scheduler_tenant_cap");
    }
}
