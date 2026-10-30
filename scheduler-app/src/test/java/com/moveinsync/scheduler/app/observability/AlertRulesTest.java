package com.moveinsync.scheduler.app.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * {@code ops/prometheus/alerts.yml} has the structure Prometheus expects (promtool is not
 * available in the build), implements every "Alert when" of docs/design/architecture.md section 14, uses only known
 * metric names, and links every rule to an existing section of {@code docs/RUNBOOK.md}.
 */
class AlertRulesTest {

    private static List<Map<String, Object>> rules;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void load() {
        Map<String, Object> file = new Yaml().load(OpsFiles.read("ops/prometheus/alerts.yml"));
        assertThat(file).containsOnlyKeys("groups");
        rules = new ArrayList<>();
        for (Map<String, Object> group : (List<Map<String, Object>>) file.get("groups")) {
            assertThat(group).containsKeys("name", "rules");
            assertThat((String) group.get("name")).isNotBlank();
            rules.addAll((List<Map<String, Object>>) group.get("rules"));
        }
    }

    @Test
    void everyRuleIsWellFormed() {
        assertThat(rules).isNotEmpty();
        for (Map<String, Object> rule : rules) {
            String name = (String) rule.get("alert");
            assertThat(name).as("rule %s", rule).matches("[A-Z][A-Za-z0-9]+");
            assertThat(rule.keySet()).as(name).isSubsetOf("alert", "expr", "for", "labels", "annotations",
                    "keep_firing_for");
            assertThat((String) rule.get("expr")).as(name).isNotBlank();
            if (rule.containsKey("for")) {
                assertThat(String.valueOf(rule.get("for"))).as(name).matches("\\d+(ms|s|m|h|d)");
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> labels = (Map<String, Object>) rule.get("labels");
            assertThat(labels).as(name).containsKeys("severity", "summary");
            assertThat(labels.get("severity")).as(name).isIn("critical", "warning", "info");
            @SuppressWarnings("unchecked")
            Map<String, Object> annotations = (Map<String, Object>) rule.get("annotations");
            assertThat(annotations).as(name).containsKey("runbook_url");
            assertThat(balanced((String) rule.get("expr"))).as("balanced brackets in %s", name).isTrue();
        }
        assertThat(rules.stream().map(r -> r.get("alert")).distinct().count()).isEqualTo(rules.size());
    }

    @Test
    void everyExpressionReferencesOnlyKnownMetrics() {
        Set<String> known = OpsFiles.allMetricNames();
        for (Map<String, Object> rule : rules) {
            String expr = (String) rule.get("expr");
            assertThat(OpsFiles.metricNamesIn(expr)).as("metrics of %s", rule.get("alert")).isNotEmpty()
                    .isSubsetOf(known);
        }
    }

    @Test
    void implementsEveryPlanAlert() {
        assertThat(rule("SchedulerFireLagHigh")).contains("scheduler_fire_lag_seconds_bucket", "0.99", "> 2");
        assertThat(forOf("SchedulerFireLagHigh")).isEqualTo("5m");
        assertThat(rule("SchedulerDeadLettersIncreasing")).contains("scheduler_runs{state=\"dead_lettered\"}", "> 0");
        assertThat(rule("SchedulerQueueDepthGrowing")).contains("deriv(scheduler_queue_depth[2m])");
        assertThat(forOf("SchedulerQueueDepthGrowing")).isEqualTo("2m");
        assertThat(rule("SchedulerErrorRateHigh")).contains("scheduler_run_outcome_total", "> 0.05");
        assertThat(rule("SchedulerStaleCommitsRejected")).contains("scheduler_stale_commit_rejected_total", "> 0");
        assertThat(labelOf("SchedulerStaleCommitsRejected", "severity")).isEqualTo("warning");
        assertThat(rule("SchedulerPartitionsUnowned")).contains("sum(scheduler_partitions_owned) != 128");
        assertThat(forOf("SchedulerPartitionsUnowned")).isEqualTo("10s");
        assertThat(rule("SchedulerLeaseRenewFailures")).contains("scheduler_lease_renew_failures_total", "> 0");
        assertThat(rule("SchedulerClaimBatchSlow")).contains("scheduler_claim_batch_seconds_bucket", "> 0.1");
        assertThat(rule("SchedulerTenantCapDraining")).contains("kind=\"effective\"", ">", "kind=\"configured\"");
        assertThat(rule("SchedulerLeaseLostSpike")).contains("scheduler_lease_lost_total");
        assertThat(rule("SchedulerOrphanTakeoverSlow")).contains("scheduler_orphan_takeover_time_seconds_bucket",
                "> 11.2");
        assertThat(rule("SchedulerTargetDown")).contains("up{", "== 0");
    }

    @Test
    void runbookUrlsPointAtExistingRunbookSections() {
        Set<String> anchors = OpsFiles.anchors(OpsFiles.read("docs/RUNBOOK.md"));
        for (Map<String, Object> rule : rules) {
            @SuppressWarnings("unchecked")
            String url = (String) ((Map<String, Object>) rule.get("annotations")).get("runbook_url");
            assertThat(url).as("runbook_url of %s", rule.get("alert")).startsWith("docs/RUNBOOK.md#");
            String anchor = url.substring(url.indexOf('#') + 1);
            assertThat(anchor).isEqualTo(((String) rule.get("alert")).toLowerCase());
            assertThat(anchors).as("RUNBOOK.md heading for %s", rule.get("alert")).contains(anchor);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void prometheusLoadsTheRulesAndScrapesNodesAndSink() {
        Map<String, Object> config = new Yaml().load(OpsFiles.read("ops/prometheus/prometheus.yml"));
        assertThat((List<String>) config.get("rule_files")).containsExactly("/etc/prometheus/alerts.yml");
        List<String> jobs = ((List<Map<String, Object>>) config.get("scrape_configs")).stream()
                .map(j -> (String) j.get("job_name")).toList();
        assertThat(jobs).containsExactlyInAnyOrder("scheduler", "notification-sink");
    }

    private static String rule(String name) {
        return (String) find(name).get("expr");
    }

    private static String forOf(String name) {
        return String.valueOf(find(name).get("for"));
    }

    @SuppressWarnings("unchecked")
    private static String labelOf(String name, String label) {
        return (String) ((Map<String, Object>) find(name).get("labels")).get(label);
    }

    private static Map<String, Object> find(String name) {
        return rules.stream().filter(r -> name.equals(r.get("alert"))).findFirst()
                .orElseThrow(() -> new AssertionError("no alert rule " + name));
    }

    private static boolean balanced(String expr) {
        int round = 0;
        int curly = 0;
        int square = 0;
        boolean quoted = false;
        for (char c : expr.toCharArray()) {
            if (c == '"') {
                quoted = !quoted;
            }
            if (quoted) {
                continue;
            }
            switch (c) {
                case '(' -> round++;
                case ')' -> round--;
                case '{' -> curly++;
                case '}' -> curly--;
                case '[' -> square++;
                case ']' -> square--;
                default -> { }
            }
            if (round < 0 || curly < 0 || square < 0) {
                return false;
            }
        }
        return round == 0 && curly == 0 && square == 0 && !quoted;
    }
}
