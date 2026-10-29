package com.moveinsync.scheduler.app.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * {@code docker-compose.yml} parses and wires observability the way the demo needs it. Docker is
 * not available in the build, so this checks the file, not a running stack.
 */
class ComposeConfigTest {

    private static Map<String, Map<String, Object>> services;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void load() {
        Map<String, Object> compose = new Yaml().load(OpsFiles.read("docker-compose.yml"));
        services = (Map<String, Map<String, Object>>) compose.get("services");
    }

    @Test
    void everyServiceHasAHealthcheck() {
        assertThat(services).containsKeys("postgres", "toxiproxy", "notification-sink", "scheduler-1", "scheduler-2",
                "scheduler-3", "prometheus", "grafana");
        services.forEach((name, service) -> assertThat(service).as(name).containsKey("healthcheck"));
    }

    @Test
    void nodesRunJsonAndDevProfilesAndReachTheSinkThroughToxiproxy() {
        for (String node : List.of("scheduler-1", "scheduler-2", "scheduler-3")) {
            Map<String, Object> env = env(node);
            assertThat(env.get("SPRING_PROFILES_ACTIVE")).as(node).isEqualTo("json,dev");
            assertThat((String) env.get("SCHEDULER_SINK_BASE_URL")).as(node).startsWith("http://toxiproxy:");
            assertThat(env.get("NODE_ID")).isEqualTo(node);
            assertThat(dependsOn(node)).containsEntry("postgres", Map.of("condition", "service_healthy"))
                    .containsEntry("notification-sink", Map.of("condition", "service_healthy"));
        }
    }

    @Test
    void sinkLogsJsonAndHasADevAdminToken() {
        Map<String, Object> env = env("notification-sink");
        assertThat(env.get("SPRING_PROFILES_ACTIVE")).isEqualTo("json");
        assertThat((String) env.get("SINK_ADMIN_TOKEN")).startsWith("${SINK_ADMIN_TOKEN:-dev-");
        assertThat(dependsOn("notification-sink")).containsEntry("postgres", Map.of("condition", "service_healthy"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void prometheusMountsConfigAndAlertRules() {
        List<String> volumes = (List<String>) services.get("prometheus").get("volumes");
        assertThat(volumes).contains("./ops/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro",
                "./ops/prometheus/alerts.yml:/etc/prometheus/alerts.yml:ro");
    }

    @Test
    @SuppressWarnings("unchecked")
    void grafanaProvisionsTheDashboard() {
        List<String> volumes = (List<String>) services.get("grafana").get("volumes");
        assertThat(volumes).contains("./ops/grafana/provisioning:/etc/grafana/provisioning:ro",
                "./ops/grafana/dashboards:/var/lib/grafana/dashboards:ro");
        assertThat(env("grafana")).containsEntry("GF_AUTH_ANONYMOUS_ORG_ROLE", "Viewer");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> env(String service) {
        return (Map<String, Object>) services.get(service).get("environment");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> dependsOn(String service) {
        return (Map<String, Object>) services.get(service).get("depends_on");
    }
}
