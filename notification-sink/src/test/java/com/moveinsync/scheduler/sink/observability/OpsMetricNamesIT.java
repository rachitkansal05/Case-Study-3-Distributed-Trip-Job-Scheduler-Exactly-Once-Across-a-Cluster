package com.moveinsync.scheduler.sink.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.sink.support.AbstractEmbeddedPostgresIT;
import com.moveinsync.scheduler.sink.support.SinkTestClient;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.web.server.LocalServerPort;

/**
 * Every {@code [sink]} name in {@code ops/metric-names.txt} (the list the Grafana dashboard, alert
 * rules and runbook are checked against) is really exported on the sink's {@code /actuator/prometheus}.
 */
@AutoConfigureObservability(tracing = false)
class OpsMetricNamesIT extends AbstractEmbeddedPostgresIT {

    @LocalServerPort
    private int port;

    @Test
    void everySinkNameInTheOpsListIsExported() throws IOException {
        SinkTestClient sink = new SinkTestClient(port);
        sink.deliver(SinkTestClient.newRunId(), SinkTestClient.newKey("ops"), "1.1");
        String scrape = sink.getText("/actuator/prometheus").getBody();
        Set<String> exported = new LinkedHashSet<>();
        for (String line : scrape.split("\\R")) {
            if (!line.isBlank() && !line.startsWith("#")) {
                exported.add(line.split("[{ ]", 2)[0]);
            }
        }
        Set<String> listed = sinkSection();
        assertThat(listed).isNotEmpty();
        assertThat(exported).containsAll(listed);
    }

    private static Set<String> sinkSection() throws IOException {
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null && !Files.isRegularFile(dir.resolve("ops/metric-names.txt"))) {
            dir = dir.getParent();
        }
        if (dir == null) {
            throw new IllegalStateException("ops/metric-names.txt not found");
        }
        Set<String> names = new LinkedHashSet<>();
        boolean inSink = false;
        for (String raw : Files.readAllLines(dir.resolve("ops/metric-names.txt"), StandardCharsets.UTF_8)) {
            String line = raw.strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            if (line.startsWith("[")) {
                inSink = line.equals("[sink]");
            } else if (inSink) {
                names.add(line);
            }
        }
        return names;
    }
}
