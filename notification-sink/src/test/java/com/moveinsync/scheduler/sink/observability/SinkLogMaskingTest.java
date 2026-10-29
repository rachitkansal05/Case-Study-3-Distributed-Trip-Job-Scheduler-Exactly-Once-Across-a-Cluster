package com.moveinsync.scheduler.sink.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The sink's {@code logback-spring.xml} masks PII in both outputs: profile {@code json} (as in
 * docker-compose) writes one JSON object per line with the MDC and masks message, MDC values and
 * stack trace; the default plain-text pattern masks message and stack trace.
 */
class SinkLogMaskingTest {

    @AfterEach
    void restoreDefaultLogging() {
        SinkLogSupport.loadLogbackConfig();
    }

    @Test
    void jsonOutputMasksPii() throws Exception {
        SinkLogSupport.loadLogbackConfig("json");
        String out = SinkLogSupport.encodeWith("JSON");
        assertThat(out.strip()).doesNotContain("\n");
        JsonNode line = new ObjectMapper().readTree(out);
        assertThat(line.path("runId").asText()).isEqualTo("4242");
        assertThat(line.path("fencingToken").asText()).isEqualTo("6.2");
        assertThat(line.path("contact").asText()).isEqualTo("[email]");
        assertThat(line.path("message").asText())
                .contains("notify [email] at [phone] or [phone] pickup [geo]", "\"lat\": [geo]", "runId=4242",
                        "fencingToken=6.2");
        assertThat(line.path("stack_trace").asText()).contains("IllegalStateException", "[email]", "[phone]");
        assertThat(out).doesNotContain("fake.rider", "example.com", "99999", "11.111111", "22.222222",
                SinkLogSupport.LAT);
    }

    @Test
    void plainOutputMasksPii() {
        SinkLogSupport.loadLogbackConfig();
        String out = SinkLogSupport.encodeWith("CONSOLE");
        assertThat(out).contains("notify [email] at [phone] or [phone] pickup [geo]", "\"lat\": [geo]",
                "runId=4242", "fencingToken=6.2", "IllegalStateException");
        assertThat(out).doesNotContain("fake.rider", "example.com", "99999", "11.111111", "22.222222",
                SinkLogSupport.LAT);
    }
}
