package com.moveinsync.scheduler.app.observability;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.util.LogbackMDCAdapter;
import ch.qos.logback.core.OutputStreamAppender;
import ch.qos.logback.core.encoder.LayoutWrappingEncoder;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class PiiMaskingTest {

    /** input, PII substring that must disappear, expected replacement, surrounding text kept. */
    static Stream<Arguments> positives() {
        return Stream.of(
                Arguments.of("notify priya.sharma+trip@example.co.in about trip 42",
                        "priya.sharma+trip@example.co.in", "notify [email] about", "about trip 42"),
                Arguments.of("user ops@moveinsync.com logged in", "ops@moveinsync.com", "[email]", " logged in"),
                Arguments.of("pickup at 12.971599, 77.594566 confirmed", "77.594566", "at [geo] confirmed", "pickup"),
                Arguments.of("drop -33.8688,151.2093 done", "151.2093", "drop [geo] done", "drop"),
                Arguments.of("payload {\"lat\": 12.9716, \"x\":1}", "12.9716", "\"lat\": [geo]", ", \"x\":1}"),
                Arguments.of("update lng=77.5946 ok", "77.5946", "lng=[geo]", " ok"),
                Arguments.of("{\"latitude\":\"12.97\"}", "12.97", "\"latitude\":\"[geo]\"", "{"),
                Arguments.of("LONGITUDE: -122.4194 end", "122.4194", "LONGITUDE: [geo]", " end"),
                Arguments.of("sending alert to +91 98450 12345 for trip 42", "98450", "to [phone] for", " for trip 42"),
                Arguments.of("sms +91-9845012345 sent", "9845012345", "sms [phone] sent", "sms "),
                Arguments.of("call +1 (415) 555-0100 now", "555-0100", "call [phone] now", " now"),
                Arguments.of("uk +44 20 7946 0958.", "7946", "uk [phone].", "uk "),
                Arguments.of("driver 9845012345 assigned", "9845012345", "driver [phone] assigned", "driver"),
                Arguments.of("driver 98450 12345 assigned", "98450", "driver [phone] assigned", "driver "),
                Arguments.of("office (415) 555-0100 closed", "555-0100", "office [phone] closed", "office "),
                Arguments.of("office 415-555-0100 closed", "415-555", "office [phone] closed", " closed"),
                Arguments.of("office 415.555.0100 closed", "415.555", "office [phone] closed", " closed"),
                Arguments.of("desk 080-12345678 open", "12345678", "desk [phone] open", "desk "),
                Arguments.of("desk 022 23456789 open", "23456789", "desk [phone] open", " open"));
    }

    @ParameterizedTest
    @MethodSource("positives")
    void masksPii(String input, String pii, String expected, String kept) {
        String masked = PiiMasker.mask(input);
        assertThat(masked).doesNotContain(pii).contains(expected).contains(kept);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "job 3f2b1c9e-8a7d-4e6f-9b0a-1c2d3e4f5a6b claimed",
        "at 2026-10-30T10:15:30.123Z fired",
        "at 2026-10-30 10:15:30 fired",
        "fencingToken=8589934593.12 accepted",
        "token 4294967297.7 stale",
        "query took 250 ms",
        "lag=1234ms",
        "runId=123456 partition=7 of 64",
        "connecting to localhost:8090",
        "node 10.0.12.7 joined",
        "epoch 1761819330123 now",
        "scale 1.5, 2.0 applied",
        "nothing sensitive here at all",
        "attempt 3/5 backoff 2000ms",
    })
    void leavesNonPiiUnchanged(String input) {
        assertThat(PiiMasker.mask(input)).isEqualTo(input);
    }

    @Test
    void masksMixedJsonLine() {
        String line = "{\"email\":\"priya.sharma@example.com\",\"phone\":\"+91 98450 12345\","
                + "\"lat\":12.9716,\"lng\":77.5946,\"tripId\":\"T-42\"}";
        assertThat(PiiMasker.mask(line)).isEqualTo("{\"email\":\"[email]\",\"phone\":\"[phone]\","
                + "\"lat\":[geo],\"lng\":[geo],\"tripId\":\"T-42\"}");
    }

    @Test
    void handlesNullAndEmpty() {
        assertThat(PiiMasker.mask(null)).isNull();
        assertThat(PiiMasker.mask("")).isEmpty();
    }

    @Test
    void returnsSameInstanceWhenUnchanged() {
        String s = "runId=123456 took 250 ms";
        assertThat(PiiMasker.mask(s)).isSameAs(s);
    }

    @Test
    void longAdversarialInputCompletesQuickly() {
        String evil = "a".repeat(50_000) + "@" + "1".repeat(50_000) + " +" + "9 ".repeat(20_000) + "x.".repeat(20_000);
        long start = System.nanoTime();
        PiiMasker.mask(evil);
        assertThat(System.nanoTime() - start).isLessThan(2_000_000_000L);
    }

    @Test
    void messageConverterMasksFormattedArguments() {
        LoggerContext ctx = new LoggerContext();
        Logger logger = ctx.getLogger("test");
        LoggingEvent event = new LoggingEvent("fqcn", logger, Level.INFO,
                "alert {} at {}", null, new Object[] {"priya@example.com", "+91 98450 12345"});

        PiiMaskingConverter converter = new PiiMaskingConverter();
        converter.setContext(ctx);
        converter.start();

        assertThat(converter.convert(event)).isEqualTo("alert [email] at [phone]");
    }

    @Test
    void throwableConverterMasksExceptionMessage() {
        LoggerContext ctx = new LoggerContext();
        Logger logger = ctx.getLogger("test");
        LoggingEvent event = new LoggingEvent("fqcn", logger, Level.ERROR, "failed",
                new IllegalStateException("no user priya@example.com"), null);

        PiiMaskingThrowableConverter converter = new PiiMaskingThrowableConverter();
        converter.setContext(ctx);
        converter.start();

        assertThat(converter.convert(event))
                .contains("IllegalStateException")
                .contains("no user [email]")
                .doesNotContain("priya@example.com");
    }

    @Test
    void valueMaskerMasksStringsAndReturnsNullWhenUnchanged() {
        PiiValueMasker masker = new PiiValueMasker();
        assertThat(masker.mask(null, "mail priya@example.com")).isEqualTo("mail [email]");
        assertThat(masker.mask(null, "runId=123456")).isNull();
        assertThat(masker.mask(null, 42)).isNull();
    }

    @Test
    void patternLayoutEndToEndMasksOutput() {
        LoggerContext ctx = new LoggerContext();
        ctx.setMDCAdapter(new LogbackMDCAdapter());
        ctx.start();

        PatternLayout layout = new PatternLayout();
        layout.setContext(ctx);
        layout.getInstanceConverterMap().put("maskedMsg", PiiMaskingConverter::new);
        layout.getInstanceConverterMap().put("maskedEx", PiiMaskingThrowableConverter::new);
        layout.setPattern("%level %maskedMsg%n%maskedEx");
        layout.start();

        LayoutWrappingEncoder<ILoggingEvent> encoder = new LayoutWrappingEncoder<>();
        encoder.setContext(ctx);
        encoder.setLayout(layout);
        encoder.setCharset(StandardCharsets.UTF_8);
        encoder.start();

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        OutputStreamAppender<ILoggingEvent> appender = new OutputStreamAppender<>();
        appender.setContext(ctx);
        appender.setEncoder(encoder);
        appender.setOutputStream(bytes);
        appender.start();

        Logger logger = ctx.getLogger("e2e");
        logger.setAdditive(false);
        logger.addAppender(appender);
        logger.info("rider {} at {}, {} phone {}", "priya.sharma@example.co.in", "12.971599", "77.594566",
                "9845012345");
        logger.warn("lookup failed", new IllegalArgumentException("bad phone +91-9845012345"));
        appender.stop();

        String out = bytes.toString(StandardCharsets.UTF_8);
        assertThat(out)
                .startsWith("INFO rider [email] at [geo] phone [phone]")
                .contains("WARN lookup failed")
                .contains("bad phone [phone]")
                .doesNotContain("priya.sharma")
                .doesNotContain("9845012345")
                .doesNotContain("77.594566");
    }
}
