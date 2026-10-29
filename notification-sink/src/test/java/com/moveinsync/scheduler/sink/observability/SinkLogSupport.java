package com.moveinsync.scheduler.sink.observability;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.OutputStreamAppender;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.slf4j.LoggerFactory;
import org.springframework.boot.logging.LoggingInitializationContext;
import org.springframework.boot.logging.LoggingSystem;
import org.springframework.core.env.StandardEnvironment;

/**
 * Loads the sink's {@code logback-spring.xml} under given profiles and encodes one PII-laden
 * logging event with the encoder it installed. Values are fake.
 *
 * <p>Spring Boot initialises logback once per JVM while any context is alive (test contexts are
 * cached), so tests cannot rely on their own context to switch profiles; they reload the file
 * explicitly and restore the default afterwards.
 */
final class SinkLogSupport {

    static final String PHONE = "+91 99999 00000";
    static final String MOBILE = "9999900000";
    static final String EMAIL = "fake.rider@example.com";
    static final String PAIR = "11.111111, 22.222222";
    static final String LAT = "12.345678";

    private SinkLogSupport() {
    }

    /** Re-initialises logging from {@code classpath:logback-spring.xml} with {@code profiles} active. */
    static void loadLogbackConfig(String... profiles) {
        LoggingSystem system = LoggingSystem.get(SinkLogSupport.class.getClassLoader());
        system.cleanUp();
        StandardEnvironment env = new StandardEnvironment();
        env.setActiveProfiles(profiles);
        system.beforeInitialize();
        system.initialize(new LoggingInitializationContext(env), "classpath:logback-spring.xml", null);
    }

    @SuppressWarnings("unchecked")
    static String encodeWith(String appenderName) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        OutputStreamAppender<ILoggingEvent> appender =
                (OutputStreamAppender<ILoggingEvent>) root.getAppender(appenderName);
        if (appender == null) {
            throw new AssertionError("no appender " + appenderName + " on the root logger");
        }
        Logger logger = context.getLogger("com.moveinsync.scheduler.sink.test");
        String message = "notify " + EMAIL + " at " + PHONE + " or " + MOBILE + " pickup " + PAIR
                + " payload {\"lat\": " + LAT + "} runId=4242 fencingToken=6.2";
        LoggingEvent event = new LoggingEvent(Logger.class.getName(), logger, Level.WARN, message,
                new IllegalStateException("failed for " + EMAIL + " " + PHONE), null);
        event.setMDCPropertyMap(Map.of("runId", "4242", "fencingToken", "6.2", "contact", EMAIL));
        return new String(appender.getEncoder().encode(event), StandardCharsets.UTF_8);
    }
}
