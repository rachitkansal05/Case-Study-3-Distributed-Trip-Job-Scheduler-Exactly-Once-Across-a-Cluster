package com.moveinsync.scheduler.sink.support;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Base class for Spring integration tests of {@code notification-sink}: boots the sink on a random
 * port against database {@value #DATABASE} on the shared embedded PostgreSQL 16 server (no
 * Docker). Flyway creates and migrates the {@code sink} schema on first context start.
 *
 * <p>The database is shared by all subclasses in a JVM; use unique idempotency keys / run ids.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class AbstractEmbeddedPostgresIT {

    /** Database used by Spring-context tests. */
    public static final String DATABASE = "sink_it";

    private static volatile boolean created;

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        ensureDatabase();
        registry.add("spring.datasource.url", () -> EmbeddedPostgresSupport.jdbcUrl(DATABASE));
        registry.add("spring.datasource.username", () -> EmbeddedPostgresSupport.USER);
        registry.add("spring.datasource.password", () -> "");
    }

    private static synchronized void ensureDatabase() {
        if (!created) {
            EmbeddedPostgresSupport.createDatabase(DATABASE);
            created = true;
        }
    }
}
