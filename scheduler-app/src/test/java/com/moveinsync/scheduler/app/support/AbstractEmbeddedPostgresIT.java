package com.moveinsync.scheduler.app.support;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Base class for Spring integration tests of {@code scheduler-app}: boots the full application on
 * a random port against database {@value #DATABASE} on the shared embedded PostgreSQL 16 server
 * (no Docker). Flyway migrates it on first context start; Spring caches the context across test
 * classes with the same configuration.
 *
 * <p>The database is shared by all subclasses in a JVM, so tests must use unique tenant ids (or
 * clean up) instead of assuming empty tables. Tests that need a pristine database should use
 * {@link EmbeddedPostgresSupport#createDatabase(String)} directly, or register their own database
 * URL under {@value #OWN_DATABASE_URL_PROPERTY} from a {@code @DynamicPropertySource} method.
 * (Registering {@code spring.datasource.url} directly does not work: this class's method is
 * applied after the subclass's and would win.)
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public abstract class AbstractEmbeddedPostgresIT {

    /** Database used by Spring-context tests. */
    public static final String DATABASE = "scheduler_it";

    /** Property a subclass sets to use its own database; see {@code application-test.yml}. */
    public static final String OWN_DATABASE_URL_PROPERTY = "test.db.url";

    private static volatile boolean created;

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        ensureDatabase();
        registry.add("test.db.default-url", () -> EmbeddedPostgresSupport.jdbcUrl(DATABASE));
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
