package com.moveinsync.scheduler.sink.support;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * One real PostgreSQL 16 server per test JVM, started from the zonky embedded binaries.
 *
 * <p>No Docker is needed. The server starts lazily on first use and stops on JVM exit. Tests that
 * need isolation call {@link #createDatabase(String)} to get their own fresh database on the same
 * server instead of starting another server.
 */
public final class EmbeddedPostgresSupport {

    /** Superuser that the embedded server is initialised with (trust auth, no password). */
    public static final String USER = "postgres";

    private static final Pattern SAFE_NAME = Pattern.compile("[a-z][a-z0-9_]{0,62}");
    private static volatile EmbeddedPostgres server;

    private EmbeddedPostgresSupport() {
    }

    /** Returns the shared server, starting it on first call. */
    public static EmbeddedPostgres server() {
        EmbeddedPostgres local = server;
        if (local == null) {
            synchronized (EmbeddedPostgresSupport.class) {
                local = server;
                if (local == null) {
                    local = start();
                    server = local;
                }
            }
        }
        return local;
    }

    /** JDBC URL of database {@code db} on the shared server. */
    public static String jdbcUrl(String db) {
        return server().getJdbcUrl(USER, db);
    }

    /** DataSource for database {@code db} on the shared server. */
    public static DataSource dataSource(String db) {
        return server().getDatabase(USER, db);
    }

    /**
     * Drops (if present) and creates an empty database, returning a DataSource for it.
     *
     * @param name lower-case identifier, e.g. {@code "schema_it"}
     */
    public static DataSource createDatabase(String name) {
        if (!SAFE_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("unsafe database name: " + name);
        }
        try (Connection c = server().getPostgresDatabase().getConnection();
             Statement s = c.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)");
            s.execute("CREATE DATABASE " + name);
        } catch (SQLException e) {
            throw new IllegalStateException("could not create database " + name, e);
        }
        return dataSource(name);
    }

    private static EmbeddedPostgres start() {
        try {
            EmbeddedPostgres pg = EmbeddedPostgres.builder()
                    .setServerConfig("max_connections", "300")
                    .setServerConfig("fsync", "off")
                    .setServerConfig("timezone", "UTC")
                    .start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    pg.close();
                } catch (IOException ignored) {
                    // best effort on JVM exit
                }
            }, "embedded-postgres-stop"));
            return pg;
        } catch (IOException e) {
            throw new UncheckedIOException("could not start embedded PostgreSQL", e);
        }
    }
}
