package com.moveinsync.scheduler.loadgen.support;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * One embedded PostgreSQL 16 server (zonky binaries, no Docker) for the end-to-end tests and the
 * in-process benchmark. The scheduler and the notification sink each get their own database on it,
 * like the compose setup where both use one Postgres server.
 *
 * <p>Tests use {@link #forTests()} (fsync off, like the other modules' ITs). The benchmark uses
 * {@link #durable(Map)} with the default durability settings (fsync on, synchronous_commit on), so
 * its numbers include real WAL flushes.
 */
public final class EmbeddedPg implements AutoCloseable {

    public static final String USER = "postgres";
    private static final Pattern SAFE_NAME = Pattern.compile("[a-z][a-z0-9_]{0,62}");
    private static volatile EmbeddedPg shared;

    private final EmbeddedPostgres pg;
    private final Map<String, String> settings;

    private EmbeddedPg(Map<String, String> settings) {
        this.settings = Map.copyOf(settings);
        try {
            EmbeddedPostgres.Builder b = EmbeddedPostgres.builder();
            settings.forEach(b::setServerConfig);
            this.pg = b.start();
        } catch (IOException e) {
            throw new UncheckedIOException("could not start embedded PostgreSQL", e);
        }
    }

    /** Shared server for the end-to-end ITs of this JVM (fsync off), stopped on JVM exit. */
    public static EmbeddedPg forTests() {
        EmbeddedPg local = shared;
        if (local == null) {
            synchronized (EmbeddedPg.class) {
                local = shared;
                if (local == null) {
                    Map<String, String> s = new LinkedHashMap<>();
                    s.put("max_connections", "400");
                    s.put("fsync", "off");
                    s.put("timezone", "UTC");
                    local = new EmbeddedPg(s);
                    EmbeddedPg started = local;
                    Runtime.getRuntime().addShutdownHook(new Thread(started::close, "embedded-pg-stop"));
                    shared = local;
                }
            }
        }
        return local;
    }

    /**
     * A dedicated server with the given settings on top of max_connections=400, timezone UTC and
     * fsync, synchronous_commit and full_page_writes explicitly on (the PostgreSQL defaults).
     */
    public static EmbeddedPg durable(Map<String, String> extra) {
        Map<String, String> s = new LinkedHashMap<>();
        s.put("max_connections", "400");
        s.put("timezone", "UTC");
        // zonky's builder turns these off by default; a benchmark must pay for real WAL flushes
        s.put("fsync", "on");
        s.put("synchronous_commit", "on");
        s.put("full_page_writes", "on");
        s.putAll(extra);
        return new EmbeddedPg(s);
    }

    public int port() {
        return pg.getPort();
    }

    /** Server settings this server was started with (beyond the PostgreSQL defaults). */
    public Map<String, String> settings() {
        return settings;
    }

    /** JDBC URL of database {@code db}. */
    public String jdbcUrl(String db) {
        return "jdbc:postgresql://localhost:" + pg.getPort() + "/" + db + "?user=" + USER;
    }

    /** Drops (if present) and creates an empty database. */
    public void createDatabase(String name) {
        if (!SAFE_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("unsafe database name: " + name);
        }
        try (Connection c = pg.getPostgresDatabase().getConnection(); Statement s = c.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)");
            s.execute("CREATE DATABASE " + name);
        } catch (SQLException e) {
            throw new IllegalStateException("could not create database " + name, e);
        }
    }

    /** {@code SHOW name} on the server. */
    public String show(String name) {
        try (Connection c = pg.getPostgresDatabase().getConnection(); Statement s = c.createStatement();
             var rs = s.executeQuery("SHOW " + name)) {
            rs.next();
            return rs.getString(1);
        } catch (SQLException e) {
            throw new IllegalStateException("SHOW " + name, e);
        }
    }

    @Override
    public void close() {
        try {
            pg.close();
        } catch (IOException ignored) {
            // best effort
        }
    }
}
