package com.moveinsync.scheduler.app.failover;

import static org.awaitility.Awaitility.await;

import com.moveinsync.scheduler.app.repository.claim.FencingEpochReader;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import org.flywaydb.core.Flyway;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

/**
 * A Postgres 16 primary and one streaming standby ({@code application_name = standby1}) on a
 * private Docker network, for the failover tests (plan section 12, F1-F5).
 *
 * <p>Order matters: the primary starts <b>without</b> {@code synchronous_standby_names}, because
 * with it every commit (including the container's own readiness checks and Flyway) would block
 * until a standby attaches. {@link #start()} starts the standby, waits until it streams, and only
 * then {@link #enableSynchronousReplication() switches the primary to synchronous mode} and checks
 * {@code sync_state = 'sync'}.
 *
 * <p>The setting goes through {@code ALTER SYSTEM}, i.e. {@code postgresql.auto.conf}, which is not
 * replicated. The promoted standby therefore runs without a synchronous standby (it has none), so
 * its writes do not block after failover.
 */
final class PgReplicationPair implements AutoCloseable {

    static final String IMAGE = "postgres:16";
    static final String STANDBY_NAME = "standby1";
    private static final int PG_PORT = 5432;
    private static final String PRIMARY_ALIAS = "pg-primary";
    private static final String STANDBY_ALIAS = "pg-standby";

    /**
     * Runs inside the primary's first initdb (after the image has written its pg_hba.conf, before
     * the final server start): allows replication connections from the Docker network. The
     * image's {@code host all all all scram-sha-256} line does not match replication connections.
     */
    private static final String PRIMARY_INIT_SCRIPT = """
            #!/bin/bash
            set -e
            echo "host replication all all trust" >> "$PGDATA/pg_hba.conf"
            """;

    /**
     * Standby entrypoint, run as user postgres: base backup from the primary (creating the
     * physical slot), standby.signal via -R, an explicit primary_conninfo with the
     * application_name that synchronous_standby_names refers to, then the server in hot standby.
     */
    private static final String STANDBY_SCRIPT = """
            #!/bin/bash
            set -euo pipefail
            export PGDATA=/var/lib/postgresql/data
            until pg_isready -h pg-primary -p 5432 -q; do sleep 0.5; done
            pg_basebackup -h pg-primary -p 5432 -U "$REPL_USER" -D "$PGDATA" -X stream -C -S standby_slot -R -c fast
            chmod 0700 "$PGDATA"
            cat >> "$PGDATA/postgresql.auto.conf" <<EOF
            primary_conninfo = 'host=pg-primary port=5432 user=$REPL_USER application_name=standby1'
            primary_slot_name = 'standby_slot'
            EOF
            exec postgres -D "$PGDATA" -c hot_standby=on -c listen_addresses='*'
            """;

    private final Network network;
    private final PostgreSQLContainer<?> primary;
    private final GenericContainer<?> standby;
    private volatile boolean primaryKilled;

    @SuppressWarnings("resource")
    private PgReplicationPair() {
        network = Network.newNetwork();
        primary = new PostgreSQLContainer<>(IMAGE)
                .withNetwork(network)
                .withNetworkAliases(PRIMARY_ALIAS)
                .withCopyToContainer(Transferable.of(PRIMARY_INIT_SCRIPT, 0755),
                        "/docker-entrypoint-initdb.d/10-replication.sh")
                .withCommand("postgres",
                        "-c", "wal_level=replica",
                        "-c", "max_wal_senders=10",
                        "-c", "max_replication_slots=10",
                        "-c", "hot_standby=on",
                        "-c", "synchronous_commit=on");
        primary.withStartupTimeout(Duration.ofMinutes(2));
        standby = new GenericContainer<>(IMAGE)
                .withNetwork(network)
                .withNetworkAliases(STANDBY_ALIAS)
                .withExposedPorts(PG_PORT)
                .withEnv("REPL_USER", primary.getUsername())
                .withCopyToContainer(Transferable.of(STANDBY_SCRIPT, 0755), "/standby.sh")
                .withCreateContainerCmdModifier(cmd -> cmd.withUser("postgres").withEntrypoint("bash", "/standby.sh"))
                .waitingFor(Wait.forLogMessage(".*database system is ready to accept read.only connections.*\\s", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));
    }

    /** Starts primary and standby, and turns on synchronous replication (confirmed {@code sync}). */
    static PgReplicationPair start() {
        PgReplicationPair pair = new PgReplicationPair();
        try {
            pair.primary.start();
            pair.standby.start();
            pair.awaitStandbyStreaming();
            pair.enableSynchronousReplication();
            return pair;
        } catch (RuntimeException e) {
            pair.close();
            throw e;
        }
    }

    // ---- connections -----------------------------------------------------------------------

    String user() {
        return primary.getUsername();
    }

    String password() {
        return primary.getPassword();
    }

    String database() {
        return primary.getDatabaseName();
    }

    String primaryJdbcUrl() {
        return "jdbc:postgresql://" + primary.getHost() + ":" + primary.getMappedPort(PG_PORT) + "/" + database()
                + "?connectTimeout=5&socketTimeout=30";
    }

    String standbyJdbcUrl() {
        return "jdbc:postgresql://" + standby.getHost() + ":" + standby.getMappedPort(PG_PORT) + "/" + database()
                + "?connectTimeout=5&socketTimeout=30";
    }

    /**
     * Multi-host URL listing both servers; pgjdbc connects only to the one that accepts writes
     * ({@code targetServerType=primary}), like production's {@code target_session_attrs=read-write}.
     * Built while both containers run (their mapped ports are read here).
     */
    String failoverJdbcUrl() {
        return "jdbc:postgresql://" + primary.getHost() + ":" + primary.getMappedPort(PG_PORT) + ","
                + standby.getHost() + ":" + standby.getMappedPort(PG_PORT) + "/" + database()
                + "?targetServerType=primary&connectTimeout=3&socketTimeout=10&loginTimeout=10";
    }

    Connection primaryConnection() throws SQLException {
        return DriverManager.getConnection(primaryJdbcUrl(), user(), password());
    }

    Connection standbyConnection() throws SQLException {
        return DriverManager.getConnection(standbyJdbcUrl(), user(), password());
    }

    HikariDataSource pool(String name, String jdbcUrl, int size) {
        HikariConfig cfg = new HikariConfig();
        cfg.setPoolName(name);
        cfg.setJdbcUrl(jdbcUrl);
        cfg.setUsername(user());
        cfg.setPassword(password());
        cfg.setMaximumPoolSize(size);
        cfg.setConnectionTimeout(5_000);
        cfg.setValidationTimeout(2_000);
        return new HikariDataSource(cfg);
    }

    /** Runs the application's Flyway migrations on the primary (replicated to the standby). */
    void migratePrimary() {
        try (HikariDataSource ds = pool("flyway", primaryJdbcUrl(), 2)) {
            Flyway.configure().dataSource(ds).locations("classpath:db/migration").load().migrate();
        }
    }

    // ---- replication -----------------------------------------------------------------------

    private void awaitStandbyStreaming() {
        await("standby streaming").atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(250))
                .ignoreExceptions()
                .until(() -> STANDBY_NAME.equals(queryString(
                        "SELECT application_name FROM pg_stat_replication"
                                + " WHERE application_name = '" + STANDBY_NAME + "' AND state = 'streaming'")));
    }

    /** {@code FIRST 1 (standby1)} on the primary, then waits for {@code sync_state = 'sync'}. */
    void enableSynchronousReplication() {
        try (Connection c = primaryConnection(); Statement st = c.createStatement()) {
            st.execute("ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (" + STANDBY_NAME + ")'");
            st.execute("SELECT pg_reload_conf()");
        } catch (SQLException e) {
            throw new IllegalStateException("cannot enable synchronous replication", e);
        }
        await("standby is synchronous").atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200))
                .ignoreExceptions()
                .until(() -> "sync".equals(syncState()));
    }

    /** {@code sync_state} of standby1 in the primary's {@code pg_stat_replication}, or null. */
    String syncState() throws SQLException {
        return queryString("SELECT sync_state FROM pg_stat_replication WHERE application_name = '"
                + STANDBY_NAME + "'");
    }

    /** Number of rows in the primary's {@code pg_stat_replication}. */
    long connectedStandbys() throws SQLException {
        try (Connection c = primaryConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM pg_stat_replication")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private String queryString(String sql) throws SQLException {
        try (Connection c = primaryConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    /**
     * A write on the primary whose commit is acknowledged only after the synchronous standby has
     * flushed it. Waiters are released in WAL order, so once this returns, every commit that was
     * visible on the primary before this call is also on the standby (F1). Used to turn "visible on
     * the primary" (a commit can be visible while it still waits for the standby) into
     * "acknowledged".
     */
    void syncBarrier() throws SQLException {
        try (Connection c = primaryConnection(); Statement st = c.createStatement()) {
            st.executeUpdate("UPDATE cluster_meta SET cluster_generation = cluster_generation");
        }
    }

    // ---- failure injection -----------------------------------------------------------------

    /** kill -9 of the primary container: no shutdown checkpoint, connections reset. */
    void killPrimary() {
        primary.getDockerClient().killContainerCmd(primary.getContainerId()).withSignal("KILL").exec();
        primaryKilled = true;
    }

    /** Stops the standby container (its walreceiver disconnects from the primary). */
    void stopStandby() {
        standby.stop();
    }

    /** {@code pg_promote(wait => true)} on the standby, then waits until it accepts writes. */
    void promoteStandby() throws SQLException {
        try (Connection c = standbyConnection(); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT pg_promote(true, 60)")) {
            if (!rs.next() || !rs.getBoolean(1)) {
                throw new IllegalStateException("pg_promote returned false");
            }
        }
        await("standby promoted").atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                .ignoreExceptions()
                .until(() -> {
                    try (Connection c = standbyConnection(); Statement st = c.createStatement();
                         ResultSet rs = st.executeQuery("SELECT pg_is_in_recovery()")) {
                        return rs.next() && !rs.getBoolean(1);
                    }
                });
    }

    // ---- epoch and timeline ----------------------------------------------------------------

    /** Timeline ID of the server's current WAL file (first 8 hex digits); primaries only. */
    static long timeline(Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT pg_walfile_name(pg_current_wal_lsn())");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return Long.parseLong(rs.getString(1).substring(0, 8), 16);
        }
    }

    /** The fencing epoch a claim on this server would use right now. */
    static long epoch(Connection c) throws SQLException {
        return new FencingEpochReader().read(c);
    }

    @Override
    public void close() {
        try {
            standby.stop();
        } catch (RuntimeException ignored) {
            // best effort
        }
        try {
            primary.stop(); // also removes a killed container
        } catch (RuntimeException ignored) {
            // best effort
        }
        try {
            network.close();
        } catch (RuntimeException ignored) {
            // best effort
        }
    }

    boolean primaryKilled() {
        return primaryKilled;
    }
}
