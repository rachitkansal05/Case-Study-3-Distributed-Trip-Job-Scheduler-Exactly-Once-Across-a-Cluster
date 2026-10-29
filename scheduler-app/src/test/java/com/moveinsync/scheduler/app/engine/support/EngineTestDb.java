package com.moveinsync.scheduler.app.engine.support;

import com.moveinsync.scheduler.app.support.EmbeddedPostgresSupport;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * A fresh, migrated database on the shared embedded PostgreSQL server for one engine IT class,
 * plus connection pools for the simulated nodes. Every IT class uses its own database name, so
 * classes never see each other's rows.
 */
public final class EngineTestDb implements AutoCloseable {

    private final String name;
    private final HikariDataSource admin;
    private final List<HikariDataSource> pools = new CopyOnWriteArrayList<>();

    private EngineTestDb(String name) {
        this.name = name;
        DataSource raw = EmbeddedPostgresSupport.createDatabase(name);
        Flyway.configure().dataSource(raw).locations("classpath:db/migration").load().migrate();
        this.admin = newPool("admin-" + name, 8);
    }

    /** Drops and recreates database {@code name}, migrated to the latest schema. */
    public static EngineTestDb create(String name) {
        return new EngineTestDb(name);
    }

    /** Pool for test setup and assertions (never used by a node). */
    public DataSource admin() {
        return admin;
    }

    /** JdbcClient on the admin pool. */
    public JdbcClient jdbc() {
        return JdbcClient.create(admin);
    }

    /**
     * A new pool, e.g. one per simulated node; closing it drops every connection, which is how a
     * killed node's in-flight transactions get rolled back.
     */
    public HikariDataSource newPool(String poolName, int size) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(EmbeddedPostgresSupport.jdbcUrl(name));
        cfg.setUsername(EmbeddedPostgresSupport.USER);
        cfg.setPassword("");
        cfg.setPoolName(poolName);
        cfg.setMaximumPoolSize(size);
        cfg.setMinimumIdle(1);
        cfg.setConnectionTimeout(10_000);
        HikariDataSource ds = new HikariDataSource(cfg);
        pools.add(ds);
        return ds;
    }

    @Override
    public void close() {
        for (HikariDataSource ds : pools) {
            ds.close();
        }
        admin.close();
    }
}
