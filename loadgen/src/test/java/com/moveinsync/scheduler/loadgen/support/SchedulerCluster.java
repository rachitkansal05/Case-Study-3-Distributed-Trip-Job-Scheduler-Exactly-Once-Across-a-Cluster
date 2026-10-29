package com.moveinsync.scheduler.loadgen.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.EngineSettings;
import com.moveinsync.scheduler.app.engine.SchedulerNode;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.app.sink.HttpEffectSink;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.flywaydb.core.Flyway;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Several real {@link SchedulerNode}s in this JVM against one migrated scheduler database, each
 * with its own connection pool and meter registry (like separate processes), all calling the real
 * sink over HTTP through {@link HttpEffectSink}. {@link #kill} is {@code kill -9}: threads stop and
 * the node's pool is closed, which drops its connections.
 */
public final class SchedulerCluster implements AutoCloseable {

    private final EmbeddedPg pg;
    private final String db;
    private final URI sinkUrl;
    private final HandlerRegistry handlers;
    private final HikariDataSource admin;
    private final List<SchedulerNode> nodes = new ArrayList<>();
    private final Map<String, HikariDataSource> pools = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();

    /** Creates database {@code db} fresh, migrated with the scheduler's Flyway migrations. */
    public SchedulerCluster(EmbeddedPg pg, String db, URI sinkUrl, HandlerRegistry handlers) {
        this.pg = pg;
        this.db = db;
        this.sinkUrl = sinkUrl;
        this.handlers = handlers;
        pg.createDatabase(db);
        this.admin = pool("admin-" + db, 10);
        Flyway.configure().dataSource(admin).locations("classpath:db/migration").load().migrate();
    }

    public SchedulerCluster(EmbeddedPg pg, String db, URI sinkUrl) {
        this(pg, db, sinkUrl, HandlerRegistry.defaults(new ObjectMapper()));
    }

    public JdbcClient jdbc() {
        return JdbcClient.create(admin);
    }

    public HikariDataSource admin() {
        return admin;
    }

    public HikariDataSource pool(String name, int size) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(pg.jdbcUrl(db));
        cfg.setUsername(EmbeddedPg.USER);
        cfg.setPassword("");
        cfg.setPoolName(name);
        cfg.setMaximumPoolSize(size);
        cfg.setMinimumIdle(1);
        cfg.setConnectionTimeout(10_000);
        return new HikariDataSource(cfg);
    }

    /** A real HTTP client of the sink, as production wires it (connect 2 s, request 5 s). */
    public HttpEffectSink newSinkClient() {
        return new HttpEffectSink(sinkUrl, Duration.ofSeconds(2), Duration.ofSeconds(5), mapper);
    }

    /** Creates and starts a node. */
    public SchedulerNode start(EngineSettings settings) {
        SchedulerNode node = create(settings);
        node.start();
        return node;
    }

    /** Creates (does not start) a node with its own pool and sink client. */
    public SchedulerNode create(EngineSettings settings) {
        HikariDataSource pool = pool("pool-" + settings.nodeId(), Math.min(60, settings.workers() + 10));
        pools.put(settings.nodeId(), pool);
        SchedulerNode node = new SchedulerNode(settings, pool, newSinkClient(), handlers, new SimpleMeterRegistry());
        synchronized (nodes) {
            nodes.add(node);
        }
        return node;
    }

    public void kill(SchedulerNode node) {
        node.kill();
        HikariDataSource pool = pools.get(node.nodeId());
        if (pool != null) {
            pool.close();
        }
    }

    public List<SchedulerNode> nodes() {
        synchronized (nodes) {
            return List.copyOf(nodes);
        }
    }

    public SchedulerNode node(String id) {
        return nodes().stream().filter(n -> n.nodeId().equals(id)).findFirst()
                .orElseThrow(() -> new AssertionError("no node " + id));
    }

    // ------------------------------------------------------------------ fixtures (test setup only)

    /** Tenant in UTC with slot rows 0..cap-1. */
    public void tenant(String id, int cap) {
        jdbc().sql("INSERT INTO tenant (id, time_zone, configured_cap) VALUES (?, 'UTC', ?)").params(id, cap).update();
        jdbc().sql("INSERT INTO tenant_slot (tenant_id, slot_no) SELECT ?, g FROM generate_series(0, ? - 1) g")
                .params(id, cap).update();
    }

    /** One-off PICKUP_ALERT job for {@code employees} employees ({@code emp-0001}...); returns its id. */
    public java.util.UUID pickupAlertJob(String tenant, int employees) {
        java.util.UUID id = java.util.UUID.randomUUID();
        int partition = com.moveinsync.scheduler.core.cluster.PartitionHasher.partitionOf(id);
        jdbc().sql("""
                INSERT INTO job (id, tenant_id, kind, handler, run_at, payload, priority, max_run_time,
                                 misfire, misfire_threshold, max_attempts, partition_id, version)
                VALUES (?, ?, 'ONE_OFF', 'PICKUP_ALERT', clock_timestamp(), ?::jsonb, 5, interval '15 minutes',
                        'FIRE_NOW', interval '60 seconds', 5, ?, 0)""")
                .params(id, tenant, alertPayload(employees), partition).update();
        return id;
    }

    /** One SCHEDULED run of {@code job}, due now (DB clock); returns its id. */
    public long dueRun(java.util.UUID job) {
        return jdbc().sql("""
                INSERT INTO job_run (job_id, tenant_id, partition_id, priority, job_version, max_run_time,
                                     scheduled_at, due_at, idem_key, state)
                SELECT j.id, j.tenant_id, j.partition_id, j.priority, j.version, j.max_run_time, t.at, t.at,
                       j.id || ':' || to_char(t.at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'), 'SCHEDULED'
                  FROM job j, (SELECT date_trunc('milliseconds', clock_timestamp()) AS at) t
                 WHERE j.id = ?
                RETURNING id""").param(job).query(Long.class).single();
    }

    /** owner, token, state, lease_until and DB now of one run. */
    public RunRow run(long runId) {
        return jdbc().sql("""
                SELECT owner_node, fencing_epoch, fencing_seq, state, lease_until, clock_timestamp() AS db_now
                  FROM job_run WHERE id = ?""").param(runId).query((rs, i) -> new RunRow(
                rs.getString("owner_node"),
                rs.getObject("fencing_seq") == null ? null
                        : new FencingToken(rs.getLong("fencing_epoch"), rs.getLong("fencing_seq")),
                rs.getString("state"),
                rs.getObject("lease_until", OffsetDateTime.class) == null ? null
                        : rs.getObject("lease_until", OffsetDateTime.class).toInstant(),
                rs.getObject("db_now", OffsetDateTime.class).toInstant())).single();
    }

    public Instant dbNow() {
        return jdbc().sql("SELECT clock_timestamp()").query(OffsetDateTime.class).single().toInstant();
    }

    /** {@code {"employeeIds":["emp-0001",...],"messageTemplate":...}}. */
    public static String alertPayload(int n) {
        StringBuilder sb = new StringBuilder("{\"employeeIds\":[");
        for (int i = 1; i <= n; i++) {
            if (i > 1) {
                sb.append(',');
            }
            sb.append(String.format("\"emp-%04d\"", i));
        }
        return sb.append("],\"messageTemplate\":\"Your cab is assigned, {employeeId}\"}").toString();
    }

    /** A snapshot of one {@code job_run} row. */
    public record RunRow(String ownerNode, FencingToken token, String state, Instant leaseUntil, Instant dbNow) {
        public boolean running() {
            return "RUNNING".equals(state);
        }
    }

    @Override
    public void close() {
        for (SchedulerNode n : nodes()) {
            if (n.state() == SchedulerNode.State.RUNNING) {
                n.unfreeze();
                n.stop(Duration.ofSeconds(5));
            }
        }
        pools.values().forEach(HikariDataSource::close);
        admin.close();
    }
}
