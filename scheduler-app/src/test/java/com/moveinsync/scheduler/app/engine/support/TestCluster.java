package com.moveinsync.scheduler.app.engine.support;

import com.moveinsync.scheduler.app.engine.EngineSettings;
import com.moveinsync.scheduler.app.engine.SchedulerNode;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.core.ports.EffectSink;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Several {@link SchedulerNode}s in one JVM sharing one database: each node gets its own
 * connection pool and meter registry, like a separate process. {@link #kill} stops a node's threads
 * abruptly and closes its pool, which drops its connections (in-flight transactions roll back).
 */
public final class TestCluster implements AutoCloseable {

    private final EngineTestDb db;
    private final EffectSink sink;
    private final HandlerRegistry handlers;
    private final List<SchedulerNode> nodes = new ArrayList<>();
    private final Map<String, HikariDataSource> pools = new ConcurrentHashMap<>();

    public TestCluster(EngineTestDb db, EffectSink sink, HandlerRegistry handlers) {
        this.db = db;
        this.sink = sink;
        this.handlers = handlers;
    }

    /**
     * Short timings for most tests: run lease 3 s (renew 1 s, margin 1 s), partition lease 2 s,
     * heartbeat 0.5 s / dead after 2 s, poll 50 ms, sweep 200 ms, fast backoff.
     */
    public static EngineSettings.Builder fast(String nodeId) {
        return EngineSettings.builder(nodeId)
                .runLease(Duration.ofSeconds(3))
                .runRenewInterval(Duration.ofMillis(1000))
                .selfFenceMargin(Duration.ofMillis(1000))
                .partitionLease(Duration.ofSeconds(2))
                .partitionRenewInterval(Duration.ofMillis(700))
                .partitionLoopInterval(Duration.ofMillis(300))
                .heartbeatInterval(Duration.ofMillis(500))
                .deadAfter(Duration.ofSeconds(2))
                .pollInterval(Duration.ofMillis(50))
                .orphanSweepInterval(Duration.ofMillis(200))
                .backoffBase(Duration.ofMillis(50))
                .backoffCap(Duration.ofMillis(400))
                .materializerInterval(Duration.ofMillis(200))
                .compactorInterval(Duration.ofMillis(200))
                .metricsRefreshInterval(Duration.ofSeconds(1))
                .workers(16);
    }

    /** Starts a node with its own pool and meter registry. */
    public SchedulerNode start(EngineSettings settings) {
        SchedulerNode node = create(settings);
        node.start();
        return node;
    }

    /** Creates (does not start) a node. */
    public SchedulerNode create(EngineSettings settings) {
        HikariDataSource pool = db.newPool("pool-" + settings.nodeId(), Math.min(40, settings.workers() + 8));
        pools.put(settings.nodeId(), pool);
        SchedulerNode node = new SchedulerNode(settings, pool, sink, handlers, new SimpleMeterRegistry());
        synchronized (nodes) {
            nodes.add(node);
        }
        return node;
    }

    /** kill -9: threads stop, nothing released, connections dropped. */
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

    @Override
    public void close() {
        for (SchedulerNode n : nodes()) {
            if (n.state() == SchedulerNode.State.RUNNING) {
                n.unfreeze();
                n.stop(Duration.ofSeconds(5));
            }
        }
        pools.values().forEach(HikariDataSource::close);
    }
}
