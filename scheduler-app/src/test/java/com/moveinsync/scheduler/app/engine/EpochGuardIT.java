package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.app.engine.support.TestCluster;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.app.repository.claim.FencingEpochReader;
import com.moveinsync.scheduler.app.sink.HttpEffectSink;
import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.ports.EffectSink;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Plan F5: a node refuses to start when its fencing epoch ({@code cluster_generation * 2^32 +
 * timeline}) is below the sink's highest accepted epoch, and fails closed when the sink cannot be
 * read; raising {@code cluster_meta.cluster_generation} lets it start.
 */
class EpochGuardIT {

    private static final Duration GUARD_TIMEOUT = Duration.ofMillis(300);

    private static EngineTestDb db;
    private static Fixtures fx;
    private static HandlerRegistry handlers;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("epoch_guard_it");
        fx = new Fixtures(db.jdbc());
        handlers = HandlerRegistry.defaults(new ObjectMapper());
    }

    @AfterAll
    static void close() {
        if (db != null) {
            db.close();
        }
    }

    @AfterEach
    void resetGeneration() {
        db.jdbc().sql("UPDATE cluster_meta SET cluster_generation = 0").update();
    }

    @Test
    void sinkEpochDecidesWhetherTheNodeStarts() throws Exception {
        FakeEffectSink sink = new FakeEffectSink();
        long current = currentEpoch();
        assertThat(current).as("generation 0, timeline >= 1").isBetween(1L, (1L << 32) - 1);

        try (TestCluster cluster = new TestCluster(db, sink, handlers)) {
            // (a) the sink has accepted nothing yet: highest epoch 0
            assertThat(sink.highestAcceptedEpoch()).isZero();
            SchedulerNode fresh = cluster.start(settings("eg-a"));
            assertThat(fresh.state()).isEqualTo(SchedulerNode.State.RUNNING);
            assertThat(nodeRows("eg-a")).isEqualTo(1);
            fresh.stop(Duration.ofSeconds(5));

            // (b) the sink has seen a newer epoch (database rebuilt): refuse, write nothing
            sink.setHighestEpochOverride(current + 1);
            SchedulerNode node = cluster.create(settings("eg-b"));
            assertThatThrownBy(node::start).isInstanceOf(EpochGuard.EpochGuardException.class)
                    .hasMessageContaining("lower than the sink's highest accepted epoch");
            assertThat(node.state()).isNotEqualTo(SchedulerNode.State.RUNNING);
            assertThat(nodeRows("eg-b")).isZero();
            assertThat(node.ownedPartitions()).isEmpty();

            // (c) the operator raises the generation: epoch 2^32 + timeline beats the sink's
            db.jdbc().sql("UPDATE cluster_meta SET cluster_generation = 1").update();
            assertThat(currentEpoch()).isEqualTo((1L << 32) + current).isGreaterThan(current + 1);
            node.start(); // the same node, unchanged config
            assertThat(node.state()).isEqualTo(SchedulerNode.State.RUNNING);
            assertThat(nodeRows("eg-b")).isEqualTo(1);
            node.stop(Duration.ofSeconds(5));
        }
    }

    @Test
    void unreadableSinkEpochRefusesAfterTheTimeout() {
        AtomicInteger calls = new AtomicInteger();
        EffectSink broken = new EffectSink() {
            @Override
            public DeliveryOutcome send(long runId, String idemKey, FencingToken token, String payloadJson) {
                throw new UnsupportedOperationException("not used");
            }

            @Override
            public long highestAcceptedEpoch() {
                calls.incrementAndGet();
                throw new IllegalStateException("sink unreachable");
            }
        };
        try (TestCluster cluster = new TestCluster(db, broken, handlers)) {
            SchedulerNode node = cluster.create(settings("eg-d"));
            long t0 = System.nanoTime();
            assertThatThrownBy(node::start).isInstanceOf(EpochGuard.EpochGuardException.class)
                    .hasMessageContaining("unavailable");
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
            assertThat(elapsedMs).isGreaterThanOrEqualTo(GUARD_TIMEOUT.toMillis()).isLessThan(5_000);
            assertThat(calls.get()).isGreaterThanOrEqualTo(2); // retried, never assumed 0
            assertThat(node.state()).isNotEqualTo(SchedulerNode.State.RUNNING);
            assertThat(nodeRows("eg-d")).isZero();
        }
    }

    @Test
    void httpSinkStubRefusesOn500AndStartsOnAValidEpoch() throws Exception {
        AtomicReference<Integer> status = new AtomicReference<>(500);
        AtomicReference<String> body = new AtomicReference<>("{\"code\":\"INTERNAL\"}");
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/fence/highest-epoch", exchange -> {
            byte[] bytes = body.get().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status.get(), bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        try {
            URI base = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            HttpEffectSink sink = new HttpEffectSink(base, Duration.ofSeconds(1), Duration.ofSeconds(1),
                    new ObjectMapper());
            try (TestCluster cluster = new TestCluster(db, sink, handlers)) {
                SchedulerNode node = cluster.create(settings("eg-e"));
                assertThatThrownBy(node::start).isInstanceOf(EpochGuard.EpochGuardException.class);
                assertThat(node.state()).isNotEqualTo(SchedulerNode.State.RUNNING);
                assertThat(nodeRows("eg-e")).isZero();

                status.set(200);
                body.set("{\"highestEpoch\": 1}");
                node.start();
                assertThat(node.state()).isEqualTo(SchedulerNode.State.RUNNING);
                assertThat(nodeRows("eg-e")).isEqualTo(1);
                node.stop(Duration.ofSeconds(5));
            }
        } finally {
            server.stop(0);
        }
    }

    private static EngineSettings settings(String nodeId) {
        return TestCluster.fast(nodeId).epochGuardEnabled(true).epochGuardTimeout(GUARD_TIMEOUT).build();
    }

    private static long nodeRows(String nodeId) {
        return fx.count("SELECT count(*) FROM scheduler_node WHERE node_id = ?", nodeId);
    }

    private static long currentEpoch() throws Exception {
        try (Connection c = db.admin().getConnection()) {
            return new FencingEpochReader().read(c);
        }
    }
}
