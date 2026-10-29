package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.app.engine.support.TestCluster;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.core.ports.PartitionLeaseStore;
import com.moveinsync.scheduler.app.repository.cluster.JdbcPartitionLeaseStore;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** End to end on 3 in-process nodes: partitions spread, one-off jobs materialise, run and commit. */
class EngineSmokeIT {

    private static EngineTestDb db;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("engine_smoke_it");
    }

    @AfterAll
    static void close() {
        db.close();
    }

    @Test
    void threeNodesRunJobsToCompletion() {
        FakeEffectSink sink = new FakeEffectSink();
        Fixtures fx = new Fixtures(db.jdbc());
        fx.tenant("smoke", 8);
        for (int i = 0; i < 200; i++) {
            fx.job("smoke").pending().insert();
        }
        try (TestCluster cluster = new TestCluster(db, sink, HandlerRegistry.defaults(new ObjectMapper()))) {
            List<SchedulerNode> nodes = List.of(
                    cluster.start(TestCluster.fast("n1").build()),
                    cluster.start(TestCluster.fast("n2").build()),
                    cluster.start(TestCluster.fast("n3").build()));

            await().atMost(Duration.ofSeconds(20)).until(() ->
                    fx.count("SELECT count(*) FROM job_run WHERE tenant_id = 'smoke' AND state = 'SUCCEEDED'") == 200);
            assertThat(sink.distinctDeliveries()).isEqualTo(200);
            assertThat(sink.maxDeliveriesPerKey()).isEqualTo(1);

            await().atMost(Duration.ofSeconds(10)).until(() -> {
                Set<Integer> all = new HashSet<>();
                int sum = 0;
                for (SchedulerNode n : nodes) {
                    all.addAll(n.ownedPartitions());
                    sum += n.ownedPartitions().size();
                }
                return all.size() == 128 && sum == 128 && nodes.stream().allMatch(n -> !n.ownedPartitions().isEmpty());
            });
            List<PartitionLeaseStore.PartitionLease> rows = new JdbcPartitionLeaseStore(db.admin()).findAll();
            assertThat(rows).allMatch(r -> r.ownerNode() != null);
            assertThat(nodes.get(0).metrics().outcomeCount("success") + nodes.get(1).metrics().outcomeCount("success")
                    + nodes.get(2).metrics().outcomeCount("success")).isEqualTo(200);
        }
        // graceful stop released everything
        assertThat(fx.count("SELECT count(*) FROM partition_lease WHERE owner_node IS NOT NULL")).isZero();
        assertThat(fx.count("SELECT count(*) FROM scheduler_node")).isZero();
    }
}
