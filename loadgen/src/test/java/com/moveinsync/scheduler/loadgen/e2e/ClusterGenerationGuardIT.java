package com.moveinsync.scheduler.loadgen.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.moveinsync.scheduler.app.engine.EngineSettings;
import com.moveinsync.scheduler.app.engine.EpochGuard;
import com.moveinsync.scheduler.app.engine.SchedulerNode;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.loadgen.support.EmbeddedPg;
import com.moveinsync.scheduler.loadgen.support.SchedulerCluster;
import com.moveinsync.scheduler.loadgen.support.SinkProcess;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Plan F5 with the <b>real</b> sink (the engine's {@code EpochGuardIT} covers the same rule against
 * a stub HTTP server; this one proves the real {@code /api/v1/fence/highest-epoch} and the real
 * client agree). Scenario: the scheduler database was rebuilt from a fresh initdb (generation 0,
 * timeline 1, so epoch 1) while the sink has already accepted tokens of generation 1. A node must
 * refuse to start; after the operator raises {@code cluster_meta.cluster_generation} above the
 * sink's, it starts and its new tokens beat everything the sink has seen.
 */
class ClusterGenerationGuardIT {

    /** generation 1, timeline 5: what the old cluster used before the rebuild. */
    private static final long SINK_EPOCH = FencingToken.epochOf(1, 5);

    private static SinkProcess sink;

    @BeforeAll
    static void startSink() {
        sink = SinkProcess.start(EmbeddedPg.forTests(), "e2e_guard_sink", 5);
    }

    @AfterAll
    static void stopSink() {
        if (sink != null) {
            sink.close();
        }
    }

    @Test
    void nodeRefusesToStartBelowTheSinksHighestEpochUntilTheGenerationIsRaised() {
        // seed the real sink's fence through its public API, as the pre-rebuild cluster did
        assertThat(sink.deliver(999_999L, "pre-rebuild:item-1", SINK_EPOCH + ".77")).isEqualTo(201);
        assertThat(sink.highestEpoch()).isEqualTo(SINK_EPOCH);

        try (SchedulerCluster cluster = new SchedulerCluster(EmbeddedPg.forTests(), "e2e_guard_sched",
                sink.baseUrl())) {
            long dbEpoch = currentEpoch(cluster);
            assertThat(dbEpoch).as("fresh initdb: generation 0").isLessThan(SINK_EPOCH);

            SchedulerNode node = cluster.create(EngineSettings.builder("guard-1").build());
            assertThatThrownBy(node::start).isInstanceOf(EpochGuard.EpochGuardException.class)
                    .hasMessageContaining("lower than the sink's highest accepted epoch");
            assertThat(node.state()).isEqualTo(SchedulerNode.State.NEW);
            assertThat(cluster.jdbc().sql("SELECT count(*) FROM scheduler_node").query(Long.class).single())
                    .as("a refused node never heartbeats").isZero();

            // generation 1 is still not enough (1 x 2^32 + 1 < 1 x 2^32 + 5)
            cluster.jdbc().sql("UPDATE cluster_meta SET cluster_generation = 1").update();
            assertThatThrownBy(() -> cluster.create(EngineSettings.builder("guard-2").build()).start())
                    .isInstanceOf(EpochGuard.EpochGuardException.class);

            // operator raises the generation past the sink's: the node starts and runs work
            cluster.jdbc().sql("UPDATE cluster_meta SET cluster_generation = 2").update();
            SchedulerNode ok = cluster.start(EngineSettings.builder("guard-3").build());
            assertThat(ok.state()).isEqualTo(SchedulerNode.State.RUNNING);

            cluster.tenant("guard", 2);
            UUID job = cluster.pickupAlertJob("guard", 3);
            long runId = cluster.dueRun(job);
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(100))
                    .until(() -> "SUCCEEDED".equals(cluster.run(runId).state()));
            FencingToken token = cluster.run(runId).token();
            System.out.printf("ClusterGenerationGuardIT: sink highest epoch %d; DB epoch at generation 0 = %d "
                            + "(refused), generation 1 = %d (refused), generation 2 = %d (started); run token %s%n",
                    SINK_EPOCH, dbEpoch, FencingToken.epochOf(1, 1), currentEpoch(cluster), token);
            assertThat(token.epoch()).isGreaterThan(SINK_EPOCH);
            assertThat(sink.runStats(runId).deliveredDistinct()).isEqualTo(3);
            assertThat(sink.highestEpoch()).isEqualTo(token.epoch());
        }
    }

    private static long currentEpoch(SchedulerCluster cluster) {
        return cluster.jdbc().sql("""
                SELECT cluster_generation::bigint * 4294967296
                       + ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::bigint
                  FROM cluster_meta""").query(Long.class).single();
    }
}
