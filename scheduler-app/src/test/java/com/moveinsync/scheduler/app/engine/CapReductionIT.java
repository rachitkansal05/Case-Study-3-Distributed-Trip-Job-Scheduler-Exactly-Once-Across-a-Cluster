package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.app.engine.support.Fixtures;
import com.moveinsync.scheduler.app.engine.support.TestCluster;
import com.moveinsync.scheduler.app.handler.HandlerRegistry;
import com.moveinsync.scheduler.core.domain.HandlerType;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * R7, plan section 10 "Changing a cap": 28 runs running, cap lowered 32 → 16 with 30 more runs
 * waiting. Sampled every 10 ms with no tolerance: slot rows never increase, running ≤ slot rows,
 * and no new run starts until fewer than 16 are running. One long run in a high slot does not
 * block starts: it is compacted into a freed low slot while it keeps running with its token.
 */
class CapReductionIT {

    private static EngineTestDb db;
    private static Fixtures fx;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("cap_reduction_it");
        fx = new Fixtures(db.jdbc());
    }

    @AfterAll
    static void close() {
        db.close();
    }

    private record Sample(long running, long slots, long newStarted, long longRunSlot) {
    }

    @Test
    void reductionDrainsWithoutNewStartsAndCompactsTheLongRun() throws Exception {
        fx.tenant("red", 32);
        UUID job = fx.job("red").handler(HandlerType.ROSTER_GEN).maxRunTime(Duration.ofMinutes(5)).insert();
        Set<Long> old = new HashSet<>(fx.dueRuns(job, 28));
        ItHoldScript script = new ItHoldScript(old::contains);
        ItScriptedHandler handler = new ItScriptedHandler(HandlerType.ROSTER_GEN, script);
        try (TestCluster cluster = new TestCluster(db, new FakeEffectSink(), new HandlerRegistry(List.of(handler)))) {
            cluster.start(TestCluster.fast("red-node").workers(64).build());
            await().atMost(Duration.ofSeconds(15)).until(() -> running() == 28);
            long longRun = db.jdbc().sql("SELECT id FROM job_run WHERE tenant_id = 'red' AND state = 'RUNNING' "
                    + "ORDER BY slot_no DESC LIMIT 1").query(Long.class).single();
            long longSeq = fx.count("SELECT fencing_seq FROM job_run WHERE id = ?", longRun);
            assertThat(fx.intColumn(longRun, "slot_no")).isEqualTo(27);

            // the admin lowers the cap; more work is waiting
            db.jdbc().sql("UPDATE tenant SET configured_cap = 16, version = version + 1 WHERE id = 'red'").update();
            Set<Long> fresh = new HashSet<>(fx.dueRuns(job, 30));

            List<Sample> samples = new CopyOnWriteArrayList<>();
            List<String> violations = new CopyOnWriteArrayList<>();
            AtomicBoolean sampling = new AtomicBoolean(true);
            Thread sampler = Thread.ofPlatform().start(() -> {
                long prevSlots = Long.MAX_VALUE;
                while (sampling.get()) {
                    Sample s = sample(longRun);
                    samples.add(s);
                    if (s.slots() > prevSlots) {
                        violations.add("slot rows increased " + prevSlots + " -> " + s.slots());
                    }
                    if (s.running() > s.slots()) {
                        violations.add("running " + s.running() + " > slot rows " + s.slots());
                    }
                    if (s.newStarted() > 0 && s.running() > 16) {
                        violations.add("new run started while " + s.running() + " running");
                    }
                    prevSlots = s.slots();
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });

            // finish the old runs one by one, except the long one in the highest slot
            for (long id : old) {
                if (id != longRun) {
                    script.release(id);
                    Thread.sleep(30);
                }
            }
            // all 30 waiting runs start and finish while the long run is still running
            await().atMost(Duration.ofSeconds(20)).until(() -> fx.count(
                    "SELECT count(*) FROM job_run WHERE tenant_id = 'red' AND state = 'SUCCEEDED' AND id = ANY (?::bigint[])",
                    "{" + String.join(",", fresh.stream().map(String::valueOf).toList()) + "}") == 30);
            assertThat(fx.state(longRun)).isEqualTo("RUNNING");
            assertThat(fx.intColumn(longRun, "slot_no")).as("long run compacted into a low slot").isLessThan(16);
            assertThat(fx.count("SELECT fencing_seq FROM job_run WHERE id = ?", longRun))
                    .as("moving a run between slots keeps its token").isEqualTo(longSeq);

            script.releaseAll();
            await().atMost(Duration.ofSeconds(10)).until(() -> running() == 0);
            sampling.set(false);
            sampler.join();

            assertThat(violations).isEmpty();
            assertThat(samples.size()).isGreaterThan(50);
            assertThat(samples.get(0).slots()).isLessThanOrEqualTo(32);
            assertThat(db.jdbc().sql("SELECT slot_no FROM tenant_slot WHERE tenant_id = 'red' ORDER BY slot_no")
                    .query(Integer.class).list()).containsExactlyElementsOf(java.util.stream.IntStream.range(0, 16).boxed().toList());
            assertThat(fx.count("SELECT count(*) FROM job_run WHERE tenant_id = 'red' AND state = 'SUCCEEDED'")).isEqualTo(58);
            // the effective cap only went down and reached 16
            assertThat(samples.get(samples.size() - 1).slots()).isEqualTo(16);
        }
    }

    private static long running() {
        return fx.count("SELECT count(*) FROM job_run WHERE tenant_id = 'red' AND state = 'RUNNING'");
    }

    private static Sample sample(long longRun) {
        return db.jdbc().sql("""
                SELECT (SELECT count(*) FROM job_run WHERE tenant_id = 'red' AND state = 'RUNNING'),
                       (SELECT count(*) FROM tenant_slot WHERE tenant_id = 'red'),
                       (SELECT count(*) FROM job_run WHERE tenant_id = 'red' AND started_at IS NOT NULL
                           AND id > (SELECT max(id) - 30 FROM job_run WHERE tenant_id = 'red')),
                       (SELECT coalesce(slot_no, -1) FROM job_run WHERE id = ?)
                """).param(longRun)
                .query((rs, i) -> new Sample(rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getLong(4))).single();
    }
}
