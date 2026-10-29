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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * R7: raising a cap takes effect at once. Cap 2 with 10 waiting runs → 2 running; cap raised to
 * 6 (slot rows inserted by the compactor, as the API would) → 6 running on the next polls, never
 * more than the slot rows at any 10 ms sample.
 */
class CapIncreaseIT {

    private static EngineTestDb db;
    private static Fixtures fx;

    @BeforeAll
    static void init() {
        db = EngineTestDb.create("cap_increase_it");
        fx = new Fixtures(db.jdbc());
    }

    @AfterAll
    static void close() {
        db.close();
    }

    @Test
    void raisedCapIsUsableOnTheNextPoll() throws Exception {
        fx.tenant("inc", 2);
        UUID job = fx.job("inc").handler(HandlerType.ROSTER_GEN).insert();
        fx.dueRuns(job, 10);
        ItHoldScript script = new ItHoldScript(id -> true);
        ItScriptedHandler handler = new ItScriptedHandler(HandlerType.ROSTER_GEN, script);
        try (TestCluster cluster = new TestCluster(db, new FakeEffectSink(), new HandlerRegistry(List.of(handler)))) {
            cluster.start(TestCluster.fast("inc-1").build());
            cluster.start(TestCluster.fast("inc-2").build());

            List<String> violations = new CopyOnWriteArrayList<>();
            AtomicBoolean sampling = new AtomicBoolean(true);
            Thread sampler = Thread.ofPlatform().start(() -> {
                while (sampling.get()) {
                    long[] s = db.jdbc().sql("SELECT (SELECT count(*) FROM job_run WHERE tenant_id = 'inc' "
                                    + "AND state = 'RUNNING'), (SELECT count(*) FROM tenant_slot WHERE tenant_id = 'inc')")
                            .query((rs, i) -> new long[] {rs.getLong(1), rs.getLong(2)}).single();
                    if (s[0] > s[1]) {
                        violations.add("running " + s[0] + " > slots " + s[1]);
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });

            await().atMost(Duration.ofSeconds(15)).until(() -> running() == 2);
            Thread.sleep(500); // still capped: nothing else starts
            assertThat(running()).isEqualTo(2);

            long raisedAt = System.nanoTime();
            db.jdbc().sql("UPDATE tenant SET configured_cap = 6, version = version + 1 WHERE id = 'inc'").update();
            await().atMost(Duration.ofSeconds(5)).until(() -> running() == 6);
            long tookMs = (System.nanoTime() - raisedAt) / 1_000_000;
            assertThat(fx.count("SELECT count(*) FROM tenant_slot WHERE tenant_id = 'inc'")).isEqualTo(6);
            Thread.sleep(300);
            assertThat(running()).isEqualTo(6);

            script.releaseAll();
            await().atMost(Duration.ofSeconds(15)).until(() ->
                    fx.count("SELECT count(*) FROM job_run WHERE tenant_id = 'inc' AND state = 'SUCCEEDED'") == 10);
            sampling.set(false);
            sampler.join();
            assertThat(violations).isEmpty();
            System.out.println("CapIncreaseIT: 2 -> 6 running " + tookMs + " ms after the cap was raised");
        }
    }

    private static long running() {
        return fx.count("SELECT count(*) FROM job_run WHERE tenant_id = 'inc' AND state = 'RUNNING'");
    }
}
