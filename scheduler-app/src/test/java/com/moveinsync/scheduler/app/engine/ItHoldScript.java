package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.app.handler.EffectContext;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongPredicate;

/**
 * Test script: sends the run's single effect, then holds the run (still RUNNING, lease renewed)
 * until the test releases it, unless {@code holds} says this run should finish at once.
 */
final class ItHoldScript implements ItScriptedHandler.Script {

    private final Set<Long> released = ConcurrentHashMap.newKeySet();
    private final LongPredicate holds;
    private volatile boolean releaseAll;

    ItHoldScript(LongPredicate holds) {
        this.holds = holds;
    }

    void release(long runId) {
        released.add(runId);
    }

    void releaseAll() {
        releaseAll = true;
    }

    @Override
    public void run(EffectContext ctx, int execution) throws InterruptedException {
        long id = ctx.run().runId();
        ctx.send(ctx.run().idemKey(), "{\"it\":true}");
        if (!holds.test(id)) {
            Thread.sleep(10);
            return;
        }
        while (!releaseAll && !released.contains(id)) {
            Thread.sleep(5);
        }
    }
}
