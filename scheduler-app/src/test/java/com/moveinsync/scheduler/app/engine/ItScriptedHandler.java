package com.moveinsync.scheduler.app.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.handler.EffectContext;
import com.moveinsync.scheduler.app.handler.JobHandler;
import com.moveinsync.scheduler.core.domain.HandlerType;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only handler whose behaviour is a lambda. It accepts any payload (the raw JSON is passed
 * through) and counts executions per run id, so a script can act on "the n-th execution of this
 * run" (lease-lost executions included, unlike {@code RunContext.attempt()}).
 */
final class ItScriptedHandler extends JobHandler<String> {

    /** What one execution does. {@code execution} is 1-based and counts every handler call of the run. */
    @FunctionalInterface
    interface Script {
        void run(EffectContext ctx, int execution) throws InterruptedException;
    }

    private final HandlerType type;
    private final Script script;
    private final Map<Long, AtomicInteger> executions = new ConcurrentHashMap<>();
    private final AtomicInteger total = new AtomicInteger();

    ItScriptedHandler(HandlerType type, Script script) {
        super(new ObjectMapper());
        this.type = Objects.requireNonNull(type, "type");
        this.script = Objects.requireNonNull(script, "script");
    }

    @Override
    public HandlerType type() {
        return type;
    }

    @Override
    protected String parsePayload(String payloadJson) {
        return payloadJson;
    }

    @Override
    protected void execute(EffectContext ctx, String payload) throws InterruptedException {
        int n = executions.computeIfAbsent(ctx.run().runId(), id -> new AtomicInteger()).incrementAndGet();
        total.incrementAndGet();
        script.run(ctx, n);
    }

    /** Handler calls for one run so far. */
    int executionsOf(long runId) {
        AtomicInteger n = executions.get(runId);
        return n == null ? 0 : n.get();
    }

    /** Handler calls over all runs. */
    int totalExecutions() {
        return total.get();
    }
}
