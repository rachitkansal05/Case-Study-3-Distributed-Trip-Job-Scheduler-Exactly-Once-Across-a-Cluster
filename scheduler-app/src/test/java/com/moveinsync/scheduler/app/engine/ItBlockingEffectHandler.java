package com.moveinsync.scheduler.app.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.handler.EffectContext;
import com.moveinsync.scheduler.app.handler.JobHandler;
import com.moveinsync.scheduler.core.domain.HandlerType;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test-only single-effect handler: sends its one effect, and on the <b>first</b> attempt only then
 * signals {@link #effectSent} and blocks until {@link #release()} (bounded), so a test can freeze
 * the node between the last effect and the commit. Later attempts send and return at once.
 */
final class ItBlockingEffectHandler extends JobHandler<String> {

    private final HandlerType type;
    private final AtomicBoolean firstAttempt = new AtomicBoolean(true);
    final CountDownLatch effectSent = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);

    ItBlockingEffectHandler(ObjectMapper mapper, HandlerType type) {
        super(mapper);
        this.type = type;
    }

    void release() {
        release.countDown();
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
        ctx.send(ctx.run().idemKey(), "{\"effect\":\"blocking\"}");
        if (firstAttempt.compareAndSet(true, false)) {
            effectSent.countDown();
            if (!release.await(60, TimeUnit.SECONDS)) {
                throw new IllegalStateException("test never released the handler");
            }
        }
    }
}
