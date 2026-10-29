package com.moveinsync.scheduler.app.handler;

import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.LeaseLostReason;
import com.moveinsync.scheduler.core.domain.RunContext;
import com.moveinsync.scheduler.core.domain.TenantId;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.IntConsumer;

/** Test {@link EffectContext}: sends to a {@link FakeEffectSink} and records sends and checkpoints. */
final class RecordingEffectContext implements EffectContext {

    record Sent(String idemKey, String payloadJson, DeliveryOutcome outcome) {}

    private final RunContext run;
    private final HandlerType type;
    private final String payloadJson;
    private final FakeEffectSink sink;
    private final List<Sent> sends = new ArrayList<>();
    private final List<String> checkpoints = new ArrayList<>();
    private IntConsumer beforeSend = i -> {};

    RecordingEffectContext(RunContext run, HandlerType type, String payloadJson, FakeEffectSink sink) {
        this.run = run;
        this.type = type;
        this.payloadJson = payloadJson;
        this.sink = sink;
    }

    /** Context for run 77, key {@code idem}, token 7.41. */
    static RecordingEffectContext of(HandlerType type, String payloadJson, String checkpointJson, FakeEffectSink sink) {
        return new RecordingEffectContext(run(checkpointJson), type, payloadJson, sink);
    }

    static RunContext run(String checkpointJson) {
        return new RunContext(77L, UUID.randomUUID(), tenant(), 1, "idem", new FencingToken(7, 41),
                checkpointJson, Instant.now().plusSeconds(60));
    }

    private static TenantId tenant() {
        return new TenantId("t1");
    }

    /** Hook called with the 0-based send index before each send. */
    RecordingEffectContext beforeSend(IntConsumer hook) {
        this.beforeSend = hook;
        return this;
    }

    @Override
    public RunContext run() {
        return run;
    }

    @Override
    public HandlerType handlerType() {
        return type;
    }

    @Override
    public String payloadJson() {
        return payloadJson;
    }

    @Override
    public boolean mayStartEffect() {
        return true;
    }

    @Override
    public DeliveryOutcome send(String idemKey, String effectPayloadJson) {
        beforeSend.accept(sends.size());
        DeliveryOutcome outcome = sink.send(run.runId(), idemKey, run.fencingToken(), effectPayloadJson);
        sends.add(new Sent(idemKey, effectPayloadJson, outcome));
        if (outcome == DeliveryOutcome.STALE_TOKEN) {
            throw new LeaseLostException(LeaseLostReason.STALE_TOKEN);
        }
        return outcome;
    }

    @Override
    public void checkpoint(String checkpointJson) {
        checkpoints.add(checkpointJson);
    }

    List<Sent> sends() {
        return sends;
    }

    List<String> checkpoints() {
        return checkpoints;
    }
}
