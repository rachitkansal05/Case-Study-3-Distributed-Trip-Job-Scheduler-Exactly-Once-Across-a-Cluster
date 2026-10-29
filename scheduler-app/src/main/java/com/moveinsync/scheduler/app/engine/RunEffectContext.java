package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.app.handler.EffectContext;
import com.moveinsync.scheduler.app.handler.LeaseLostException;
import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.LeaseLostReason;
import com.moveinsync.scheduler.core.domain.RunContext;
import com.moveinsync.scheduler.core.policy.RetryableJobException;
import com.moveinsync.scheduler.core.ports.ClaimProtocol;
import com.moveinsync.scheduler.core.ports.EffectSink;
import java.util.Objects;

/**
 * The engine's {@link EffectContext}: the only path from a handler to the sink and to the
 * checkpoint, so the three fencing layers cannot be skipped.
 *
 * <ol>
 *   <li><b>Self-fence</b> before every effect: {@code LeaseClock.mayStartEffect()} (plus any lease
 *       loss already recorded by the renewer).
 *   <li><b>Sink fence</b>: every effect carries the run's token; a 409 is a lease loss.
 *   <li><b>DB fence</b>: checkpoints go through the fenced {@link ClaimProtocol#checkpoint}.
 * </ol>
 */
final class RunEffectContext implements EffectContext {

    private final ActiveRun active;
    private final RunContext run;
    private final String payloadJson;
    private final EffectSink sink;
    private final ClaimProtocol protocol;
    private final NodeGate gate;

    RunEffectContext(ActiveRun active, RunContext run, String payloadJson, EffectSink sink, ClaimProtocol protocol,
                     NodeGate gate) {
        this.active = Objects.requireNonNull(active, "active");
        this.run = Objects.requireNonNull(run, "run");
        this.payloadJson = Objects.requireNonNull(payloadJson, "payloadJson");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.protocol = Objects.requireNonNull(protocol, "protocol");
        this.gate = Objects.requireNonNull(gate, "gate");
    }

    @Override
    public RunContext run() {
        return run;
    }

    @Override
    public HandlerType handlerType() {
        return active.run().handler();
    }

    @Override
    public String payloadJson() {
        return payloadJson;
    }

    @Override
    public boolean mayStartEffect() {
        return active.mayStartEffect();
    }

    @Override
    public DeliveryOutcome send(String idemKey, String effectPayloadJson) {
        ensureMayStartEffect();
        passGate(); // a pause can land here, between the self-fence check and the send
        DeliveryOutcome outcome = sink.send(run.runId(), idemKey, run.fencingToken(), effectPayloadJson);
        if (outcome == DeliveryOutcome.STALE_TOKEN) {
            active.loseLease(LeaseLostReason.STALE_TOKEN, false);
            throw new LeaseLostException(LeaseLostReason.STALE_TOKEN);
        }
        return outcome;
    }

    @Override
    public void checkpoint(String checkpointJson) {
        if (active.lost() != null) {
            throw new LeaseLostException(active.lost());
        }
        passGate();
        if (!protocol.checkpoint(run.runId(), run.fencingToken(), checkpointJson)) {
            active.loseLease(LeaseLostReason.FENCED_WRITE_REJECTED, false);
            throw new LeaseLostException(LeaseLostReason.FENCED_WRITE_REJECTED);
        }
    }

    private void ensureMayStartEffect() {
        LeaseLostReason lost = active.lost();
        if (lost != null) {
            throw new LeaseLostException(lost);
        }
        if (!active.mayStartEffect()) {
            if (active.timedOut()) {
                throw new RetryableJobException("TIMEOUT", "max run time exceeded");
            }
            active.loseLease(LeaseLostReason.SELF_FENCE, false);
            throw new LeaseLostException(LeaseLostReason.SELF_FENCE);
        }
    }

    private void passGate() {
        try {
            gate.pass();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableJobException("INTERRUPTED", "worker interrupted");
        }
    }
}
