package com.moveinsync.scheduler.app.sink;

import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.ports.EffectSink;

/**
 * An {@link EffectSink} that also takes the delivery channel. The engine always works with a view
 * {@link #boundTo(HandlerType) bound} to the run's handler, so the channel is never guessed.
 */
public interface ChannelAwareEffectSink extends EffectSink {

    /**
     * Sends one effect on an explicit channel.
     *
     * @param channel sink channel
     * @param runId run the effect belongs to (fencing is per run)
     * @param idemKey run or item idempotency key
     * @param token fencing token of the current claim
     * @param payloadJson effect body, a JSON object; never logged
     * @return the sink's verdict
     */
    DeliveryOutcome send(SinkChannel channel, long runId, String idemKey, FencingToken token, String payloadJson);

    /**
     * Port view whose {@code send} uses {@link SinkChannel#forHandler(HandlerType)} and whose
     * {@code highestAcceptedEpoch} delegates to this sink.
     */
    default EffectSink boundTo(HandlerType handler) {
        SinkChannel channel = SinkChannel.forHandler(handler);
        ChannelAwareEffectSink self = this;
        return new EffectSink() {
            @Override
            public DeliveryOutcome send(long runId, String idemKey, FencingToken token, String payloadJson) {
                return self.send(channel, runId, idemKey, token, payloadJson);
            }

            @Override
            public long highestAcceptedEpoch() {
                return self.highestAcceptedEpoch();
            }

            @Override
            public String toString() {
                return "EffectSink[" + channel + "]";
            }
        };
    }
}
