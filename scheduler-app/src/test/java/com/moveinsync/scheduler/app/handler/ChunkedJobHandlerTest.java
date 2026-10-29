package com.moveinsync.scheduler.app.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.LeaseLostReason;
import com.moveinsync.scheduler.core.policy.PermanentJobException;
import com.moveinsync.scheduler.core.policy.RetryableJobException;
import java.io.UncheckedIOException;
import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class ChunkedJobHandlerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final FakeEffectSink sink = new FakeEffectSink();

    /** Payload is the item count; item ids are emp0..emp{n-1}. */
    private final class CountingHandler extends ChunkedJobHandler<Integer> {
        CountingHandler() {
            super(mapper);
        }

        @Override
        public HandlerType type() {
            return HandlerType.PICKUP_ALERT;
        }

        @Override
        protected Integer parsePayload(String payloadJson) {
            return readObject(payloadJson).get("n").intValue();
        }

        @Override
        protected List<Item> items(Integer n) {
            return IntStream.range(0, n).mapToObj(i -> new Item("emp" + i, "{\"i\":" + i + "}")).toList();
        }
    }

    private RecordingEffectContext ctx(int n, String checkpoint) {
        return RecordingEffectContext.of(HandlerType.PICKUP_ALERT, "{\"n\":" + n + "}", checkpoint, sink);
    }

    @Test
    void sends1200ItemsWith24Checkpoints() throws Exception {
        RecordingEffectContext ctx = ctx(1200, null);
        new CountingHandler().handle(ctx);

        assertThat(ctx.sends()).hasSize(1200);
        assertThat(ctx.sends().get(0).idemKey()).isEqualTo("idem:emp0");
        assertThat(ctx.sends().get(1199).idemKey()).isEqualTo("idem:emp1199");
        assertThat(ctx.checkpoints()).hasSize(24);
        assertThat(ctx.checkpoints().get(0)).isEqualTo("{\"nextItem\":50}");
        assertThat(ctx.checkpoints().get(23)).isEqualTo("{\"nextItem\":1200}");
        assertThat(sink.distinctDeliveries()).isEqualTo(1200);
        assertThat(sink.maxDeliveriesPerKey()).isEqualTo(1);
    }

    @Test
    void finalPartialChunkIsCheckpointed() throws Exception {
        RecordingEffectContext ctx = ctx(120, null);
        new CountingHandler().handle(ctx);
        assertThat(ctx.checkpoints()).containsExactly("{\"nextItem\":50}", "{\"nextItem\":100}", "{\"nextItem\":120}");
    }

    @Test
    void resumesFromCheckpoint() throws Exception {
        RecordingEffectContext ctx = ctx(1200, "{\"nextItem\":650}");
        new CountingHandler().handle(ctx);

        assertThat(ctx.sends()).hasSize(550);
        assertThat(ctx.sends().get(0).idemKey()).isEqualTo("idem:emp650");
        assertThat(ctx.checkpoints()).hasSize(11).last().isEqualTo("{\"nextItem\":1200}");
    }

    @Test
    void replayOfAChunkIsAlreadyDelivered() throws Exception {
        new CountingHandler().handle(ctx(100, null));
        RecordingEffectContext replay = ctx(100, "{\"nextItem\":50}");
        new CountingHandler().handle(replay);
        assertThat(replay.sends()).extracting(RecordingEffectContext.Sent::outcome)
                .containsOnly(DeliveryOutcome.ALREADY_DELIVERED);
        assertThat(sink.distinctDeliveries()).isEqualTo(100);
    }

    @Test
    void checkpointAtEndSendsNothing() throws Exception {
        RecordingEffectContext ctx = ctx(10, "{\"nextItem\":10}");
        new CountingHandler().handle(ctx);
        assertThat(ctx.sends()).isEmpty();
        assertThat(ctx.checkpoints()).isEmpty();
    }

    @Test
    void malformedCheckpointIsPermanent() {
        for (String bad : List.of("not json", "[]", "{}", "{\"nextItem\":-1}", "{\"nextItem\":11}",
                "{\"nextItem\":\"5\"}", "{\"nextItem\":1.5}")) {
            RecordingEffectContext ctx = ctx(10, bad);
            assertThatThrownBy(() -> new CountingHandler().handle(ctx))
                    .isInstanceOfSatisfying(PermanentJobException.class,
                            e -> assertThat(e.errorCode()).isEqualTo("INVALID_CHECKPOINT"));
            assertThat(ctx.sends()).isEmpty();
        }
    }

    @Test
    void staleTokenStopsSending() {
        RecordingEffectContext ctx = ctx(200, null).beforeSend(i -> {
            if (i == 120) { // a newer claim for the same run reaches the sink
                sink.send(77L, "other", new FencingToken(7, 42), "{}");
            }
        });
        assertThatThrownBy(() -> new CountingHandler().handle(ctx))
                .isInstanceOfSatisfying(LeaseLostException.class,
                        e -> assertThat(e.reason()).isEqualTo(LeaseLostReason.STALE_TOKEN));
        assertThat(ctx.sends()).hasSize(121);
        assertThat(ctx.checkpoints()).hasSize(2);
        assertThat(sink.deliveriesFor(77L)).hasSize(121); // 120 items + "other"
    }

    @Test
    void interruptStopsBetweenItems() {
        RecordingEffectContext ctx = ctx(200, null).beforeSend(i -> {
            if (i == 10) {
                Thread.currentThread().interrupt();
            }
        });
        assertThatThrownBy(() -> new CountingHandler().handle(ctx)).isInstanceOf(InterruptedException.class);
        assertThat(ctx.sends()).hasSize(11);
        assertThat(Thread.interrupted()).isFalse();
    }

    @Test
    void ioErrorsBecomeRetryable() {
        RecordingEffectContext ctx = ctx(5, null).beforeSend(i -> {
            throw new UncheckedIOException(new IOException("boom"));
        });
        assertThatThrownBy(() -> new CountingHandler().handle(ctx))
                .isInstanceOfSatisfying(RetryableJobException.class,
                        e -> assertThat(e.errorCode()).isEqualTo("IO_ERROR"));
    }

    @Test
    void otherRuntimeExceptionsPropagate() {
        RecordingEffectContext ctx = ctx(5, null).beforeSend(i -> {
            throw new IllegalStateException("bug");
        });
        assertThatThrownBy(() -> new CountingHandler().handle(ctx)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void retryableFromSinkPropagatesUnchanged() {
        sink.setFailureRate(1.0);
        RecordingEffectContext ctx = ctx(5, null);
        assertThatThrownBy(() -> new CountingHandler().handle(ctx))
                .isInstanceOfSatisfying(RetryableJobException.class,
                        e -> assertThat(e.errorCode()).isEqualTo("SINK_5XX"));
    }

    @Test
    void unparseablePayloadIsInvalidPayload() {
        RecordingEffectContext ctx = RecordingEffectContext.of(HandlerType.PICKUP_ALERT, "{\"m\":1}", null, sink);
        assertThatThrownBy(() -> new CountingHandler().handle(ctx))
                .isInstanceOfSatisfying(PermanentJobException.class,
                        e -> assertThat(e.errorCode()).isEqualTo("INVALID_PAYLOAD"));
    }

    @Test
    void customChunkSize() throws Exception {
        ChunkedJobHandler<Integer> handler = new ChunkedJobHandler<>(mapper, 7) {
            @Override
            public HandlerType type() {
                return HandlerType.PICKUP_ALERT;
            }

            @Override
            protected Integer parsePayload(String payloadJson) {
                return 20;
            }

            @Override
            protected List<Item> items(Integer n) {
                return IntStream.range(0, n).mapToObj(i -> new Item("x" + i, "{}")).toList();
            }
        };
        RecordingEffectContext ctx = ctx(0, null);
        handler.handle(ctx);
        assertThat(ctx.checkpoints()).containsExactly(
                "{\"nextItem\":7}", "{\"nextItem\":14}", "{\"nextItem\":20}");
    }

    @Test
    void invalidItemIdRejectedBeforeAnySend() {
        ChunkedJobHandler<Integer> handler = new ChunkedJobHandler<>(mapper) {
            @Override
            public HandlerType type() {
                return HandlerType.PICKUP_ALERT;
            }

            @Override
            protected Integer parsePayload(String payloadJson) {
                return 0;
            }

            @Override
            protected List<Item> items(Integer n) {
                return List.of(new Item("ok", "{}"), new Item("has space", "{}"));
            }
        };
        RecordingEffectContext ctx = ctx(0, null);
        assertThatThrownBy(() -> handler.handle(ctx)).isInstanceOf(PermanentJobException.class);
        assertThat(ctx.sends()).isEmpty();
    }
}
