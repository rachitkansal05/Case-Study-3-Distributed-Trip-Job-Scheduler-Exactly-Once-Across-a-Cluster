package com.moveinsync.scheduler.app.engine.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.policy.PermanentJobException;
import com.moveinsync.scheduler.core.policy.RetryableJobException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class FakeEffectSinkTest {

    private static final FencingToken T7_41 = new FencingToken(7, 41);
    private static final FencingToken T7_42 = new FencingToken(7, 42);

    private final FakeEffectSink sink = new FakeEffectSink();

    @Test
    void firstDeliveredThenAlreadyDelivered() {
        assertThat(sink.send(1, "k", T7_41, "{}")).isEqualTo(DeliveryOutcome.DELIVERED);
        assertThat(sink.send(1, "k", T7_41, "{}")).isEqualTo(DeliveryOutcome.ALREADY_DELIVERED);
        assertThat(sink.distinctDeliveries()).isEqualTo(1);
        assertThat(sink.attemptCount()).isEqualTo(2);
        assertThat(sink.maxDeliveriesPerKey()).isEqualTo(1);
        assertThat(sink.attempts()).extracting(FakeEffectSink.Attempt::outcome)
                .containsExactly(DeliveryOutcome.DELIVERED, DeliveryOutcome.ALREADY_DELIVERED);
    }

    @Test
    void fenceCheckComesBeforeKeyCheck() {
        sink.send(1, "k", T7_41, "{}");
        sink.send(1, "other", T7_42, "{}");
        // k was delivered, but a stale holder still gets STALE_TOKEN, not ALREADY_DELIVERED
        assertThat(sink.send(1, "k", T7_41, "{}")).isEqualTo(DeliveryOutcome.STALE_TOKEN);
        assertThat(sink.staleRejections()).isEqualTo(1);
        assertThat(sink.attemptCount()).isEqualTo(3);
        assertThat(sink.fenceOf(1)).isEqualTo(T7_42);
    }

    @Test
    void staleCallDeliversNothing() {
        sink.send(1, "a", T7_42, "{}");
        assertThat(sink.send(1, "b", T7_41, "{}")).isEqualTo(DeliveryOutcome.STALE_TOKEN);
        assertThat(sink.deliveryOf("b")).isNull();
        assertThat(sink.deliveriesFor(1)).hasSize(1);
    }

    @Test
    void equalTokensAcceptedForManyKeys() {
        for (int i = 0; i < 5; i++) {
            assertThat(sink.send(1, "k" + i, T7_41, "{}")).isEqualTo(DeliveryOutcome.DELIVERED);
        }
        assertThat(sink.send(1, "k0", T7_41, "{}")).isEqualTo(DeliveryOutcome.ALREADY_DELIVERED);
        assertThat(sink.staleRejections()).isZero();
    }

    @Test
    void keysAreGlobalAcrossRuns() {
        assertThat(sink.send(1, "k", T7_41, "{}")).isEqualTo(DeliveryOutcome.DELIVERED);
        assertThat(sink.send(2, "k", new FencingToken(1, 1), "{\"different\":true}"))
                .isEqualTo(DeliveryOutcome.ALREADY_DELIVERED);
        assertThat(sink.deliveryOf("k").runId()).isEqualTo(1);
        assertThat(sink.deliveriesFor(2)).isEmpty();
    }

    @Test
    void fencesArePerRun() {
        sink.send(1, "a", new FencingToken(9, 9), "{}");
        assertThat(sink.send(2, "b", new FencingToken(1, 1), "{}")).isEqualTo(DeliveryOutcome.DELIVERED);
    }

    @Test
    void epochBeatsSeq() {
        sink.send(1, "a", new FencingToken(8, 1), "{}");
        assertThat(sink.send(1, "b", new FencingToken(7, 999_999), "{}")).isEqualTo(DeliveryOutcome.STALE_TOKEN);
        long gen1 = FencingToken.epochOf(1, 1);
        assertThat(sink.send(1, "c", new FencingToken(gen1, 0), "{}")).isEqualTo(DeliveryOutcome.DELIVERED);
        assertThat(sink.highestAcceptedEpoch()).isEqualTo(gen1);
    }

    @Test
    void highestEpochOnlyFromAcceptedTokens() {
        assertThat(sink.highestAcceptedEpoch()).isZero();
        sink.send(1, "a", T7_42, "{}");
        sink.send(1, "b", new FencingToken(7, 1), "{}");
        assertThat(sink.highestAcceptedEpoch()).isEqualTo(7);
        sink.setHighestEpochOverride(99L);
        assertThat(sink.highestAcceptedEpoch()).isEqualTo(99);
        sink.setHighestEpochOverride(null);
        assertThat(sink.highestAcceptedEpoch()).isEqualTo(7);
    }

    @Test
    void injectedFailureWritesNothing() {
        sink.setFailureRate(1.0);
        assertThatThrownBy(() -> sink.send(1, "k", T7_41, "{}"))
                .isInstanceOfSatisfying(RetryableJobException.class,
                        e -> assertThat(e.errorCode()).isEqualTo("SINK_5XX"));
        assertThat(sink.attemptCount()).isZero();
        assertThat(sink.distinctDeliveries()).isZero();
        assertThat(sink.fenceOf(1)).isNull();
        assertThat(sink.highestAcceptedEpoch()).isZero();
        sink.setFailureRate(0.0);
        assertThat(sink.send(1, "k", T7_41, "{}")).isEqualTo(DeliveryOutcome.DELIVERED);
    }

    @Test
    void invalidRequestsRejectedLikeA400() {
        assertThatThrownBy(() -> sink.send(1, "has space", T7_41, "{}")).isInstanceOf(PermanentJobException.class);
        assertThatThrownBy(() -> sink.send(1, "k", T7_41, "[1]")).isInstanceOf(PermanentJobException.class);
        assertThatThrownBy(() -> sink.send(1, "k".repeat(257), T7_41, "{}")).isInstanceOf(PermanentJobException.class);
        assertThat(sink.attemptCount()).isZero();
    }

    @Test
    void admissionValidatorRejectsAsStaleWithoutRaisingFence() {
        sink.setAdmissionValidator((runId, token) -> token.seq() != 50);
        assertThat(sink.send(1, "a", new FencingToken(7, 50), "{}")).isEqualTo(DeliveryOutcome.STALE_TOKEN);
        assertThat(sink.fenceOf(1)).isNull();
        assertThat(sink.send(1, "a", T7_41, "{}")).isEqualTo(DeliveryOutcome.DELIVERED);
        assertThat(sink.staleRejections()).isEqualTo(1);
    }

    @Test
    void latencyAppliedBeforeDecision() {
        sink.setLatency(Duration.ofMillis(50));
        long start = System.nanoTime();
        sink.send(1, "k", T7_41, "{}");
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(45));
    }

    @Test
    void tracksInFlightPerGroup() throws Exception {
        sink.setGroupOf(runId -> runId % 2 == 0 ? "even" : "odd");
        sink.setHoldAfterAdmission(Duration.ofMillis(200));
        sink.blockSends(); // line all four up, then release them together
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            List<Future<DeliveryOutcome>> futures = new ArrayList<>();
            for (int i = 1; i <= 4; i++) {
                long runId = i;
                futures.add(pool.submit(() -> sink.send(runId, "k" + runId, T7_41, "{}")));
            }
            Thread.sleep(100);
            sink.unblockSends();
            for (Future<DeliveryOutcome> f : futures) {
                assertThat(f.get(5, TimeUnit.SECONDS)).isEqualTo(DeliveryOutcome.DELIVERED);
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(sink.maxInFlight("even")).isEqualTo(2);
        assertThat(sink.maxInFlight("odd")).isEqualTo(2);
        assertThat(sink.inFlight("even")).isZero();
    }

    @Test
    void blockedSendsWaitUntilUnblocked() throws Exception {
        sink.blockSends();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<DeliveryOutcome> f = pool.submit(() -> sink.send(1, "k", T7_41, "{}"));
            Thread.sleep(100);
            assertThat(f.isDone()).isFalse();
            assertThat(sink.attemptCount()).isZero();
            sink.unblockSends();
            assertThat(f.get(5, TimeUnit.SECONDS)).isEqualTo(DeliveryOutcome.DELIVERED);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void concurrentDuplicatesDeliverOnce() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<DeliveryOutcome>> futures = new ArrayList<>();
            for (int i = 0; i < 400; i++) {
                String key = "k" + (i % 50);
                futures.add(pool.submit(() -> sink.send(1, key, T7_41, "{}")));
            }
            for (Future<DeliveryOutcome> f : futures) {
                f.get(5, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(sink.distinctDeliveries()).isEqualTo(50);
        assertThat(sink.maxDeliveriesPerKey()).isEqualTo(1);
        assertThat(sink.attemptCount()).isEqualTo(400);
    }
}
