package com.moveinsync.scheduler.core.fairness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.core.domain.TenantId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DeficitRoundRobinDispatcherTest {

    private static final TenantId A = TenantId.of("a");
    private static final TenantId B = TenantId.of("b");
    private static final TenantId C = TenantId.of("c");

    record Item(TenantId tenant, int priority, int seq) {
    }

    private static DeficitRoundRobinDispatcher<Item> dispatcher() {
        return new DeficitRoundRobinDispatcher<>(Item::tenant, Item::priority);
    }

    private static void fill(DeficitRoundRobinDispatcher<Item> d, TenantId t, int priority, int n) {
        for (int i = 0; i < n; i++) {
            d.offer(new Item(t, priority, i));
        }
    }

    private static int lastIndexOfTenant(DeficitRoundRobinDispatcher<Item> d, TenantId t, int count) {
        int served = 0;
        for (int poll = 1; ; poll++) {
            Item item = d.poll().orElseThrow();
            if (item.tenant().equals(t) && ++served == count) {
                return poll;
            }
        }
    }

    @Test
    void smallTenantIsNotStuckBehindDeepQueue() {
        for (int priority : new int[] {1, 5, 9}) {
            DeficitRoundRobinDispatcher<Item> d = dispatcher();
            fill(d, A, priority, 50_000);
            fill(d, B, priority, 20);
            int last = lastIndexOfTenant(d, B, 20);
            assertThat(last).as("priority %d", priority).isLessThanOrEqualTo(2 * 20 + DeficitRoundRobinDispatcher.QUANTUM);
        }
    }

    @Test
    void priorityWeightsShareNineToOne() {
        DeficitRoundRobinDispatcher<Item> d = dispatcher();
        fill(d, A, 1, 10_000);
        fill(d, B, 9, 10_000);
        int a = 0;
        int b = 0;
        for (int i = 0; i < 1_000; i++) {
            if (d.poll().orElseThrow().tenant().equals(A)) {
                a++;
            } else {
                b++;
            }
        }
        // Per round: 9 priority-1 items (cost 1 each) vs 1 priority-9 item (cost 9).
        assertThat(a).isEqualTo(900);
        assertThat(b).isEqualTo(100);
    }

    @Test
    void mixedPrioritiesShareByCost() {
        DeficitRoundRobinDispatcher<Item> d = dispatcher();
        fill(d, A, 3, 10_000);
        fill(d, B, 9, 10_000);
        int a = 0;
        for (int i = 0; i < 4_000; i++) {
            if (d.poll().orElseThrow().tenant().equals(A)) {
                a++;
            }
        }
        // Throughput ratio 3:1 (cost 3 vs cost 9, same quantum).
        assertThat(a).isEqualTo(3_000);
    }

    @Test
    void fifoWithinTenant() {
        DeficitRoundRobinDispatcher<Item> d = dispatcher();
        for (int i = 0; i < 100; i++) {
            d.offer(new Item(A, 1 + i % 9, i));
            d.offer(new Item(B, 9 - i % 9, i));
        }
        List<Integer> seqA = new ArrayList<>();
        List<Integer> seqB = new ArrayList<>();
        Optional<Item> next;
        while ((next = d.poll()).isPresent()) {
            (next.get().tenant().equals(A) ? seqA : seqB).add(next.get().seq());
        }
        assertThat(seqA).isSorted().hasSize(100);
        assertThat(seqB).isSorted().hasSize(100);
    }

    @Test
    void recoveredLaneGoesFirst() {
        DeficitRoundRobinDispatcher<Item> d = dispatcher();
        fill(d, A, 1, 3);
        Item r1 = new Item(B, 9, 100);
        Item r2 = new Item(C, 5, 101);
        d.offerRecovered(r1);
        d.offer(r2, true);
        assertThat(d.recoveredSize()).isEqualTo(2);
        assertThat(d.size()).isEqualTo(5);
        assertThat(d.sizeOf(B)).isZero();
        assertThat(d.poll()).contains(r1);
        assertThat(d.poll()).contains(r2);
        assertThat(d.poll().orElseThrow().tenant()).isEqualTo(A);
    }

    @Test
    void predicateRoutesToRecoveredLane() {
        DeficitRoundRobinDispatcher<Item> d =
                new DeficitRoundRobinDispatcher<>(Item::tenant, Item::priority, i -> i.seq() < 0, 1);
        d.offer(new Item(A, 1, 1));
        d.offer(new Item(A, 1, -1));
        assertThat(d.recoveredSize()).isEqualTo(1);
        assertThat(d.poll().orElseThrow().seq()).isEqualTo(-1);
    }

    @Test
    void reservedWorkerOnlyForRecovered() {
        DeficitRoundRobinDispatcher<Item> d = dispatcher();
        fill(d, A, 1, 2);
        assertThat(d.canStartNormal(3, 4)).isFalse();
        assertThat(d.canStartNormal(2, 4)).isTrue();
        assertThat(d.canStartRecovered(3, 4)).isTrue();
        assertThat(d.canStartRecovered(4, 4)).isFalse();

        assertThat(d.poll(3, 4)).isEmpty();
        assertThat(d.size()).isEqualTo(2);
        Item r = new Item(B, 5, 0);
        d.offerRecovered(r);
        assertThat(d.poll(4, 4)).isEmpty();
        assertThat(d.poll(3, 4)).contains(r);
        assertThat(d.poll(4, 4)).isEmpty();
        assertThat(d.poll(2, 4)).isPresent();

        DeficitRoundRobinDispatcher<Item> none =
                new DeficitRoundRobinDispatcher<>(Item::tenant, Item::priority, i -> false, 0);
        fill(none, A, 1, 1);
        assertThat(none.poll(3, 4)).isPresent();
        assertThatThrownBy(() -> new DeficitRoundRobinDispatcher<>(Item::tenant, Item::priority, i -> false, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> d.poll(-1, 4)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void emptyTenantsLeaveRingAndDeficitResets() {
        DeficitRoundRobinDispatcher<Item> d = dispatcher();
        fill(d, A, 1, 1);
        fill(d, B, 1, 5);
        assertThat(d.activeTenants()).isEqualTo(2);
        assertThat(d.poll().orElseThrow().tenant()).isEqualTo(A);
        // A had deficit 9, spent 1, but it is empty now: it leaves the ring with nothing banked.
        assertThat(d.activeTenants()).isEqualTo(1);
        assertThat(d.deficitOf(A)).isZero();
        assertThat(d.depths()).containsExactly(Map.entry(B, 5));
        assertThat(d.poll().orElseThrow().tenant()).isEqualTo(B);
        assertThat(d.deficitOf(B)).isEqualTo(8);

        fill(d, A, 1, 1);
        assertThat(d.deficitOf(A)).isZero();
        assertThat(d.activeTenants()).isEqualTo(2);
    }

    @Test
    void sizesDepthsRemoveAndDrain() {
        DeficitRoundRobinDispatcher<Item> d = dispatcher();
        assertThat(d.isEmpty()).isTrue();
        assertThat(d.poll()).isEmpty();
        fill(d, A, 2, 3);
        fill(d, B, 2, 2);
        d.offerRecovered(new Item(A, 2, 9));
        assertThat(d.depths()).containsExactly(Map.entry(A, 3), Map.entry(B, 2));
        assertThat(d.sizeOf(A)).isEqualTo(3);
        assertThat(d.sizeOf(C)).isZero();

        List<Item> removed = d.removeTenant(A);
        assertThat(removed).extracting(Item::seq).containsExactly(9, 0, 1, 2);
        assertThat(d.size()).isEqualTo(2);
        assertThat(d.activeTenants()).isEqualTo(1);
        assertThat(d.removeTenant(C)).isEmpty();

        assertThat(d.drain()).extracting(Item::seq).containsExactly(0, 1);
        assertThat(d.isEmpty()).isTrue();
        assertThat(d.depths()).isEmpty();
    }

    @Test
    void rejectsBadPriority() {
        DeficitRoundRobinDispatcher<Item> d = dispatcher();
        assertThatThrownBy(() -> d.offer(new Item(A, 0, 0))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> d.offer(new Item(A, 10, 0))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> d.offer(null)).isInstanceOf(NullPointerException.class);
        assertThat(d.isEmpty()).isTrue();
        assertThat(DeficitRoundRobinDispatcher.costOf(1)).isEqualTo(1);
        assertThat(DeficitRoundRobinDispatcher.costOf(9)).isEqualTo(9);
    }

    @Test
    void claimedRunFactoryReservesOneWorker() {
        assertThat(DeficitRoundRobinDispatcher.forClaimedRuns().reservedForRecovery()).isEqualTo(1);
        assertThat(DeficitRoundRobinDispatcher.forClaimedRuns(2).reservedForRecovery()).isEqualTo(2);
    }

    @Test
    void concurrentProducersAndConsumersDeliverEachItemOnce() throws Exception {
        DeficitRoundRobinDispatcher<Item> d = dispatcher();
        int producers = 4;
        int perProducer = 20_000;
        int total = producers * perProducer;
        Set<Item> seen = ConcurrentHashMap.newKeySet();
        AtomicInteger delivered = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(producers + 3);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int p = 0; p < producers; p++) {
                TenantId tenant = TenantId.of("t" + p);
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < perProducer; i++) {
                        d.offer(new Item(tenant, 1 + i % 9, i), i % 50 == 0);
                    }
                    return null;
                }));
            }
            for (int c = 0; c < 3; c++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    while (delivered.get() < total) {
                        Optional<Item> item = d.poll(0, 4);
                        if (item.isPresent()) {
                            if (!seen.add(item.get())) {
                                duplicates.incrementAndGet();
                            }
                            delivered.incrementAndGet();
                        } else {
                            Thread.onSpinWait();
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(duplicates.get()).isZero();
        assertThat(seen).hasSize(total);
        assertThat(d.isEmpty()).isTrue();
    }
}
