package com.moveinsync.scheduler.core.fairness;

import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.TenantId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/**
 * Local dispatch queue between the claim loop and the worker pool (plan sections 06 and 10). It
 * decides the <em>order</em> in which claimed items start; the tenant cap itself is enforced by
 * slot rows in the database, not here.
 *
 * <h2>Lanes</h2>
 *
 * <ul>
 *   <li><b>Recovered lane</b>: one FIFO queue for runs taken over by the orphan sweep. It is always
 *       served before any tenant queue, so a takeover never waits behind new work.
 *   <li><b>Tenant lanes</b>: one FIFO queue per tenant. Order inside a tenant is the offer order;
 *       the claim query already orders by priority and due time, so the dispatcher does not
 *       re-sort.
 * </ul>
 *
 * <h2>Deficit round robin over tenants</h2>
 *
 * Tenants with queued work form an active ring. When a tenant reaches the head of the ring its
 * turn starts and its deficit grows by {@link #QUANTUM} ({@value #QUANTUM}). Serving its head item
 * costs {@link #costOf(int) costOf(priority)} = {@code priority}, so priority 1 (most urgent) costs
 * 1 and priority 9 costs 9. The tenant keeps being served while its head item's cost fits in the
 * deficit; otherwise the unused deficit is carried over and the tenant moves to the back of the
 * ring. Per round a backlogged tenant therefore receives about 9 priority-1 items, 1.8 priority-5
 * items or 1 priority-9 item: throughput between backlogged tenants is in inverse proportion to
 * the priority number of their work, and one tenant's queue depth has no effect on another's
 * share.
 *
 * <p>Because the largest cost equals the quantum, every tenant serves at least one item per turn,
 * so no tenant can starve another: with {@code n} active tenants an item at the head of its
 * tenant's queue starts within at most {@code 9 * (n - 1)} other starts. A tenant whose queue
 * becomes empty leaves the ring and its deficit is reset to 0 (classic DRR), so idle tenants
 * cannot bank credit. Every operation except {@link #removeTenant}, {@link #drain} and {@link
 * #depths} is O(1) amortised; no operation scans all tenants on a poll.
 *
 * <h2>Reserved worker</h2>
 *
 * {@code reservedForRecovery} workers (config {@code scheduler.workers.reserved-for-recovery},
 * default 1) are kept free for recovered runs: a normal item may start only while {@code busy <
 * total - reserved}; a recovered item may start while {@code busy < total}. If {@code total <=
 * reserved} normal items never start through {@link #poll(int, int)}.
 *
 * <h2>Thread safety</h2>
 *
 * All state is guarded by one {@link ReentrantLock}; every public method is atomic. Extractor
 * functions are called under the lock and must be cheap and side-effect free.
 *
 * @param <T> queued item, normally {@link ClaimedRun}
 */
public final class DeficitRoundRobinDispatcher<T> {

    /** Deficit added to a tenant at the start of each of its turns. */
    public static final int QUANTUM = 9;

    /** Most urgent priority. */
    public static final int MIN_PRIORITY = 1;

    /** Least urgent priority. */
    public static final int MAX_PRIORITY = 9;

    /** Default number of workers kept free for recovered runs. */
    public static final int DEFAULT_RESERVED_FOR_RECOVERY = 1;

    private final Function<T, TenantId> tenantOf;
    private final ToIntFunction<T> priorityOf;
    private final Predicate<T> recoveredOf;
    private final int reservedForRecovery;

    private final ReentrantLock lock = new ReentrantLock();
    private final ArrayDeque<T> recovered = new ArrayDeque<>();
    private final Map<TenantId, TenantQueue<T>> tenants = new HashMap<>();
    private final ArrayDeque<TenantQueue<T>> ring = new ArrayDeque<>();
    private int normalSize;

    /**
     * Creates a dispatcher.
     *
     * @param tenantOf owning tenant of an item, non-null result
     * @param priorityOf priority of an item, {@code 1..9}, 1 most urgent
     * @param recoveredOf whether {@link #offer(Object)} routes an item to the recovered lane
     * @param reservedForRecovery workers kept free for recovered items, {@code >= 0}
     */
    public DeficitRoundRobinDispatcher(
            Function<T, TenantId> tenantOf,
            ToIntFunction<T> priorityOf,
            Predicate<T> recoveredOf,
            int reservedForRecovery) {
        this.tenantOf = Objects.requireNonNull(tenantOf, "tenantOf");
        this.priorityOf = Objects.requireNonNull(priorityOf, "priorityOf");
        this.recoveredOf = Objects.requireNonNull(recoveredOf, "recoveredOf");
        if (reservedForRecovery < 0) {
            throw new IllegalArgumentException("reservedForRecovery must be >= 0");
        }
        this.reservedForRecovery = reservedForRecovery;
    }

    /**
     * Creates a dispatcher whose {@link #offer(Object)} always uses the tenant lanes and that
     * reserves {@value #DEFAULT_RESERVED_FOR_RECOVERY} worker.
     *
     * @param tenantOf owning tenant of an item
     * @param priorityOf priority of an item, {@code 1..9}
     */
    public DeficitRoundRobinDispatcher(Function<T, TenantId> tenantOf, ToIntFunction<T> priorityOf) {
        this(tenantOf, priorityOf, item -> false, DEFAULT_RESERVED_FOR_RECOVERY);
    }

    /**
     * Dispatcher for claimed runs, routing by {@link ClaimedRun#recovered()}.
     *
     * @param reservedForRecovery workers kept free for recovered runs, {@code >= 0}
     * @return a new empty dispatcher
     */
    public static DeficitRoundRobinDispatcher<ClaimedRun> forClaimedRuns(int reservedForRecovery) {
        return new DeficitRoundRobinDispatcher<>(
                ClaimedRun::tenantId, ClaimedRun::priority, ClaimedRun::recovered, reservedForRecovery);
    }

    /**
     * Dispatcher for claimed runs with {@value #DEFAULT_RESERVED_FOR_RECOVERY} reserved worker.
     *
     * @return a new empty dispatcher
     */
    public static DeficitRoundRobinDispatcher<ClaimedRun> forClaimedRuns() {
        return forClaimedRuns(DEFAULT_RESERVED_FOR_RECOVERY);
    }

    /**
     * DRR cost of serving an item of the given priority: equal to the priority number.
     *
     * @param priority {@code 1..9}
     * @return cost in deficit units, {@code 1..9}
     */
    public static int costOf(int priority) {
        checkPriority(priority);
        return priority;
    }

    /** @return workers kept free for recovered items */
    public int reservedForRecovery() {
        return reservedForRecovery;
    }

    /**
     * Enqueues an item, routing it to the recovered lane if the {@code recoveredOf} predicate says
     * so.
     *
     * @param item item to enqueue
     */
    public void offer(T item) {
        Objects.requireNonNull(item, "item");
        offer(item, recoveredOf.test(item));
    }

    /**
     * Enqueues an item into the recovered lane or its tenant's lane.
     *
     * @param item item to enqueue
     * @param recoveredLane true for the recovered lane
     * @throws IllegalArgumentException if the item's priority is outside {@code 1..9}
     */
    public void offer(T item, boolean recoveredLane) {
        Objects.requireNonNull(item, "item");
        lock.lock();
        try {
            if (recoveredLane) {
                recovered.addLast(item);
                return;
            }
            TenantId tenant = Objects.requireNonNull(tenantOf.apply(item), "tenant");
            checkPriority(priorityOf.applyAsInt(item));
            TenantQueue<T> queue = tenants.get(tenant);
            if (queue == null) {
                queue = new TenantQueue<>(tenant);
                tenants.put(tenant, queue);
                ring.addLast(queue);
            }
            queue.items.addLast(item);
            normalSize++;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Enqueues an item into the recovered lane.
     *
     * @param item item to enqueue
     */
    public void offerRecovered(T item) {
        offer(item, true);
    }

    /**
     * Whether a normal item may start.
     *
     * @param busyWorkers workers currently executing, {@code >= 0}
     * @param totalWorkers pool size, {@code >= 0}
     * @return {@code busyWorkers < totalWorkers - reservedForRecovery}
     */
    public boolean canStartNormal(int busyWorkers, int totalWorkers) {
        checkWorkers(busyWorkers, totalWorkers);
        return busyWorkers < totalWorkers - reservedForRecovery;
    }

    /**
     * Whether a recovered item may start.
     *
     * @param busyWorkers workers currently executing, {@code >= 0}
     * @param totalWorkers pool size, {@code >= 0}
     * @return {@code busyWorkers < totalWorkers}
     */
    public boolean canStartRecovered(int busyWorkers, int totalWorkers) {
        checkWorkers(busyWorkers, totalWorkers);
        return busyWorkers < totalWorkers;
    }

    /**
     * Takes the next item that may start given the worker pool state: the head of the recovered
     * lane if any and {@link #canStartRecovered}, otherwise the next DRR item if {@link
     * #canStartNormal}. A queued recovered item that cannot start (pool full) blocks nothing
     * because then no normal item could start either.
     *
     * @param busyWorkers workers currently executing
     * @param totalWorkers pool size
     * @return the item to start, or empty
     */
    public Optional<T> poll(int busyWorkers, int totalWorkers) {
        checkWorkers(busyWorkers, totalWorkers);
        lock.lock();
        try {
            if (!recovered.isEmpty()) {
                return busyWorkers < totalWorkers ? Optional.of(recovered.pollFirst()) : Optional.empty();
            }
            return busyWorkers < totalWorkers - reservedForRecovery ? pollNormal() : Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Takes the next item ignoring worker availability: recovered lane first, then DRR.
     *
     * @return the next item, or empty if nothing is queued
     */
    public Optional<T> poll() {
        lock.lock();
        try {
            if (!recovered.isEmpty()) {
                return Optional.of(recovered.pollFirst());
            }
            return pollNormal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes every queued item of a tenant from both lanes (for example when the tenant is
     * paused). O(n) in the recovered lane length.
     *
     * @param tenant tenant to remove
     * @return removed items, recovered ones first, each lane in FIFO order
     */
    public List<T> removeTenant(TenantId tenant) {
        Objects.requireNonNull(tenant, "tenant");
        lock.lock();
        try {
            List<T> removed = new ArrayList<>();
            Iterator<T> it = recovered.iterator();
            while (it.hasNext()) {
                T item = it.next();
                if (tenant.equals(tenantOf.apply(item))) {
                    removed.add(item);
                    it.remove();
                }
            }
            TenantQueue<T> queue = tenants.remove(tenant);
            if (queue != null) {
                ring.remove(queue);
                normalSize -= queue.items.size();
                removed.addAll(queue.items);
            }
            return Collections.unmodifiableList(removed);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes everything (for example on shutdown, to release the claims).
     *
     * @return all items: the recovered lane in FIFO order, then each tenant lane in ring order
     */
    public List<T> drain() {
        lock.lock();
        try {
            List<T> all = new ArrayList<>(recovered.size() + normalSize);
            all.addAll(recovered);
            for (TenantQueue<T> queue : ring) {
                all.addAll(queue.items);
            }
            recovered.clear();
            ring.clear();
            tenants.clear();
            normalSize = 0;
            return Collections.unmodifiableList(all);
        } finally {
            lock.unlock();
        }
    }

    /** @return total queued items, both lanes */
    public int size() {
        lock.lock();
        try {
            return recovered.size() + normalSize;
        } finally {
            lock.unlock();
        }
    }

    /** @return items in the recovered lane */
    public int recoveredSize() {
        lock.lock();
        try {
            return recovered.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param tenant tenant
     * @return items in that tenant's lane (recovered lane not included)
     */
    public int sizeOf(TenantId tenant) {
        Objects.requireNonNull(tenant, "tenant");
        lock.lock();
        try {
            TenantQueue<T> queue = tenants.get(tenant);
            return queue == null ? 0 : queue.items.size();
        } finally {
            lock.unlock();
        }
    }

    /** @return immutable snapshot of tenant lane depths, only tenants with work, in ring order */
    public Map<TenantId, Integer> depths() {
        lock.lock();
        try {
            Map<TenantId, Integer> copy = new LinkedHashMap<>();
            for (TenantQueue<T> queue : ring) {
                copy.put(queue.tenant, queue.items.size());
            }
            return Collections.unmodifiableMap(copy);
        } finally {
            lock.unlock();
        }
    }

    /** @return true if both lanes are empty */
    public boolean isEmpty() {
        return size() == 0;
    }

    /** Current deficit of a tenant, 0 if it has no work. For tests. */
    int deficitOf(TenantId tenant) {
        lock.lock();
        try {
            TenantQueue<T> queue = tenants.get(tenant);
            return queue == null ? 0 : queue.deficit;
        } finally {
            lock.unlock();
        }
    }

    /** Number of tenants in the active ring. For tests. */
    int activeTenants() {
        lock.lock();
        try {
            return ring.size();
        } finally {
            lock.unlock();
        }
    }

    private Optional<T> pollNormal() {
        // Terminates within two iterations: a fresh turn adds QUANTUM >= any cost.
        while (!ring.isEmpty()) {
            TenantQueue<T> queue = ring.peekFirst();
            if (!queue.inTurn) {
                queue.deficit += QUANTUM;
                queue.inTurn = true;
            }
            T head = queue.items.peekFirst();
            int cost = costOf(priorityOf.applyAsInt(head));
            if (cost <= queue.deficit) {
                queue.deficit -= cost;
                queue.items.pollFirst();
                normalSize--;
                if (queue.items.isEmpty()) {
                    ring.pollFirst();
                    tenants.remove(queue.tenant);
                    queue.deficit = 0;
                    queue.inTurn = false;
                }
                return Optional.of(head);
            }
            queue.inTurn = false;
            ring.addLast(ring.pollFirst());
        }
        return Optional.empty();
    }

    private static void checkPriority(int priority) {
        if (priority < MIN_PRIORITY || priority > MAX_PRIORITY) {
            throw new IllegalArgumentException("priority must be 1-9, was " + priority);
        }
    }

    private static void checkWorkers(int busyWorkers, int totalWorkers) {
        if (busyWorkers < 0 || totalWorkers < 0) {
            throw new IllegalArgumentException("worker counts must be >= 0");
        }
    }

    private static final class TenantQueue<T> {
        private final TenantId tenant;
        private final ArrayDeque<T> items = new ArrayDeque<>();
        private int deficit;
        private boolean inTurn;

        private TenantQueue(TenantId tenant) {
            this.tenant = tenant;
        }
    }
}
