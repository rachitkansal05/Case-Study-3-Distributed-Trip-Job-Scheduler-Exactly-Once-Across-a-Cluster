package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.app.repository.claim.JdbcRunQueries;
import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.TenantId;
import com.moveinsync.scheduler.core.ports.ClaimProtocol;
import com.moveinsync.scheduler.core.ports.SchedulerClock;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

/**
 * Claim loop (plan section 07). Two kinds of tick:
 *
 * <ul>
 *   <li><b>Full poll</b>, every {@code pollInterval} (200 ms): for each tenant with due runs in the
 *       owned partitions, {@code claimDue} with the free worker count, then hand the committed
 *       claims to the dispatcher. Claims are per tenant so one tenant's burst cannot fill the
 *       batch, and the tenant order rotates each poll so no tenant always goes first.
 *   <li><b>Slot-freed claim</b>, between full polls: when a run of tenant T finishes on this node
 *       (its commit freed T's slot), the dispatcher calls {@link #slotFreed} on the finishing worker
 *       thread, which claims for T alone right there. Without this a tenant at its cap drains at
 *       only {@code cap / pollInterval} (32 / 0.2 s = 160 runs/s) however short its runs are; with
 *       it the freed slot is refilled within one claim round trip. At most {@link
 *       #INLINE_CLAIMS} such claims run at once per node; above that, or while the node is frozen,
 *       the tenant is queued and the poll loop is woken to claim for it (coalesced).
 * </ul>
 *
 * <p>Slot-freed claims are bounded by completions on this node: each finished run causes at most
 * one claim for its tenant, and a claim that finds nothing due is not retried until the next
 * completion or full poll. When nothing runs, nothing finishes, so there is no busy loop. Nothing
 * is claimed that cannot start.
 */
final class Poller extends PeriodicLoop {

    private final String nodeId;
    private final ClaimProtocol protocol;
    private final JdbcRunQueries queries;
    private final Supplier<Set<Integer>> owned;
    private final Dispatcher dispatcher;
    private final SchedulerClock clock;
    private final long fullPollNanos;
    /** Concurrent slot-freed claims per node, on worker threads. */
    static final int INLINE_CLAIMS = 4;

    private final NodeGate gate;
    private final Semaphore inline = new Semaphore(INLINE_CLAIMS);
    private final Set<TenantId> freed = ConcurrentHashMap.newKeySet();
    private volatile long nextFullPoll = System.nanoTime();
    private int rotation;

    Poller(String nodeId, Duration interval, NodeGate gate, ClaimProtocol protocol, JdbcRunQueries queries,
           Supplier<Set<Integer>> owned, Dispatcher dispatcher, SchedulerClock clock) {
        super("poller", nodeId, interval, gate);
        this.nodeId = nodeId;
        this.gate = gate;
        this.protocol = Objects.requireNonNull(protocol, "protocol");
        this.queries = Objects.requireNonNull(queries, "queries");
        this.owned = Objects.requireNonNull(owned, "owned");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.fullPollNanos = interval.toNanos();
    }

    /** A run of {@code tenant} finished on this node and freed one of its slots: claim again now. */
    void slotFreed(TenantId tenant) {
        if (gate.isRunning() && inline.tryAcquire()) {
            try {
                Set<Integer> partitions = owned.get();
                if (!partitions.isEmpty()) {
                    claim(tenant, partitions);
                }
                return;
            } catch (RuntimeException e) {
                // DB trouble or a dying node: leave it to the poll loop, which logs and backs off
            } finally {
                inline.release();
            }
        }
        freed.add(tenant);
        wakeUp();
    }

    @Override
    protected void tick() {
        long now = System.nanoTime();
        if (now - nextFullPoll >= 0) {
            nextFullPoll = now + fullPollNanos;
            freed.clear(); // the full poll covers every tenant
            fullPoll();
        } else {
            claimFreed();
        }
    }

    @Override
    protected Duration nextDelay() {
        // a slot-freed tick must not push the next full poll back
        return Duration.ofNanos(Math.max(1_000_000L, nextFullPoll - System.nanoTime()));
    }

    private void fullPoll() {
        Set<Integer> partitions = owned.get();
        if (partitions.isEmpty() || dispatcher.freeForNormal() <= 0) {
            return;
        }
        List<TenantId> tenants = queries.tenantsWithDueRuns(partitions);
        if (tenants.isEmpty()) {
            return;
        }
        int start = Math.floorMod(rotation++, tenants.size());
        for (int i = 0; i < tenants.size(); i++) {
            if (!claim(tenants.get((start + i) % tenants.size()), partitions)) {
                return;
            }
        }
    }

    private void claimFreed() {
        if (freed.isEmpty()) {
            return;
        }
        Set<Integer> partitions = owned.get();
        Iterator<TenantId> it = freed.iterator();
        while (it.hasNext()) {
            TenantId tenant = it.next();
            it.remove();
            if (partitions.isEmpty() || !claim(tenant, partitions)) {
                return;
            }
        }
    }

    /** @return false when no worker is free (stop claiming this tick) */
    private boolean claim(TenantId tenant, Set<Integer> partitions) {
        int free = dispatcher.freeForNormal();
        if (free <= 0) {
            return false;
        }
        long claimNanos = clock.monotonicNanos(); // before the claim statement: LeaseClock rule
        List<ClaimedRun> runs = protocol.claimDue(nodeId, tenant, partitions, free);
        if (!runs.isEmpty()) {
            dispatcher.submit(runs, claimNanos);
        }
        return true;
    }
}
