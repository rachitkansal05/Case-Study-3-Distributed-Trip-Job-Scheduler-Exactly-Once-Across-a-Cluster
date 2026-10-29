package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.app.repository.claim.JdbcRunQueries;
import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.TenantId;
import com.moveinsync.scheduler.core.ports.ClaimProtocol;
import com.moveinsync.scheduler.core.ports.SchedulerClock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Claim loop (plan section 07): every 200 ms, for each tenant with due runs in the owned
 * partitions, {@code claimDue} with the free worker count, then hand the committed claims to the
 * dispatcher. Claims are per tenant so one tenant's burst cannot fill the batch, and the tenant
 * order rotates each poll so no tenant always goes first. Nothing is claimed that cannot start.
 */
final class Poller extends PeriodicLoop {

    private final String nodeId;
    private final ClaimProtocol protocol;
    private final JdbcRunQueries queries;
    private final Supplier<Set<Integer>> owned;
    private final Dispatcher dispatcher;
    private final SchedulerClock clock;
    private int rotation;

    Poller(String nodeId, Duration interval, NodeGate gate, ClaimProtocol protocol, JdbcRunQueries queries,
           Supplier<Set<Integer>> owned, Dispatcher dispatcher, SchedulerClock clock) {
        super("poller", nodeId, interval, gate);
        this.nodeId = nodeId;
        this.protocol = Objects.requireNonNull(protocol, "protocol");
        this.queries = Objects.requireNonNull(queries, "queries");
        this.owned = Objects.requireNonNull(owned, "owned");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    protected void tick() {
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
            int free = dispatcher.freeForNormal();
            if (free <= 0) {
                return;
            }
            TenantId tenant = tenants.get((start + i) % tenants.size());
            long claimNanos = clock.monotonicNanos(); // before the claim statement: LeaseClock rule
            List<ClaimedRun> runs = protocol.claimDue(nodeId, tenant, partitions, free);
            if (!runs.isEmpty()) {
                dispatcher.submit(runs, claimNanos);
            }
        }
    }
}
