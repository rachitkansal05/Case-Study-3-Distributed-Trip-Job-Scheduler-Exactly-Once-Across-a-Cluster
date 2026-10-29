package com.moveinsync.scheduler.core.cluster;

import com.moveinsync.scheduler.core.ports.PartitionLeaseStore.PartitionLease;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Pure decision logic of the partition loop (plan section 06, issue 1 of section 21). The loop
 * runs every 1 s with a 6 s partition lease renewed every 2 s; each tick it reads all lease rows
 * and the live set, asks {@link #plan} what to do, and then calls {@code tryAcquireOrRenew} for
 * {@link Plan#acquire()} and {@link Plan#renew()} and {@code release} for {@link Plan#release()}.
 * The database decides races; this class only decides intent.
 *
 * <h2>Definitions</h2>
 *
 * <ul>
 *   <li><b>Live set</b> = {@code liveNodes} plus {@code selfId}: a node always considers itself
 *       live, even before its first heartbeat is visible.
 *   <li><b>Valid lease</b>: {@code ownerNode != null} and {@code leaseUntil != null} and {@code
 *       leaseUntil >= now}. This matches the acquire SQL, which takes over only when {@code
 *       lease_until < clock_timestamp()}. Otherwise the lease is <b>expired</b> (owner set,
 *       {@code leaseUntil} null or before {@code now}) or <b>unowned</b> ({@code ownerNode ==
 *       null}: released, or one of the 128 seeded rows never owned).
 *   <li><b>Desired owner</b>: {@link PartitionAssigner#desiredOwner} over the candidate set below.
 * </ul>
 *
 * <h2>Rules, per partition</h2>
 *
 * <ol>
 *   <li><b>Valid, owned by me.</b> Candidates = live set. If I am desired: {@link Plan#renew()} when
 *       {@code leaseUntil - now <= renewBefore}, else {@link Plan#keep()}. If another node is
 *       desired (it joined): {@link Plan#release()}, never renew.
 *   <li><b>Valid, owned by another node.</b> Nothing, even if I am desired: wait until it releases
 *       or its lease expires.
 *   <li><b>Unowned.</b> Candidates = live set; acquire if I am desired.
 *   <li><b>Expired, owned by me.</b> Candidates = live set; acquire (a re-acquire) if I am desired.
 *   <li><b>Expired, owned by another node X.</b> X is treated as dead <em>for this partition</em>
 *       at once, even if X still heartbeats: candidates = live set minus X. Acquire if I am the
 *       desired owner among them. Because removing a node that is not the maximum never changes
 *       the argmax, every node that sees the same rows and live set picks the same single taker,
 *       so takeover does not wait for the membership timeout and remains disjoint.
 * </ol>
 *
 * <p>Partitions absent from {@code leases} are ignored: the acquire statement is an {@code UPDATE}
 * and cannot create a row, and the table holds all 128 seeded rows anyway.
 *
 * <p><b>Races are intended.</b> If X is in fact alive and still desired, it re-acquires its own
 * expired lease in the same tick (rule 4) while the rule-5 taker tries too; the conditional
 * {@code UPDATE} lets exactly one win, and a winner that is not the desired owner releases the
 * partition on its next tick (rule 1).
 *
 * <p><b>Known effect.</b> If X's leases expire while its heartbeat is still fresh, the taker
 * acquires X's partition and, on the next tick, sees X as the desired owner of a partition it holds
 * validly and releases it (rule 1); the row then stays unowned until X renews it or leaves the live
 * set. With a 2 s heartbeat, 6 s liveness window and 6 s lease this window is about 2 s after a
 * crash. A node whose partition loop stalls while its heartbeat keeps running would leave its
 * partitions unowned this way, so the engine should stop heartbeating when its partition loop
 * stalls. Correctness never depends on partition ownership (plan section 06.5), only lag.
 *
 * <p>Stateless and thread-safe.
 */
public final class PartitionPlanner {

    /**
     * Planner settings.
     *
     * @param renewBefore renew a lease I own once at most this much of it remains ({@code
     *     leaseUntil - now <= renewBefore}); non-negative. With the default 6 s lease and 2 s renew
     *     interval this is 4 s (see {@link #of(Duration, Duration)}).
     */
    public record Settings(Duration renewBefore) {

        public Settings {
            Objects.requireNonNull(renewBefore, "renewBefore");
            if (renewBefore.isNegative()) {
                throw new IllegalArgumentException("renewBefore must be >= 0: " + renewBefore);
            }
        }

        /**
         * Settings for a lease of {@code leaseDuration} renewed every {@code renewInterval}:
         * {@code renewBefore = leaseDuration - renewInterval}.
         *
         * @throws IllegalArgumentException unless {@code 0 < renewInterval < leaseDuration}
         */
        public static Settings of(Duration leaseDuration, Duration renewInterval) {
            Objects.requireNonNull(leaseDuration, "leaseDuration");
            Objects.requireNonNull(renewInterval, "renewInterval");
            if (renewInterval.isNegative() || renewInterval.isZero()
                    || renewInterval.compareTo(leaseDuration) >= 0) {
                throw new IllegalArgumentException(
                        "need 0 < renewInterval < leaseDuration: " + renewInterval + ", " + leaseDuration);
            }
            return new Settings(leaseDuration.minus(renewInterval));
        }
    }

    /**
     * What this node should do this tick. Each set is unmodifiable, iterates in ascending order,
     * and the four sets are pairwise disjoint.
     *
     * @param acquire unowned or expired partitions for which I am the desired owner (including my
     *     own expired leases)
     * @param renew partitions I hold validly, still desire, and that are due for renewal
     * @param release partitions I hold validly but another live node is now desired for
     * @param keep partitions I hold validly, still desire, and that are not yet due
     */
    public record Plan(Set<Integer> acquire, Set<Integer> renew, Set<Integer> release, Set<Integer> keep) {

        public Plan {
            acquire = sorted(acquire, "acquire");
            renew = sorted(renew, "renew");
            release = sorted(release, "release");
            keep = sorted(keep, "keep");
            Set<Integer> seen = new HashSet<>();
            for (Set<Integer> set : List.of(acquire, renew, release, keep)) {
                for (Integer p : set) {
                    if (!seen.add(p)) {
                        throw new IllegalArgumentException("partition " + p + " appears in two sets");
                    }
                }
            }
        }

        /** Partitions this node will own after the plan succeeds: acquire, renew and keep. */
        public Set<Integer> owned() {
            TreeSet<Integer> owned = new TreeSet<>(acquire);
            owned.addAll(renew);
            owned.addAll(keep);
            return Collections.unmodifiableSortedSet(owned);
        }

        private static Set<Integer> sorted(Set<Integer> set, String name) {
            Objects.requireNonNull(set, name);
            return Collections.unmodifiableSortedSet(new TreeSet<>(set));
        }
    }

    private final Settings settings;
    private final PartitionAssigner assigner = new PartitionAssigner();

    /** Creates a planner with the given settings. */
    public PartitionPlanner(Settings settings) {
        this.settings = Objects.requireNonNull(settings, "settings");
    }

    /** The settings this planner uses. */
    public Settings settings() {
        return settings;
    }

    /**
     * Plans one tick of the partition loop by the rules in the class Javadoc.
     *
     * @param leases current lease rows (normally all 128), in any order
     * @param liveNodes ids of live nodes by DB time; {@code selfId} is added if missing
     * @param selfId this node's id
     * @param now current DB time
     * @throws IllegalArgumentException if a partition id is negative or repeated, or a node id is
     *     blank
     * @throws NullPointerException if an argument, a lease or a live node id is null
     */
    public Plan plan(List<PartitionLease> leases, Collection<String> liveNodes, String selfId, Instant now) {
        Objects.requireNonNull(leases, "leases");
        Objects.requireNonNull(liveNodes, "liveNodes");
        Objects.requireNonNull(selfId, "selfId");
        Objects.requireNonNull(now, "now");
        if (selfId.isBlank()) {
            throw new IllegalArgumentException("selfId must not be blank");
        }
        Set<String> live = new TreeSet<>();
        for (String node : liveNodes) {
            live.add(Objects.requireNonNull(node, "live node id"));
        }
        live.add(selfId);

        Set<Integer> seen = new HashSet<>();
        Set<Integer> acquire = new TreeSet<>();
        Set<Integer> renew = new TreeSet<>();
        Set<Integer> release = new TreeSet<>();
        Set<Integer> keep = new TreeSet<>();
        for (PartitionLease lease : leases) {
            Objects.requireNonNull(lease, "lease");
            int p = lease.partitionId();
            if (p < 0) {
                throw new IllegalArgumentException("partition id must be >= 0: " + p);
            }
            if (!seen.add(p)) {
                throw new IllegalArgumentException("duplicate lease row for partition " + p);
            }
            String owner = lease.ownerNode();
            boolean valid = owner != null && lease.leaseUntil() != null && !lease.leaseUntil().isBefore(now);
            if (valid) {
                if (!owner.equals(selfId)) {
                    continue;
                }
                if (!selfId.equals(assigner.desiredOwner(p, live))) {
                    release.add(p);
                } else if (Duration.between(now, lease.leaseUntil()).compareTo(settings.renewBefore()) <= 0) {
                    renew.add(p);
                } else {
                    keep.add(p);
                }
            } else {
                Collection<String> candidates = live;
                if (owner != null && !owner.equals(selfId) && live.contains(owner)) {
                    Set<String> withoutOwner = new TreeSet<>(live);
                    withoutOwner.remove(owner);
                    candidates = withoutOwner;
                }
                if (selfId.equals(assigner.desiredOwner(p, candidates))) {
                    acquire.add(p);
                }
            }
        }
        return new Plan(acquire, renew, release, keep);
    }
}
