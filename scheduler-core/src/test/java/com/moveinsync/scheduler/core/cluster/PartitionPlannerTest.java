package com.moveinsync.scheduler.core.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.core.cluster.PartitionPlanner.Plan;
import com.moveinsync.scheduler.core.cluster.PartitionPlanner.Settings;
import com.moveinsync.scheduler.core.ports.PartitionLeaseStore.PartitionLease;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** Plan section 06 and issue 1 of section 21: partition loop decisions. */
class PartitionPlannerTest {

    private static final int P = PartitionHasher.PARTITIONS;
    private static final String A = "scheduler-1";
    private static final String B = "scheduler-2";
    private static final String C = "scheduler-3";
    private static final String D = "scheduler-4";
    private static final List<String> NODES = List.of(A, B, C);
    private static final Instant NOW = Instant.parse("2026-10-30T12:00:00Z");

    private final PartitionAssigner assigner = new PartitionAssigner();
    private final PartitionPlanner planner = new PartitionPlanner(Settings.of(Duration.ofSeconds(6), Duration.ofSeconds(2)));

    private static List<PartitionLease> unowned() {
        return IntStream.range(0, P).mapToObj(p -> new PartitionLease(p, null, 0, null)).toList();
    }

    /** Leases matching {@code assignment}, each valid until {@code NOW + remaining}. */
    private static List<PartitionLease> owned(Map<String, Set<Integer>> assignment, Duration remaining) {
        PartitionLease[] rows = new PartitionLease[P];
        assignment.forEach((node, ps) -> ps.forEach(p -> rows[p] = new PartitionLease(p, node, 5, NOW.plus(remaining))));
        return List.of(rows);
    }

    private static List<PartitionLease> replace(List<PartitionLease> leases, Set<Integer> ps, String owner, Instant until) {
        List<PartitionLease> out = new ArrayList<>(leases);
        for (int p : ps) {
            out.set(p, new PartitionLease(p, owner, out.get(p).epoch(), until));
        }
        return out;
    }

    private static Set<Integer> all() {
        return IntStream.range(0, P).boxed().collect(Collectors.toCollection(TreeSet::new));
    }

    @Test
    void settingsDeriveRenewThresholdAndValidate() {
        assertThat(Settings.of(Duration.ofSeconds(6), Duration.ofSeconds(2)).renewBefore()).isEqualTo(Duration.ofSeconds(4));
        assertThat(planner.settings().renewBefore()).isEqualTo(Duration.ofSeconds(4));
        assertThatThrownBy(() -> Settings.of(Duration.ofSeconds(2), Duration.ofSeconds(2)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Settings.of(Duration.ofSeconds(6), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Settings(Duration.ofSeconds(-1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Settings(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new PartitionPlanner(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void initialEmptyClusterSplitsByRendezvous() {
        Map<String, Set<Integer>> desired = assigner.assign(P, NODES);
        Set<Integer> union = new HashSet<>();
        for (String self : NODES) {
            Plan plan = planner.plan(unowned(), NODES, self, NOW);
            assertThat(plan.acquire()).isEqualTo(desired.get(self));
            assertThat(plan.renew()).isEmpty();
            assertThat(plan.release()).isEmpty();
            assertThat(plan.keep()).isEmpty();
            for (int p : plan.acquire()) {
                assertThat(union.add(p)).as("partition %d acquired by two nodes", p).isTrue();
            }
        }
        assertThat(union).isEqualTo(all());
    }

    @Test
    void nodeAlwaysConsidersItselfLive() {
        Plan plan = planner.plan(unowned(), List.of(), "solo", NOW);
        assertThat(plan.acquire()).isEqualTo(all());
        assertThat(planner.plan(unowned(), List.of(A, B), C, NOW).acquire())
                .isEqualTo(assigner.assign(P, NODES).get(C));
    }

    @Test
    void steadyStateRenewsOnlyWhenDue() {
        Map<String, Set<Integer>> desired = assigner.assign(P, NODES);
        Plan notDue = planner.plan(owned(desired, Duration.ofMillis(4001)), NODES, A, NOW);
        assertThat(notDue.keep()).isEqualTo(desired.get(A));
        assertThat(notDue.acquire()).isEmpty();
        assertThat(notDue.renew()).isEmpty();
        assertThat(notDue.release()).isEmpty();

        Plan dueAtThreshold = planner.plan(owned(desired, Duration.ofSeconds(4)), NODES, A, NOW);
        assertThat(dueAtThreshold.renew()).isEqualTo(desired.get(A));
        assertThat(dueAtThreshold.keep()).isEmpty();

        // leaseUntil == now is still valid (the SQL takes over only when lease_until < now).
        Plan expiringNow = planner.plan(owned(desired, Duration.ZERO), NODES, A, NOW);
        assertThat(expiringNow.renew()).isEqualTo(desired.get(A));
        assertThat(expiringNow.acquire()).isEmpty();
        assertThat(planner.plan(owned(desired, Duration.ZERO), NODES, B, NOW).acquire()).isEmpty();

        // Mixed deadlines on my rows.
        int first = desired.get(A).iterator().next();
        List<PartitionLease> mixed = replace(owned(desired, Duration.ofSeconds(5)), Set.of(first), A, NOW.plusSeconds(1));
        Plan plan = planner.plan(mixed, NODES, A, NOW);
        assertThat(plan.renew()).containsExactly(first);
        assertThat(plan.owned()).isEqualTo(desired.get(A));
    }

    @Test
    void joinReleasesToJoinerWhoWaitsForRelease() {
        Map<String, Set<Integer>> three = assigner.assign(P, NODES);
        List<String> four = List.of(A, B, C, D);
        Map<String, Set<Integer>> desired = assigner.assign(P, four);
        List<PartitionLease> leases = owned(three, Duration.ofSeconds(5));

        Set<Integer> released = new TreeSet<>();
        for (String self : NODES) {
            Plan plan = planner.plan(leases, four, self, NOW);
            Set<Integer> expectedRelease = new TreeSet<>(three.get(self));
            expectedRelease.retainAll(desired.get(D));
            assertThat(plan.release()).isEqualTo(expectedRelease);
            assertThat(plan.keep()).isEqualTo(desired.get(self));
            assertThat(plan.acquire()).isEmpty();
            assertThat(plan.renew()).isEmpty();
            released.addAll(plan.release());
        }
        assertThat(released).isEqualTo(desired.get(D));

        Plan joinerBefore = planner.plan(leases, four, D, NOW);
        assertThat(joinerBefore.acquire()).as("joiner waits while old owners hold valid leases").isEmpty();

        List<PartitionLease> afterRelease = replace(leases, released, null, null);
        assertThat(planner.plan(afterRelease, four, D, NOW).acquire()).isEqualTo(desired.get(D));
        for (String self : NODES) {
            Plan plan = planner.plan(afterRelease, four, self, NOW);
            assertThat(plan.acquire()).isEmpty();
            assertThat(plan.release()).isEmpty();
        }
    }

    @Test
    void joinerTakesExpiredLeaseOfOldOwner() {
        Map<String, Set<Integer>> three = assigner.assign(P, NODES);
        List<String> four = List.of(A, B, C, D);
        Map<String, Set<Integer>> desired = assigner.assign(P, four);
        List<PartitionLease> expired = owned(three, Duration.ofSeconds(-1));
        Set<Integer> acquired = planner.plan(expired, four, D, NOW).acquire();
        assertThat(acquired).containsAll(desired.get(D));
        // Every expired owner is treated as dead for its partition, so D also takes the rows whose
        // expired owner is still desired and D is next in line; that owner re-acquires in the same
        // tick and the database picks one winner (rule 4 vs rule 5 race, converges via release).
        Set<Integer> expected = new TreeSet<>();
        for (PartitionLease lease : expired) {
            List<String> candidates = new ArrayList<>(four);
            candidates.remove(lease.ownerNode());
            if (assigner.desiredOwner(lease.partitionId(), candidates).equals(D)) {
                expected.add(lease.partitionId());
            }
        }
        assertThat(acquired).isEqualTo(expected);
        for (String self : NODES) {
            Plan plan = planner.plan(expired, four, self, NOW);
            Set<Integer> ownStillDesired = new TreeSet<>(three.get(self));
            ownStillDesired.retainAll(desired.get(self));
            assertThat(plan.acquire()).containsAll(ownStillDesired);
            assertThat(plan.acquire()).doesNotContainAnyElementsOf(desired.get(D));
        }
    }

    @Test
    void crashedOwnerStillHeartbeatingLosesPartitionsImmediately() {
        Map<String, Set<Integer>> desired = assigner.assign(P, NODES);
        List<PartitionLease> leases = owned(desired, Duration.ofSeconds(5));
        leases = replace(leases, desired.get(B), B, NOW.minusMillis(1));

        Plan a = planner.plan(leases, NODES, A, NOW);
        Plan c = planner.plan(leases, NODES, C, NOW);
        assertThat(a.acquire()).doesNotContainAnyElementsOf(c.acquire());
        Set<Integer> taken = new TreeSet<>(a.acquire());
        taken.addAll(c.acquire());
        assertThat(taken).isEqualTo(desired.get(B));
        assertThat(a.acquire()).isEqualTo(assigner.assign(P, List.of(A, C)).get(A)
                .stream().filter(desired.get(B)::contains).collect(Collectors.toSet()));
        assertThat(a.keep()).isEqualTo(desired.get(A));
        assertThat(c.keep()).isEqualTo(desired.get(C));
        assertThat(a.release()).isEmpty();

        // If B is in fact alive, it re-acquires its own expired leases; the database picks one winner.
        assertThat(planner.plan(leases, NODES, B, NOW).acquire()).isEqualTo(desired.get(B));
    }

    @Test
    void ownerWithNullLeaseUntilIsExpired() {
        Map<String, Set<Integer>> desired = assigner.assign(P, NODES);
        List<PartitionLease> leases = replace(owned(desired, Duration.ofSeconds(5)), desired.get(B), B, null);
        Set<Integer> taken = new TreeSet<>(planner.plan(leases, NODES, A, NOW).acquire());
        taken.addAll(planner.plan(leases, NODES, C, NOW).acquire());
        assertThat(taken).isEqualTo(desired.get(B));
    }

    @Test
    void deadOwnerOutsideLiveSetIsReplacedByDesiredOwner() {
        List<String> two = List.of(A, C);
        Map<String, Set<Integer>> desired = assigner.assign(P, NODES);
        List<PartitionLease> leases = replace(owned(desired, Duration.ofSeconds(5)), desired.get(B), B, NOW.minusSeconds(3));
        Map<String, Set<Integer>> twoDesired = assigner.assign(P, two);
        Plan a = planner.plan(leases, two, A, NOW);
        Set<Integer> expected = new TreeSet<>(twoDesired.get(A));
        expected.retainAll(desired.get(B));
        assertThat(a.acquire()).isEqualTo(expected);
    }

    @Test
    void myOwnExpiredLeaseIsReacquiredIfStillDesired() {
        Map<String, Set<Integer>> desired = assigner.assign(P, NODES);
        int mine = desired.get(A).iterator().next();
        List<PartitionLease> leases = replace(owned(desired, Duration.ofSeconds(5)), Set.of(mine), A, NOW.minusSeconds(1));
        Plan plan = planner.plan(leases, NODES, A, NOW);
        assertThat(plan.acquire()).containsExactly(mine);
        assertThat(plan.renew()).isEmpty();
        assertThat(plan.owned()).isEqualTo(desired.get(A));
    }

    @Test
    void myOwnExpiredLeaseNotDesiredIsLeftAlone() {
        Map<String, Set<Integer>> desired = assigner.assign(P, NODES);
        int theirs = desired.get(B).iterator().next();
        List<PartitionLease> leases = replace(owned(desired, Duration.ofSeconds(5)), Set.of(theirs), A, NOW.minusSeconds(1));
        Plan plan = planner.plan(leases, NODES, A, NOW);
        assertThat(plan.acquire()).doesNotContain(theirs);
        assertThat(plan.release()).doesNotContain(theirs);
        assertThat(planner.plan(leases, NODES, B, NOW).acquire()).containsExactly(theirs);
    }

    @Test
    void anotherNodesValidLeaseIsRespectedEvenIfIAmDesired() {
        Map<String, Set<Integer>> desired = assigner.assign(P, NODES);
        int mine = desired.get(A).iterator().next();
        List<PartitionLease> leases = replace(owned(desired, Duration.ofSeconds(5)), Set.of(mine), C, NOW.plusSeconds(1));
        Plan plan = planner.plan(leases, NODES, A, NOW);
        assertThat(plan.acquire()).doesNotContain(mine);
        assertThat(plan.owned()).doesNotContain(mine);
        assertThat(planner.plan(leases, NODES, C, NOW).release()).containsExactly(mine);
    }

    @Test
    void nullOwnerIsAcquirableOnlyByDesiredOwner() {
        int p = 17;
        String owner = assigner.desiredOwner(p, NODES);
        // A NULL owner is unowned even if lease_until is (oddly) still set.
        List<PartitionLease> leases = List.of(new PartitionLease(p, null, 3, NOW.plusSeconds(5)));
        for (String self : NODES) {
            Plan plan = planner.plan(leases, NODES, self, NOW);
            assertThat(plan.acquire()).isEqualTo(self.equals(owner) ? Set.of(p) : Set.of());
        }
    }

    @Test
    void missingPartitionsAreIgnoredAndInputValidated() {
        List<PartitionLease> some = unowned().subList(0, 10);
        Plan plan = planner.plan(some, List.of(), "solo", NOW);
        assertThat(plan.acquire()).isEqualTo(IntStream.range(0, 10).boxed().collect(Collectors.toSet()));

        List<PartitionLease> dup = List.of(new PartitionLease(1, null, 0, null), new PartitionLease(1, null, 0, null));
        assertThatThrownBy(() -> planner.plan(dup, NODES, A, NOW)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> planner.plan(List.of(new PartitionLease(-1, null, 0, null)), NODES, A, NOW))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> planner.plan(unowned(), NODES, " ", NOW)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> planner.plan(unowned(), List.of("", A), A, NOW))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> planner.plan(unowned(), NODES, A, null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> planner.plan(null, NODES, A, NOW)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void planIsImmutableSortedAndDisjoint() {
        Plan plan = new Plan(Set.of(5, 1, 3), Set.of(), Set.of(9), Set.of(2));
        assertThat(plan.acquire()).containsExactly(1, 3, 5);
        assertThat(plan.owned()).containsExactly(1, 2, 3, 5);
        assertThatThrownBy(() -> plan.acquire().add(7)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> new Plan(Set.of(1), Set.of(1), Set.of(), Set.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Plan(null, Set.of(), Set.of(), Set.of())).isInstanceOf(NullPointerException.class);
    }
}
