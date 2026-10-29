package com.moveinsync.scheduler.core.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Plan section 16, R5: determinism, balance (max/min at most 1.3 at N=3), minimal movement. */
class PartitionAssignerTest {

    private static final int P = PartitionHasher.PARTITIONS;
    private static final List<String> COMPOSE = List.of("scheduler-1", "scheduler-2", "scheduler-3");

    private final PartitionAssigner assigner = new PartitionAssigner();

    @Test
    void deterministicRegardlessOfOrderDuplicatesOrInstance() {
        Map<String, Set<Integer>> a = assigner.assign(P, COMPOSE);
        Map<String, Set<Integer>> b = new PartitionAssigner()
                .assign(P, List.of("scheduler-3", "scheduler-1", "scheduler-2", "scheduler-1"));
        assertThat(b).isEqualTo(a);
        assertThat(a.keySet()).containsExactly("scheduler-1", "scheduler-2", "scheduler-3");
        for (int p = 0; p < P; p++) {
            String owner = assigner.desiredOwner(p, List.of("scheduler-2", "scheduler-3", "scheduler-1"));
            assertThat(a.get(owner)).contains(p);
        }
    }

    @Test
    void pinnedComposeAssignment() {
        Map<String, Set<Integer>> a = assigner.assign(P, COMPOSE);
        assertThat(a.get("scheduler-1")).hasSize(39);
        assertThat(a.get("scheduler-2")).hasSize(40);
        assertThat(a.get("scheduler-3")).hasSize(49);
    }

    @Test
    void everyPartitionAssignedExactlyOnce() {
        Random random = new Random(1);
        for (int trial = 0; trial < 50; trial++) {
            List<String> nodes = randomNodes(random, 1 + random.nextInt(10));
            Map<String, Set<Integer>> a = assigner.assign(P, nodes);
            assertThat(a.keySet()).containsExactlyInAnyOrderElementsOf(new HashSet<>(nodes));
            List<Integer> all = new ArrayList<>();
            a.values().forEach(all::addAll);
            Collections.sort(all);
            assertThat(all).hasSize(P);
            for (int p = 0; p < P; p++) {
                assertThat(all.get(p)).isEqualTo(p);
            }
        }
    }

    @Test
    void resultIsImmutableAndSorted() {
        Map<String, Set<Integer>> a = assigner.assign(P, List.of("b", "a", "c"));
        assertThat(a.keySet()).containsExactly("a", "b", "c");
        for (Set<Integer> set : a.values()) {
            assertThat(new ArrayList<>(set)).isSorted();
            assertThatThrownBy(() -> set.add(999)).isInstanceOf(UnsupportedOperationException.class);
        }
        assertThatThrownBy(() -> a.put("d", Set.of())).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void nodeWithoutPartitionsStillAppears() {
        Map<String, Set<Integer>> a = assigner.assign(1, List.of("a", "b", "c"));
        assertThat(a).hasSize(3);
        assertThat(a.values().stream().mapToInt(Set::size).sum()).isEqualTo(1);
        assertThat(a.values()).anyMatch(Set::isEmpty);
    }

    @Test
    void edgeCasesAndValidation() {
        assertThat(assigner.assign(P, List.of())).isEmpty();
        assertThat(assigner.assign(P, List.of("solo")).get("solo")).hasSize(P);
        assertThatThrownBy(() -> assigner.desiredOwner(0, List.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> assigner.desiredOwner(-1, COMPOSE)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> assigner.assign(0, COMPOSE)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> assigner.assign(P, List.of("a", " ")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> assigner.assign(P, Arrays.asList("a", null)))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> assigner.assign(P, null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> PartitionAssigner.weight("", 0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void weightIsStableAndPartitionSensitive() {
        assertThat(PartitionAssigner.weight("scheduler-1", 0)).isEqualTo(PartitionAssigner.weight("scheduler-1", 0));
        assertThat(PartitionAssigner.weight("scheduler-1", 0)).isNotEqualTo(PartitionAssigner.weight("scheduler-1", 1));
        assertThat(PartitionAssigner.weight("scheduler-1", 0)).isNotEqualTo(PartitionAssigner.weight("scheduler-2", 0));
        for (int p = 0; p < P; p++) {
            String best = null;
            for (String n : COMPOSE) {
                if (best == null
                        || Long.compareUnsigned(PartitionAssigner.weight(n, p), PartitionAssigner.weight(best, p)) > 0) {
                    best = n;
                }
            }
            assertThat(assigner.desiredOwner(p, COMPOSE)).isEqualTo(best);
        }
    }

    @Test
    void composeIdsBalanced() {
        assertThat(ratio(assigner.assign(P, COMPOSE))).isLessThanOrEqualTo(1.3);
    }

    /**
     * Pure rendezvous gives multinomial counts, so 1.3 cannot hold for every id set. Measured over
     * 10,000 random 3-node sets (seed 42): 55.7% satisfy max/min at most 1.3, mean 1.31, median
     * 1.27, p90 1.58, p99 1.90. The bounds below leave generous margin around those numbers.
     */
    @Test
    void randomThreeNodeSetsBalancedOnAverage() {
        Random random = new Random(42);
        int trials = 10_000;
        int within = 0;
        double sum = 0;
        for (int t = 0; t < trials; t++) {
            double r = ratio(assigner.assign(P, randomNodes(random, 3)));
            sum += r;
            if (r <= 1.3) {
                within++;
            }
        }
        assertThat(sum / trials).isLessThan(1.40);
        assertThat((double) within / trials).isGreaterThan(0.45);
    }

    @Test
    void removingComposeNodeMovesOnlyItsPartitions() {
        Map<String, Set<Integer>> before = assigner.assign(P, COMPOSE);
        Map<String, Set<Integer>> after = assigner.assign(P, List.of("scheduler-1", "scheduler-3"));
        assertLeaveMovesOnlyLeaver(before, after, "scheduler-2");
    }

    @Test
    void addingComposeNodeMovesOnlyToJoiner() {
        Map<String, Set<Integer>> before = assigner.assign(P, COMPOSE);
        List<String> four = new ArrayList<>(COMPOSE);
        four.add("scheduler-4");
        Map<String, Set<Integer>> after = assigner.assign(P, four);
        assertJoinMovesOnlyToJoiner(before, after, "scheduler-4");
        assertThat(after.get("scheduler-4").size()).isBetween(20, 44);
    }

    @Test
    void movementPropertiesHoldForManyRandomNodeSets() {
        Random random = new Random(2024);
        for (int trial = 0; trial < 200; trial++) {
            List<String> nodes = randomNodes(random, 2 + random.nextInt(9));
            Map<String, Set<Integer>> before = assigner.assign(P, nodes);

            String leaver = nodes.get(random.nextInt(nodes.size()));
            List<String> fewer = new ArrayList<>(nodes);
            fewer.remove(leaver);
            assertLeaveMovesOnlyLeaver(before, assigner.assign(P, fewer), leaver);

            String joiner = "joiner-" + trial;
            List<String> more = new ArrayList<>(nodes);
            more.add(joiner);
            assertJoinMovesOnlyToJoiner(before, assigner.assign(P, more), joiner);
        }
    }

    private static void assertLeaveMovesOnlyLeaver(
            Map<String, Set<Integer>> before, Map<String, Set<Integer>> after, String leaver) {
        assertThat(after).doesNotContainKey(leaver);
        for (Map.Entry<String, Set<Integer>> e : after.entrySet()) {
            Set<Integer> gained = new HashSet<>(e.getValue());
            gained.removeAll(before.get(e.getKey()));
            assertThat(e.getValue()).as("survivor keeps all its partitions").containsAll(before.get(e.getKey()));
            assertThat(before.get(leaver)).as("survivor gains only the leaver's partitions").containsAll(gained);
        }
    }

    private static void assertJoinMovesOnlyToJoiner(
            Map<String, Set<Integer>> before, Map<String, Set<Integer>> after, String joiner) {
        for (Map.Entry<String, Set<Integer>> e : before.entrySet()) {
            assertThat(before.get(e.getKey())).as("old node gains nothing").containsAll(after.get(e.getKey()));
            Set<Integer> lost = new HashSet<>(e.getValue());
            lost.removeAll(after.get(e.getKey()));
            assertThat(after.get(joiner)).as("lost partitions go to the joiner").containsAll(lost);
        }
    }

    private static double ratio(Map<String, Set<Integer>> assignment) {
        int max = assignment.values().stream().mapToInt(Set::size).max().orElseThrow();
        int min = assignment.values().stream().mapToInt(Set::size).min().orElseThrow();
        return min == 0 ? Double.POSITIVE_INFINITY : (double) max / min;
    }

    private static List<String> randomNodes(Random random, int n) {
        Set<String> ids = new HashSet<>();
        while (ids.size() < n) {
            ids.add("node-" + Long.toHexString(random.nextLong()));
        }
        return new ArrayList<>(ids);
    }
}
