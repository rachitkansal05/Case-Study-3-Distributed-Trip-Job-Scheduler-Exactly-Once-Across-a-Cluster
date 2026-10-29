package com.moveinsync.scheduler.core.fairness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.core.fairness.CapChangePlanner.Move;
import com.moveinsync.scheduler.core.fairness.CapChangePlanner.Plan;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class CapChangePlannerTest {

    private final CapChangePlanner planner = new CapChangePlanner();

    private static Set<Integer> range(int fromInclusive, int toExclusive) {
        return IntStream.range(fromInclusive, toExclusive).boxed().collect(Collectors.toSet());
    }

    /** Simulated slot table: slot number to run id, {@code null} when free. */
    private static final class SlotTable {
        private final TreeMap<Integer, Long> slots = new TreeMap<>();

        Set<Integer> existing() {
            return new TreeSet<>(slots.keySet());
        }

        Set<Integer> occupied() {
            Set<Integer> occupied = new TreeSet<>();
            slots.forEach((slot, run) -> {
                if (run != null) {
                    occupied.add(slot);
                }
            });
            return occupied;
        }

        int running() {
            return occupied().size();
        }

        boolean hasFree() {
            return slots.containsValue(null);
        }

        void apply(Plan plan) {
            for (int slot : plan.insert()) {
                assertThat(slots.containsKey(slot)).isFalse();
                slots.put(slot, null);
            }
            for (int slot : plan.deleteNow()) {
                assertThat(slots.get(slot)).as("never delete an occupied slot").isNull();
                assertThat(slots.containsKey(slot)).isTrue();
                slots.remove(slot);
            }
            for (Move move : plan.moves()) {
                Long run = slots.get(move.fromSlot());
                assertThat(run).isNotNull();
                assertThat(slots.containsKey(move.toSlot())).isTrue();
                assertThat(slots.get(move.toSlot())).isNull();
                slots.put(move.toSlot(), run);
                slots.remove(move.fromSlot());
            }
        }
    }

    @Test
    void raiseInsertsMissingSlots() {
        Plan plan = planner.plan(32, range(0, 16), range(0, 10));
        assertThat(plan.insert()).containsExactlyElementsOf(new TreeSet<>(range(16, 32)));
        assertThat(plan.deleteNow()).isEmpty();
        assertThat(plan.moves()).isEmpty();
        assertThat(plan.effectiveCapBefore()).isEqualTo(16);
        assertThat(plan.effectiveCapAfter()).isEqualTo(32);
        assertThat(plan.draining()).isFalse();
    }

    @Test
    void holesBelowCapInsertedAndAboveCapIgnored() {
        Plan plan = planner.plan(4, Set.of(0, 2, 7), Set.of(7));
        assertThat(plan.insert()).containsExactly(1, 3);
        assertThat(plan.moves()).containsExactly(new Move(7, 0));
        assertThat(plan.deleteAfterMoves()).containsExactly(7);
        assertThat(plan.effectiveCapAfter()).isEqualTo(4);

        Plan idle = planner.plan(4, range(0, 4), Set.of());
        assertThat(idle.isEmpty()).isTrue();
        assertThat(idle.effectiveCapAfter()).isEqualTo(4);
    }

    @Test
    void planExampleLowering32To16With28Running() {
        Plan plan = planner.plan(16, range(0, 32), range(0, 28));
        assertThat(plan.insert()).isEmpty();
        assertThat(plan.deleteNow()).containsExactly(28, 29, 30, 31);
        assertThat(plan.moves()).isEmpty();
        assertThat(plan.effectiveCapBefore()).isEqualTo(32);
        assertThat(plan.effectiveCapAfter()).isEqualTo(28);
        assertThat(plan.draining()).isTrue();
    }

    @Test
    void movesHighestFirstIntoLowestFree() {
        Plan plan = planner.plan(4, range(0, 8), Set.of(1, 5, 6, 7));
        assertThat(plan.moves()).containsExactly(new Move(7, 0), new Move(6, 2), new Move(5, 3));
        assertThat(plan.deleteNow()).containsExactly(4);
        assertThat(plan.effectiveCapAfter()).isEqualTo(4);
        assertThat(plan.draining()).isFalse();

        Plan partial = planner.plan(2, range(0, 6), Set.of(0, 3, 4, 5));
        assertThat(partial.moves()).containsExactly(new Move(5, 1));
        assertThat(partial.deleteNow()).containsExactly(2);
        assertThat(partial.effectiveCapAfter()).isEqualTo(4);
        assertThat(partial.draining()).isTrue();
    }

    @Test
    void capZeroRemovesEverythingFree() {
        Plan plan = planner.plan(0, range(0, 3), Set.of(1));
        assertThat(plan.deleteNow()).containsExactly(0, 2);
        assertThat(plan.moves()).isEmpty();
        assertThat(plan.effectiveCapAfter()).isEqualTo(1);
        assertThat(plan.draining()).isTrue();
    }

    @Test
    void validation() {
        assertThatThrownBy(() -> planner.plan(-1, Set.of(), Set.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> planner.plan(2, Set.of(-1), Set.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> planner.plan(2, Set.of(0), Set.of(1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> planner.plan(2, null, Set.of())).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Move(1, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Move(-1, 0)).isInstanceOf(IllegalArgumentException.class);
        Plan plan = planner.plan(1, Set.of(), Set.of());
        assertThatThrownBy(() -> plan.insert().add(5)).isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     * 28 runs, cap lowered 32 to 16; slot 30 holds a run that never finishes. Runs finish over time,
     * new demand claims any free slot, and the compactor re-plans every tick.
     */
    @Test
    void reductionConvergesAsRunsFinish() {
        int cap = 16;
        long stuckRun = 30;
        SlotTable table = new SlotTable();
        for (int slot = 0; slot < 32; slot++) {
            boolean occupied = slot < 27 || slot == 30;
            table.slots.put(slot, occupied ? (long) slot : null);
        }
        assertThat(table.running()).isEqualTo(28);
        Random random = new Random(42);
        long nextRun = 1_000;
        int previousRows = table.slots.size();
        boolean stuckMovedLow = false;
        boolean newRunsStarted = false;

        for (int tick = 0; tick < 200; tick++) {
            // Some runs finish (never the stuck one).
            List<Integer> occupied = new ArrayList<>(table.occupied());
            int finishing = random.nextInt(3);
            for (int i = 0; i < finishing && !occupied.isEmpty(); i++) {
                int slot = occupied.remove(random.nextInt(occupied.size()));
                if (table.slots.get(slot) != stuckRun) {
                    table.slots.put(slot, null);
                }
            }

            Plan plan = planner.plan(cap, table.existing(), table.occupied());
            assertThat(plan.effectiveCapBefore()).isEqualTo(table.slots.size());
            table.apply(plan);
            int running = table.running();
            int rows = table.slots.size();

            assertThat(plan.effectiveCapAfter()).isEqualTo(rows);
            assertThat(running).isLessThanOrEqualTo(rows);
            assertThat(rows).isEqualTo(Math.max(cap, running));
            assertThat(rows).as("rows never increase during the reduction").isLessThanOrEqualTo(previousRows);
            assertThat(plan.draining()).isEqualTo(rows > cap);
            if (running >= cap) {
                assertThat(table.hasFree()).as("no new run can start while running >= cap").isFalse();
            }
            previousRows = rows;

            int stuckSlot = table.slots.entrySet().stream()
                    .filter(e -> e.getValue() != null && e.getValue() == stuckRun)
                    .findFirst().orElseThrow().getKey();
            if (stuckSlot < cap) {
                stuckMovedLow = true;
            }

            // New demand: a claim takes a free slot if there is one (only possible below cap).
            if (tick > 100 && table.hasFree() && random.nextBoolean()) {
                assertThat(running).isLessThan(cap);
                int free = table.slots.entrySet().stream()
                        .filter(e -> e.getValue() == null).findFirst().orElseThrow().getKey();
                assertThat(free).isLessThan(cap);
                table.slots.put(free, nextRun++);
                newRunsStarted = true;
            }
        }

        assertThat(stuckMovedLow).as("stuck run compacted into a low slot").isTrue();
        assertThat(newRunsStarted).isTrue();
        assertThat(table.slots.keySet()).containsExactlyElementsOf(new TreeSet<>(range(0, cap)));
        assertThat(table.slots.containsValue(stuckRun)).isTrue();
    }

    @Test
    void randomizedInvariants() {
        Random random = new Random(20261030L);
        for (int iteration = 0; iteration < 5_000; iteration++) {
            int cap = random.nextInt(41);
            Set<Integer> existing = new HashSet<>();
            Set<Integer> occupied = new HashSet<>();
            int span = random.nextInt(49);
            double density = random.nextDouble();
            double busy = random.nextDouble();
            for (int slot = 0; slot < span; slot++) {
                if (random.nextDouble() < density) {
                    existing.add(slot);
                    if (random.nextDouble() < busy) {
                        occupied.add(slot);
                    }
                }
            }
            Plan plan = planner.plan(cap, existing, occupied);
            String ctx = "cap=" + cap + " existing=" + new TreeSet<>(existing) + " occupied=" + new TreeSet<>(occupied);

            assertThat(plan.effectiveCapBefore()).as(ctx).isEqualTo(existing.size());
            assertThat(plan.insert()).as(ctx).allMatch(s -> s < cap && !existing.contains(s));
            assertThat(plan.deleteNow()).as(ctx).allMatch(s -> s >= cap && existing.contains(s) && !occupied.contains(s));
            Set<Integer> targets = new HashSet<>();
            for (Move move : plan.moves()) {
                assertThat(occupied).as(ctx).contains(move.fromSlot());
                assertThat(move.fromSlot()).as(ctx).isGreaterThanOrEqualTo(cap);
                assertThat(move.toSlot()).as(ctx).isLessThan(cap);
                assertThat(occupied).as(ctx).doesNotContain(move.toSlot());
                assertThat(targets.add(move.toSlot())).as(ctx + " distinct targets").isTrue();
            }

            SlotTable table = new SlotTable();
            existing.forEach(s -> table.slots.put(s, occupied.contains(s) ? (long) s : null));
            Set<Long> runsBefore = new HashSet<>(table.slots.values());
            runsBefore.remove(null);
            table.apply(plan);

            Set<Long> runsAfter = new HashSet<>(table.slots.values());
            runsAfter.remove(null);
            assertThat(runsAfter).as(ctx + " runs preserved").isEqualTo(runsBefore);
            Set<Integer> unmovedHigh = new TreeSet<>(occupied);
            unmovedHigh.removeIf(s -> s < cap);
            plan.moves().forEach(m -> unmovedHigh.remove(m.fromSlot()));
            Set<Integer> expectedSlots = new TreeSet<>(range(0, cap));
            expectedSlots.addAll(unmovedHigh);
            assertThat(table.slots.keySet()).as(ctx).isEqualTo(expectedSlots);
            assertThat(plan.effectiveCapAfter()).as(ctx).isEqualTo(table.slots.size());
            assertThat(plan.effectiveCapAfter()).as(ctx).isEqualTo(Math.max(cap, occupied.size()));
            assertThat(plan.draining()).as(ctx).isEqualTo(plan.effectiveCapAfter() > cap);
            if (occupied.size() >= cap) {
                assertThat(table.hasFree()).as(ctx).isFalse();
            }

            Plan again = planner.plan(cap, table.existing(), table.occupied());
            assertThat(again.isEmpty()).as(ctx + " converged plan is a no-op").isTrue();
        }
    }

    @Test
    void planCopiesInputs() {
        Set<Integer> existing = new HashSet<>(range(0, 4));
        Plan plan = planner.plan(2, existing, Set.of());
        existing.clear();
        assertThat(plan.deleteNow()).containsExactly(2, 3);
    }
}
