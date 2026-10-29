package com.moveinsync.scheduler.core.fairness;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Plans how one tenant's {@code tenant_slot} rows converge on its configured cap (plan section 10,
 * "Changing a cap"). Pure and stateless: the engine calls {@link #plan} on every compactor tick
 * with the current state and applies the result; as runs finish, successive plans shrink the slot
 * table until it holds exactly {@code configuredCap} rows.
 *
 * <h2>Rules</h2>
 *
 * <ul>
 *   <li><b>Low slots</b> are numbers in {@code [0, configuredCap)}, <b>high slots</b> are numbers
 *       {@code >= configuredCap}.
 *   <li><b>Raise / holes:</b> every missing low slot is inserted ({@link Plan#insert()}). Holes
 *       above the cap are left alone.
 *   <li><b>Delete now:</b> every free high slot is deleted ({@link Plan#deleteNow()}). Occupied
 *       slots are never deleted ({@code ON DELETE RESTRICT} also forbids it).
 *   <li><b>Compaction:</b> free low slots (existing ones plus the ones being inserted) receive runs
 *       from occupied high slots ({@link Plan#moves()}). High slots are taken highest number
 *       first, into the lowest free low slot first, so the plan is deterministic. After each move
 *       the emptied high slot is deleted.
 * </ul>
 *
 * <p>After the plan is applied the tenant has {@code configuredCap + unmovedHighOccupied} slot rows,
 * which equals {@code max(configuredCap, running)}: either every high run moved, or every low slot
 * is now occupied. So while {@code running >= configuredCap} no slot is free and no new run can
 * start, and the effective cap only goes down during a reduction.
 *
 * <h2>Applying a plan</h2>
 *
 * Apply in this order: {@code insert}, {@code deleteNow}, then each move (lock both slot rows, then
 * the run row, as a claim does; move the run; delete {@code fromSlot}). The plan is computed from a
 * snapshot; the executor must re-check each step under the row locks and skip a step whose
 * premise no longer holds (for example a claim took the free low slot first). Skipping is safe:
 * running stays at most the slot row count, and the next tick plans again.
 */
public final class CapChangePlanner {

    /** Creates a planner. It has no state; one instance can be shared. */
    public CapChangePlanner() {
    }

    /**
     * Moves the run in {@code fromSlot} (a high slot) into the free low slot {@code toSlot}; {@code
     * fromSlot} is deleted right after the move.
     *
     * @param fromSlot occupied slot {@code >= configuredCap}
     * @param toSlot free slot {@code < configuredCap}
     */
    public record Move(int fromSlot, int toSlot) {

        public Move {
            if (fromSlot < 0 || toSlot < 0) {
                throw new IllegalArgumentException("slot numbers must be >= 0");
            }
            if (toSlot >= fromSlot) {
                throw new IllegalArgumentException("a move must go to a lower slot");
            }
        }
    }

    /**
     * Steps that bring a tenant's slot rows towards its configured cap.
     *
     * @param configuredCap cap set by the admin
     * @param effectiveCapBefore slot rows before the plan
     * @param effectiveCapAfter slot rows after the plan is fully applied, {@code max(configuredCap,
     *     running)}
     * @param insert low slot numbers to insert, ascending
     * @param deleteNow free high slot numbers to delete, ascending
     * @param moves compaction moves in execution order; each {@code fromSlot} is deleted after its
     *     move
     */
    public record Plan(
            int configuredCap,
            int effectiveCapBefore,
            int effectiveCapAfter,
            SortedSet<Integer> insert,
            SortedSet<Integer> deleteNow,
            List<Move> moves) {

        public Plan {
            if (configuredCap < 0 || effectiveCapBefore < 0 || effectiveCapAfter < 0) {
                throw new IllegalArgumentException("caps must be >= 0");
            }
            insert = Collections.unmodifiableSortedSet(
                    new TreeSet<>(Objects.requireNonNull(insert, "insert")));
            deleteNow = Collections.unmodifiableSortedSet(
                    new TreeSet<>(Objects.requireNonNull(deleteNow, "deleteNow")));
            moves = List.copyOf(Objects.requireNonNull(moves, "moves"));
        }

        /** @return true while the tenant has more slot rows than its configured cap after this plan */
        public boolean draining() {
            return effectiveCapAfter > configuredCap;
        }

        /** @return high slots deleted after their run moved out, in move order */
        public List<Integer> deleteAfterMoves() {
            return moves.stream().map(Move::fromSlot).toList();
        }

        /** @return true if the plan changes nothing */
        public boolean isEmpty() {
            return insert.isEmpty() && deleteNow.isEmpty() && moves.isEmpty();
        }
    }

    /**
     * Computes the plan for the current state of one tenant.
     *
     * @param configuredCap configured cap, {@code >= 0}
     * @param existingSlots slot numbers that have {@code tenant_slot} rows, each {@code >= 0}
     * @param occupiedSlots slots held by a {@code RUNNING} run, a subset of {@code existingSlots}
     * @return the plan
     * @throws IllegalArgumentException on a negative cap or slot, or an occupied slot that does not
     *     exist
     */
    public Plan plan(int configuredCap, Set<Integer> existingSlots, Set<Integer> occupiedSlots) {
        Objects.requireNonNull(existingSlots, "existingSlots");
        Objects.requireNonNull(occupiedSlots, "occupiedSlots");
        if (configuredCap < 0) {
            throw new IllegalArgumentException("configuredCap must be >= 0");
        }
        TreeSet<Integer> existing = new TreeSet<>();
        for (Integer slot : existingSlots) {
            existing.add(checkSlot(slot));
        }
        TreeSet<Integer> occupied = new TreeSet<>();
        for (Integer slot : occupiedSlots) {
            if (!existing.contains(checkSlot(slot))) {
                throw new IllegalArgumentException("occupied slot " + slot + " does not exist");
            }
            occupied.add(slot);
        }

        TreeSet<Integer> insert = new TreeSet<>();
        TreeSet<Integer> freeLow = new TreeSet<>();
        for (int slot = 0; slot < configuredCap; slot++) {
            if (!existing.contains(slot)) {
                insert.add(slot);
                freeLow.add(slot);
            } else if (!occupied.contains(slot)) {
                freeLow.add(slot);
            }
        }

        TreeSet<Integer> deleteNow = new TreeSet<>();
        for (Integer slot : existing.tailSet(configuredCap, true)) {
            if (!occupied.contains(slot)) {
                deleteNow.add(slot);
            }
        }

        List<Move> moves = new ArrayList<>();
        Iterator<Integer> targets = freeLow.iterator();
        for (Integer from : occupied.tailSet(configuredCap, true).descendingSet()) {
            if (!targets.hasNext()) {
                break;
            }
            moves.add(new Move(from, targets.next()));
        }

        int after = existing.size() + insert.size() - deleteNow.size() - moves.size();
        return new Plan(configuredCap, existing.size(), after, insert, deleteNow, moves);
    }

    private static int checkSlot(Integer slot) {
        Objects.requireNonNull(slot, "slot");
        if (slot < 0) {
            throw new IllegalArgumentException("slot numbers must be >= 0, was " + slot);
        }
        return slot;
    }
}
