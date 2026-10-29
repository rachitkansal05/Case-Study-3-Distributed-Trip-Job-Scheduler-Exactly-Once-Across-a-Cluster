package com.moveinsync.scheduler.app.repository.claim;

import com.moveinsync.scheduler.core.fairness.CapChangePlanner;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import javax.sql.DataSource;

/**
 * Applies {@link CapChangePlanner} plans (plan section 10, "Changing a cap"): the sibling of
 * {@link JdbcClaimProtocol} that moves running runs between slots, following the same rules (I2
 * slot rows first, then the run row; I3 a run gets a slot only while its row lock is held). Every
 * step re-checks its premise under the locks and is skipped if it no longer holds; skipping is
 * safe because running never exceeds slot rows and the next tick plans again.
 *
 * <p>Slot inserts and free-slot deletes are normally done by the API when the cap changes; they are
 * applied here too (idempotently) so the table converges even if a cap change raced a claim.
 */
public final class JdbcSlotCompaction {

    /** Snapshot of one tenant whose slot rows differ from its configured cap. */
    public record TenantSlots(String tenantId, int configuredCap, Set<Integer> existing, Set<Integer> occupied) {
    }

    /** What one {@link #apply} did. */
    public record Applied(int inserted, int deleted, int moved, int skipped) {
    }

    /* Snapshot only (no locks): tenants whose slot rows are not exactly 0..cap-1. */
    private static final String NEEDING_WORK = """
            SELECT t.id, t.configured_cap,
                   coalesce((SELECT array_agg(s.slot_no) FROM tenant_slot s WHERE s.tenant_id = t.id), '{}') AS existing,
                   coalesce((SELECT array_agg(r.slot_no) FROM job_run r
                              WHERE r.tenant_id = t.id AND r.state = 'RUNNING'), '{}') AS occupied
              FROM tenant t
             WHERE (SELECT count(*) FROM tenant_slot s WHERE s.tenant_id = t.id) <> t.configured_cap
                OR EXISTS (SELECT 1 FROM tenant_slot s WHERE s.tenant_id = t.id AND s.slot_no >= t.configured_cap)
            """;

    /* Raise / holes: insert missing low slots, re-checked against the current cap. */
    private static final String INSERT_LOW = """
            INSERT INTO tenant_slot (tenant_id, slot_no)
            SELECT t.id, g FROM tenant t, unnest(?::int[]) AS g
             WHERE t.id = ? AND g < t.configured_cap
            ON CONFLICT DO NOTHING
            """;

    /* I2: lock the slot rows of a step first, without waiting (a claim or the API may hold them). */
    private static final String LOCK_SLOTS = """
            SELECT slot_no FROM tenant_slot WHERE tenant_id = ? AND slot_no = ANY (?::int[])
               FOR UPDATE SKIP LOCKED
            """;

    /* Delete a free high slot: still >= the cap and referenced by no run (FK RESTRICT agrees). */
    private static final String DELETE_FREE_HIGH = """
            DELETE FROM tenant_slot s
             USING tenant t
             WHERE s.tenant_id = ? AND s.slot_no = ? AND t.id = s.tenant_id AND s.slot_no >= t.configured_cap
               AND NOT EXISTS (SELECT 1 FROM job_run r WHERE r.tenant_id = s.tenant_id AND r.slot_no = s.slot_no)
            """;

    /* Premise of a move, re-read under both slot locks: to is a free low slot, from is high. */
    private static final String MOVE_PREMISE = """
            SELECT t.configured_cap,
                   EXISTS (SELECT 1 FROM job_run r WHERE r.tenant_id = t.id AND r.slot_no = ? AND r.state = 'RUNNING')
                       AS target_busy
              FROM tenant t WHERE t.id = ?
            """;

    /* I3: move the RUNNING holder of `from` into `to` (we hold both slot locks). Waits only on the
       run row, whose holder's statements (renew/checkpoint/commit) never wait on anything. Token,
       lease and checkpoint are untouched: the holder keeps running without noticing. */
    private static final String MOVE_RUN = """
            UPDATE job_run SET slot_no = ?
             WHERE tenant_id = ? AND slot_no = ? AND state = 'RUNNING'
            """;

    private final DataSource dataSource;
    private final CapChangePlanner planner = new CapChangePlanner();

    public JdbcSlotCompaction(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /** Tenants whose slot rows do not match their configured cap. */
    public List<TenantSlots> tenantsNeedingWork() {
        List<TenantSlots> out = new ArrayList<>();
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(NEEDING_WORK);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(new TenantSlots(rs.getString(1), rs.getInt(2), ints(rs.getArray(3)), ints(rs.getArray(4))));
            }
        } catch (SQLException e) {
            throw new ClaimStoreException("slot snapshot failed", e);
        }
        return out;
    }

    /** Plans and applies one tenant's steps in the planner's order (insert, delete, moves). */
    public Applied apply(TenantSlots snapshot) {
        CapChangePlanner.Plan plan = planner.plan(snapshot.configuredCap(), snapshot.existing(), snapshot.occupied());
        String tenant = snapshot.tenantId();
        int inserted = 0;
        int deleted = 0;
        int moved = 0;
        int skipped = 0;
        try (Connection c = dataSource.getConnection()) {
            if (!plan.insert().isEmpty()) {
                c.setAutoCommit(true);
                try (PreparedStatement ps = c.prepareStatement(INSERT_LOW)) {
                    ps.setArray(1, c.createArrayOf("int4", plan.insert().toArray()));
                    ps.setString(2, tenant);
                    inserted = ps.executeUpdate();
                }
            }
            for (int slot : plan.deleteNow()) {
                if (inTransaction(c, () -> lockSlots(c, tenant, slot) && deleteFreeHigh(c, tenant, slot))) {
                    deleted++;
                } else {
                    skipped++;
                }
            }
            for (CapChangePlanner.Move move : plan.moves()) {
                if (inTransaction(c, () -> move(c, tenant, move))) {
                    moved++;
                    deleted++;
                } else {
                    skipped++;
                }
            }
        } catch (SQLException e) {
            throw new ClaimStoreException("slot compaction failed", e);
        }
        return new Applied(inserted, deleted, moved, skipped);
    }

    private boolean move(Connection c, String tenant, CapChangePlanner.Move move) throws SQLException {
        if (!lockSlots(c, tenant, move.fromSlot(), move.toSlot())) {
            return false; // a claim (or the API) holds one of the slots: plan again next tick
        }
        int cap;
        boolean targetBusy;
        try (PreparedStatement ps = c.prepareStatement(MOVE_PREMISE)) {
            ps.setInt(1, move.toSlot());
            ps.setString(2, tenant);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return false;
                }
                cap = rs.getInt(1);
                targetBusy = rs.getBoolean(2);
            }
        }
        if (targetBusy || move.toSlot() >= cap || move.fromSlot() < cap) {
            return false;
        }
        try (PreparedStatement ps = c.prepareStatement(MOVE_RUN)) {
            ps.setInt(1, move.toSlot());
            ps.setString(2, tenant);
            ps.setInt(3, move.fromSlot());
            ps.executeUpdate(); // 0 rows: the holder finished meanwhile; the high slot is free anyway
        }
        return deleteFreeHigh(c, tenant, move.fromSlot());
    }

    private static boolean lockSlots(Connection c, String tenant, int... slots) throws SQLException {
        Integer[] boxed = java.util.Arrays.stream(slots).boxed().toArray(Integer[]::new);
        int locked = 0;
        try (PreparedStatement ps = c.prepareStatement(LOCK_SLOTS)) {
            ps.setString(1, tenant);
            ps.setArray(2, c.createArrayOf("int4", boxed));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    locked++;
                }
            }
        }
        return locked == slots.length;
    }

    private static boolean deleteFreeHigh(Connection c, String tenant, int slot) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(DELETE_FREE_HIGH)) {
            ps.setString(1, tenant);
            ps.setInt(2, slot);
            return ps.executeUpdate() == 1;
        }
    }

    @FunctionalInterface
    private interface Step {
        boolean run() throws SQLException;
    }

    /** Runs a step in its own transaction; commits only if it reports success. */
    private static boolean inTransaction(Connection c, Step step) throws SQLException {
        c.setAutoCommit(false);
        try {
            boolean ok = step.run();
            if (ok) {
                c.commit();
            } else {
                c.rollback();
            }
            return ok;
        } catch (SQLException | RuntimeException e) {
            c.rollback();
            throw e;
        } finally {
            c.setAutoCommit(true);
        }
    }

    private static Set<Integer> ints(Array array) throws SQLException {
        Set<Integer> out = new TreeSet<>();
        if (array != null) {
            for (Object o : (Object[]) array.getArray()) {
                if (o != null) {
                    out.add(((Number) o).intValue());
                }
            }
        }
        return out;
    }
}
