package com.moveinsync.scheduler.app.repository.tenant;

import com.moveinsync.scheduler.app.security.TenantScope;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * {@code tenant} and {@code tenant_slot}. Every method takes the caller's {@link TenantScope} and
 * restricts its SQL to it, so another tenant's row behaves as if it did not exist.
 *
 * <p>The pause flag and the cap are read from here on every request and never cached: the claim
 * reads {@code tenant.paused} directly and the cap <em>is</em> the slot rows (PLAN.md section 14).
 */
@Repository
public class TenantRepository {

    private final JdbcClient jdbc;

    public TenantRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Inserts a tenant; false when the id is taken (or outside the scope). */
    public boolean insert(TenantScope scope, String tenantId, String timeZone, int configuredCap) {
        if (!scope.includes(tenantId)) {
            return false;
        }
        return jdbc.sql("""
                        INSERT INTO tenant (id, time_zone, configured_cap) VALUES (:id, :zone, :cap)
                        ON CONFLICT (id) DO NOTHING""")
                .param("id", tenantId)
                .param("zone", timeZone)
                .param("cap", configuredCap)
                .update() == 1;
    }

    /** Time zone and pause flag; empty when the tenant does not exist in the scope. */
    public Optional<TenantRow> find(TenantScope scope, String tenantId) {
        return scope.bind(jdbc.sql("""
                        SELECT id, time_zone, paused, configured_cap,
                               (SELECT count(*) FROM tenant_slot s WHERE s.tenant_id = t.id) AS slots
                          FROM tenant t
                         WHERE t.id = :id AND """ + scope.predicate("t.id")))
                .param("id", tenantId)
                .query((rs, i) -> new TenantRow(rs.getString("id"), rs.getString("time_zone"),
                        rs.getBoolean("paused"), rs.getInt("configured_cap"), rs.getInt("slots")))
                .optional();
    }

    /** Sets {@code tenant.paused}; false when the tenant does not exist in the scope. */
    public boolean setPaused(TenantScope scope, String tenantId, boolean paused) {
        return scope.bind(jdbc.sql("UPDATE tenant SET paused = :paused WHERE id = :id AND " + scope.predicate("id")))
                .param("paused", paused)
                .param("id", tenantId)
                .update() == 1;
    }

    /**
     * Locks the tenant row for a cap change ({@code FOR UPDATE}), which serialises concurrent cap
     * changes of one tenant. Returns the current configured cap.
     */
    public Optional<Integer> lockForCapChange(TenantScope scope, String tenantId) {
        return scope.bind(jdbc.sql("SELECT configured_cap FROM tenant WHERE id = :id AND "
                        + scope.predicate("id") + " FOR UPDATE"))
                .param("id", tenantId)
                .query(Integer.class)
                .optional();
    }

    public void updateConfiguredCap(TenantScope scope, String tenantId, int cap) {
        scope.bind(jdbc.sql("UPDATE tenant SET configured_cap = :cap, version = version + 1 WHERE id = :id AND "
                        + scope.predicate("id")))
                .param("cap", cap)
                .param("id", tenantId)
                .update();
    }

    /** Inserts every missing slot row {@code 0 .. cap-1}; returns how many were added. */
    public int insertMissingSlots(TenantScope scope, String tenantId, int cap) {
        if (cap <= 0 || !scope.includes(tenantId)) {
            return 0;
        }
        return jdbc.sql("""
                        INSERT INTO tenant_slot (tenant_id, slot_no)
                        SELECT :id, s FROM generate_series(0, :cap - 1) AS s
                        ON CONFLICT (tenant_id, slot_no) DO NOTHING""")
                .param("id", tenantId)
                .param("cap", cap)
                .update();
    }

    /**
     * Deletes the <em>free</em> slot rows numbered {@code cap} or higher and returns how many.
     *
     * <p>Two statements on purpose. The first locks candidate rows, skipping any a claim holds
     * right now (claims lock the slot row before the run row). The second runs on a fresh
     * snapshot taken after those locks, so it sees every run that committed into a slot, and
     * deletes only rows no run references. With the rows locked no claim can take them in
     * between, so {@code ON DELETE RESTRICT} never fires; slots in use are left for the Engine's
     * compactor (PLAN.md section 10).
     */
    public int deleteFreeSlotsFrom(TenantScope scope, String tenantId, int cap) {
        List<Integer> locked = scope.bind(jdbc.sql(
                        "SELECT slot_no FROM tenant_slot WHERE tenant_id = :id AND slot_no >= :cap AND "
                                + scope.predicate("tenant_id") + " ORDER BY slot_no FOR UPDATE SKIP LOCKED"))
                .param("id", tenantId)
                .param("cap", cap)
                .query(Integer.class)
                .list();
        if (locked.isEmpty()) {
            return 0;
        }
        return jdbc.sql("""
                        DELETE FROM tenant_slot s
                         WHERE s.tenant_id = :id AND s.slot_no = ANY (:slots)
                           AND NOT EXISTS (SELECT 1 FROM job_run r
                                            WHERE r.tenant_id = s.tenant_id AND r.slot_no = s.slot_no)""")
                .param("id", tenantId)
                .param("slots", locked.stream().mapToInt(Integer::intValue).toArray())
                .update();
    }

    /** Number of slot rows: the effective cap. */
    public int countSlots(TenantScope scope, String tenantId) {
        return scope.bind(jdbc.sql("SELECT count(*) FROM tenant_slot WHERE tenant_id = :id AND "
                        + scope.predicate("tenant_id")))
                .param("id", tenantId)
                .query(Integer.class)
                .single();
    }

    /**
     * @param slots number of slot rows (effective cap)
     */
    public record TenantRow(String id, String timeZone, boolean paused, int configuredCap, int slots) {
    }
}
