package com.moveinsync.scheduler.app.security;

import com.moveinsync.scheduler.app.error.NotFoundException;
import java.util.Objects;
import org.springframework.jdbc.core.simple.JdbcClient.StatementSpec;

/**
 * The set of tenants a request may touch, resolved <b>only</b> from the JWT (never from a request
 * body). EMPLOYEE and ADMIN tokens get a single-tenant scope; OPERATOR tokens get all tenants.
 *
 * <p>Every repository method takes a scope and adds its {@link #predicate(String) predicate} to
 * the {@code WHERE} clause, so a row of another tenant is simply not found. Services turn "not
 * found" into a 404, which also covers other tenants' rows (PLAN.md section 13: 404, not 403).
 */
public final class TenantScope {

    private static final TenantScope ALL = new TenantScope(null);

    /** {@code null} means all tenants. */
    private final String tenantId;

    private TenantScope(String tenantId) {
        this.tenantId = tenantId;
    }

    /** Scope limited to one tenant. */
    public static TenantScope of(String tenantId) {
        return new TenantScope(Objects.requireNonNull(tenantId, "tenantId"));
    }

    /** Cross-tenant scope (OPERATOR only). */
    public static TenantScope allTenants() {
        return ALL;
    }

    public boolean isAllTenants() {
        return tenantId == null;
    }

    /** The single tenant of this scope; fails for {@link #allTenants()}. */
    public String tenantId() {
        if (tenantId == null) {
            throw new IllegalStateException("cross-tenant scope has no single tenant");
        }
        return tenantId;
    }

    /** True when {@code tenant} is inside this scope. */
    public boolean includes(String tenant) {
        return tenantId == null || tenantId.equals(tenant);
    }

    /**
     * Narrows this scope to one tenant named in a path or query parameter. A tenant outside the
     * scope is reported as not found, never as forbidden.
     */
    public TenantScope narrowTo(String tenant) {
        if (!includes(tenant)) {
            throw NotFoundException.tenant();
        }
        return of(tenant);
    }

    /**
     * SQL predicate restricting {@code column} to this scope; bind it with {@link #bind}. Example:
     * {@code "WHERE j.id = :id AND " + scope.predicate("j.tenant_id")}.
     */
    public String predicate(String column) {
        return "(:scopeAll OR " + column + " = :scopeTenant)";
    }

    /** Binds the parameters used by {@link #predicate(String)}. */
    public StatementSpec bind(StatementSpec spec) {
        return spec.param("scopeAll", tenantId == null).param("scopeTenant", tenantId == null ? "" : tenantId);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TenantScope other && Objects.equals(tenantId, other.tenantId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tenantId);
    }

    @Override
    public String toString() {
        return tenantId == null ? "TenantScope[*]" : "TenantScope[" + tenantId + "]";
    }
}
