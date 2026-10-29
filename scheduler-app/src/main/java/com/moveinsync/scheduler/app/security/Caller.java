package com.moveinsync.scheduler.app.security;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * The authenticated caller, built from a validated JWT ({@code sub}, {@code tenant},
 * {@code roles}). Controllers receive it as a method argument (see
 * {@link CallerArgumentResolver}).
 *
 * @param subject  {@code sub} claim
 * @param tenantId {@code tenant} claim: the caller's home tenant. For EMPLOYEE/ADMIN it is the
 *                 only tenant they may touch; for OPERATOR it only namespaces idempotency keys
 * @param roles    non-empty
 */
public record Caller(String subject, String tenantId, Set<Role> roles) {

    public Caller {
        Objects.requireNonNull(subject, "subject");
        Objects.requireNonNull(tenantId, "tenantId");
        if (roles == null || roles.isEmpty()) {
            throw new IllegalArgumentException("roles must not be empty");
        }
        roles = Set.copyOf(EnumSet.copyOf(roles));
    }

    public boolean has(Role role) {
        return roles.contains(role);
    }

    public boolean isOperator() {
        return has(Role.OPERATOR);
    }

    /** OPERATOR works across tenants; every other role is confined to its own tenant. */
    public TenantScope scope() {
        return isOperator() ? TenantScope.allTenants() : TenantScope.of(tenantId);
    }

    /** Only a tenant ADMIN sees job payloads; EMPLOYEE and OPERATOR never do. */
    public boolean canSeePayload() {
        return has(Role.ADMIN) && !isOperator();
    }

    /**
     * Namespace of the caller's {@code Idempotency-Key}s in {@code api_idempotency.tenant_id}.
     * Tenant users share their tenant's namespace; operators get their own so that an operator
     * key never collides with a tenant admin's key.
     */
    public String idempotencyNamespace() {
        return isOperator() ? "operator:" + tenantId : tenantId;
    }
}
