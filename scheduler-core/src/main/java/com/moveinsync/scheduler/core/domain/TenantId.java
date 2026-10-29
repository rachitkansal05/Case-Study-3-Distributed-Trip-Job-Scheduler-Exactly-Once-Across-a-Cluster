package com.moveinsync.scheduler.core.domain;

import java.util.Objects;

/**
 * Tenant identifier (column {@code tenant.id}). In the API it comes only from the JWT claim, never
 * from request bodies (plan section 13).
 *
 * @param value non-blank, at most 64 characters, no surrounding whitespace
 */
public record TenantId(String value) {

    /** Maximum accepted length. */
    public static final int MAX_LENGTH = 64;

    public TenantId {
        Objects.requireNonNull(value, "tenant id");
        if (value.isBlank() || value.length() > MAX_LENGTH || !value.equals(value.strip())) {
            throw new IllegalArgumentException("tenant id must be 1-" + MAX_LENGTH
                    + " characters without surrounding whitespace");
        }
    }

    public static TenantId of(String value) {
        return new TenantId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
