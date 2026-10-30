package com.moveinsync.scheduler.app.security;

import java.util.Optional;

/** Roles carried in the JWT {@code roles} claim (docs/design/architecture.md section 13). */
public enum Role {
    /** Reads schedules of their own tenant; never sees payloads. */
    EMPLOYEE,
    /** Manages one tenant: jobs, pause/resume, cap, its dead letters. Sees payloads. */
    ADMIN,
    /** Platform on-call: works across tenants on operational data only, never payloads. */
    OPERATOR;

    /** Spring Security authority, e.g. {@code ROLE_ADMIN}. */
    public String authority() {
        return "ROLE_" + name();
    }

    /** Parses a claim value; unknown values yield empty so the token can be rejected. */
    public static Optional<Role> parse(String value) {
        for (Role r : values()) {
            if (r.name().equals(value)) {
                return Optional.of(r);
            }
        }
        return Optional.empty();
    }
}
