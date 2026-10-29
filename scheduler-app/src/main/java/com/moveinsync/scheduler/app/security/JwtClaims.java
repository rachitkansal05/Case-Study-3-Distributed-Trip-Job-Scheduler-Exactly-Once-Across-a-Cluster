package com.moveinsync.scheduler.app.security;

import com.moveinsync.scheduler.core.domain.TenantId;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * The token format. HS256-signed JWT with:
 * <pre>
 * { "sub": "alice", "tenant": "acme", "roles": ["ADMIN"], "iat": ..., "exp": ... }
 * </pre>
 * {@code sub}, {@code tenant}, {@code roles} (non-empty, values from {@link Role}) and {@code exp}
 * are required; any other token is rejected with 401.
 */
public final class JwtClaims {

    public static final String SUBJECT = "sub";
    public static final String TENANT = "tenant";
    public static final String ROLES = "roles";

    private JwtClaims() {
    }

    /** Builds the caller from an already validated token. */
    public static Caller toCaller(Jwt jwt) {
        return new Caller(jwt.getSubject(), jwt.getClaimAsString(TENANT),
                parseRoles(jwt.getClaim(ROLES)).orElseThrow());
    }

    /** Parses the roles claim; empty when it is missing, empty or contains an unknown role. */
    static Optional<Set<Role>> parseRoles(Object claim) {
        if (!(claim instanceof Collection<?> values) || values.isEmpty()) {
            return Optional.empty();
        }
        Set<Role> roles = EnumSet.noneOf(Role.class);
        for (Object v : values) {
            Optional<Role> role = v instanceof String s ? Role.parse(s) : Optional.empty();
            if (role.isEmpty()) {
                return Optional.empty();
            }
            roles.add(role.get());
        }
        return Optional.of(roles);
    }

    /** Validator for the claims above (signature and timestamps are checked by the decoder). */
    static OAuth2TokenValidator<Jwt> validator() {
        return jwt -> {
            List<String> problems = new java.util.ArrayList<>();
            if (jwt.getSubject() == null || jwt.getSubject().isBlank()) {
                problems.add("sub");
            }
            if (!validTenant(jwt.getClaim(TENANT))) {
                problems.add("tenant");
            }
            if (parseRoles(jwt.getClaim(ROLES)).isEmpty()) {
                problems.add("roles");
            }
            if (jwt.getExpiresAt() == null) {
                problems.add("exp");
            }
            if (problems.isEmpty()) {
                return OAuth2TokenValidatorResult.success();
            }
            return OAuth2TokenValidatorResult.failure(
                    new OAuth2Error("invalid_token", "missing or invalid claims: " + problems, null));
        };
    }

    private static boolean validTenant(Object claim) {
        if (!(claim instanceof String s)) {
            return false;
        }
        try {
            TenantId.of(s);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
