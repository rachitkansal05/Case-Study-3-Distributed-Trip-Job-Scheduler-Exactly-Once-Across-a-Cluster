package com.moveinsync.scheduler.app.security;

import com.nimbusds.jose.jwk.source.ImmutableSecret;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;

/**
 * Mints HS256 tokens in the {@link JwtClaims} format. Used by the dev-only {@code /dev/token}
 * endpoint and by tests; production tokens come from the identity provider.
 */
public final class JwtTokenIssuer {

    private final NimbusJwtEncoder encoder;

    public JwtTokenIssuer(String hmacSecret) {
        this.encoder = new NimbusJwtEncoder(new ImmutableSecret<>(secretKey(hmacSecret)));
    }

    static SecretKey secretKey(String hmacSecret) {
        return new SecretKeySpec(Objects.requireNonNull(hmacSecret).getBytes(StandardCharsets.UTF_8), "HmacSHA256");
    }

    /** A signed token for one role, valid for {@code ttl} from {@code now}. */
    public String issue(String subject, String tenant, Role role, Instant now, Duration ttl) {
        JwtClaimsSet claims = JwtClaimsSet.builder()
                .subject(subject)
                .claim(JwtClaims.TENANT, tenant)
                .claim(JwtClaims.ROLES, List.of(role.name()))
                .issuedAt(now)
                .expiresAt(now.plus(ttl))
                .build();
        JwsHeader header = JwsHeader.with(MacAlgorithm.HS256).build();
        return encoder.encode(JwtEncoderParameters.from(header, claims)).getTokenValue();
    }
}
