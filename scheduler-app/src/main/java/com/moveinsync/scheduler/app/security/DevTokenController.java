package com.moveinsync.scheduler.app.security;

import com.moveinsync.scheduler.app.config.SchedulerProperties;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code POST /dev/token}: mints a demo JWT. Exists only in the {@code dev} profile; in any other
 * profile the bean is absent and the path is not open.
 */
@RestController
@Profile("dev")
public class DevTokenController {

    static final Duration TTL = Duration.ofHours(1);

    private final JwtTokenIssuer issuer;
    private final Clock clock;

    public DevTokenController(SchedulerProperties properties, Clock clock) {
        this.issuer = new JwtTokenIssuer(properties.security().jwtHmacSecret());
        this.clock = clock;
    }

    @PostMapping(JwtSecurityConfig.DEV_TOKEN_PATH)
    public TokenResponse mint(@Valid @RequestBody TokenRequest request) {
        Instant now = clock.instant();
        String subject = request.subject() == null ? "dev-" + request.role().name().toLowerCase() : request.subject();
        String token = issuer.issue(subject, request.tenant(), request.role(), now, TTL);
        return new TokenResponse(token, "Bearer", now.plus(TTL));
    }

    /**
     * @param tenant  tenant claim (for OPERATOR it only namespaces idempotency keys)
     * @param role    EMPLOYEE, ADMIN or OPERATOR
     * @param subject optional; defaults to {@code dev-<role>}
     */
    public record TokenRequest(
            @NotBlank @Size(max = 64) @Pattern(regexp = "\\S(.*\\S)?") String tenant,
            @NotNull Role role,
            @Size(min = 1, max = 128) String subject) {
    }

    public record TokenResponse(String token, String tokenType, Instant expiresAt) {
    }
}
