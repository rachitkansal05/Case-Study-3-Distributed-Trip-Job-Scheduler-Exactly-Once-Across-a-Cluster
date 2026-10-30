package com.moveinsync.scheduler.app.security;

import static com.moveinsync.scheduler.app.security.Role.ADMIN;
import static com.moveinsync.scheduler.app.security.Role.EMPLOYEE;
import static com.moveinsync.scheduler.app.security.Role.OPERATOR;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.config.SchedulerProperties;
import java.util.Arrays;
import java.util.List;
import org.springframework.boot.actuate.autoconfigure.security.servlet.EndpointRequest;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.info.InfoEndpoint;
import org.springframework.boot.actuate.metrics.export.prometheus.PrometheusScrapeEndpoint;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.security.oauth2.server.resource.web.authentication.BearerTokenAuthenticationFilter;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Stateless HS256 JWT resource server with the role matrix of docs/design/architecture.md section 13. Roles are
 * checked here, per method and path; tenant isolation is enforced below, by {@link TenantScope}
 * in every repository query.
 *
 * <p>Order of checks: no/invalid token → 401; wrong role → 403 (reveals nothing about data);
 * right role but another tenant's resource → 404.
 */
@Configuration
public class JwtSecurityConfig {

    static final String DEV_TOKEN_PATH = "/dev/token";

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http, ObjectMapper mapper, Environment env)
            throws Exception {
        ProblemSecurityHandlers handlers = new ProblemSecurityHandlers(mapper);
        boolean dev = env.acceptsProfiles(Profiles.of("dev"));
        http.csrf(csrf -> csrf.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .httpBasic(b -> b.disable())
                .formLogin(f -> f.disable())
                .logout(l -> l.disable())
                .requestCache(c -> c.disable())
                .authorizeHttpRequests(auth -> {
                    auth.requestMatchers(EndpointRequest.to(HealthEndpoint.class, InfoEndpoint.class,
                            PrometheusScrapeEndpoint.class)).permitAll();
                    // error dispatches must not turn a real status (e.g. 500) into 401
                    auth.requestMatchers("/error").permitAll();
                    if (dev) {
                        auth.requestMatchers(HttpMethod.POST, DEV_TOKEN_PATH).permitAll();
                    }
                    auth.requestMatchers(HttpMethod.POST, "/api/v1/tenants").hasRole(OPERATOR.name())
                            .requestMatchers(HttpMethod.POST, "/api/v1/tenants/*/pause", "/api/v1/tenants/*/resume")
                            .hasAnyRole(names(ADMIN, OPERATOR))
                            .requestMatchers(HttpMethod.PUT, "/api/v1/tenants/*/cap").hasAnyRole(names(ADMIN, OPERATOR))

                            .requestMatchers(HttpMethod.POST, "/api/v1/jobs").hasRole(ADMIN.name())
                            .requestMatchers(HttpMethod.GET, "/api/v1/jobs/*/next-fire-times")
                            .hasAnyRole(names(ADMIN, EMPLOYEE))
                            .requestMatchers(HttpMethod.GET, "/api/v1/jobs/*/runs").hasAnyRole(names(ADMIN, OPERATOR))
                            .requestMatchers(HttpMethod.POST, "/api/v1/jobs/*/trigger").hasAnyRole(names(ADMIN, OPERATOR))
                            .requestMatchers(HttpMethod.GET, "/api/v1/jobs/*").hasAnyRole(names(ADMIN, EMPLOYEE, OPERATOR))
                            .requestMatchers(HttpMethod.DELETE, "/api/v1/jobs/*").hasRole(ADMIN.name())

                            .requestMatchers(HttpMethod.GET, "/api/v1/dead-letters").hasAnyRole(names(ADMIN, OPERATOR))
                            .requestMatchers(HttpMethod.POST, "/api/v1/dead-letters/*/replay").hasRole(OPERATOR.name())

                            .requestMatchers(HttpMethod.GET, "/api/v1/cluster").hasRole(OPERATOR.name())
                            // anything else needs a valid token and then 404s/405s normally
                            .anyRequest().authenticated();
                })
                .exceptionHandling(e -> e
                        .authenticationEntryPoint(handlers.entryPoint())
                        .accessDeniedHandler(handlers.accessDeniedHandler()))
                .oauth2ResourceServer(o -> o
                        .authenticationEntryPoint(handlers.entryPoint())
                        .accessDeniedHandler(handlers.accessDeniedHandler())
                        .jwt(jwt -> jwt.jwtAuthenticationConverter(authenticationConverter())))
                .addFilterAfter(new TenantMdcFilter(), BearerTokenAuthenticationFilter.class);
        return http.build();
    }

    /** The public fallback key in application.yml; anyone can sign tokens with it. */
    static final String DEV_ONLY_SECRET = "dev-only-insecure-hmac-key-do-not-use-in-prod-0123456789";

    @Bean
    JwtDecoder jwtDecoder(SchedulerProperties properties, Environment env) {
        requireRealSecret(properties.security().jwtHmacSecret(), env);
        NimbusJwtDecoder decoder = NimbusJwtDecoder
                .withSecretKey(JwtTokenIssuer.secretKey(properties.security().jwtHmacSecret()))
                .macAlgorithm(MacAlgorithm.HS256)
                .build();
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(JwtValidators.createDefault(),
                JwtClaims.validator()));
        return decoder;
    }

    /**
     * Refuses to start with the public dev key unless the {@code dev} or {@code test} profile is
     * active: a node deployed without {@code SCHEDULER_JWT_HMAC_SECRET} would otherwise accept
     * OPERATOR tokens signed by anyone.
     */
    static void requireRealSecret(String secret, Environment env) {
        if (DEV_ONLY_SECRET.equals(secret) && !env.acceptsProfiles(Profiles.of("dev", "test"))) {
            throw new IllegalStateException("scheduler.security.jwt-hmac-secret is the public dev-only default; "
                    + "set SCHEDULER_JWT_HMAC_SECRET (at least 32 characters) or run with the dev profile");
        }
    }

    /** Authorities come from the {@code roles} claim, e.g. {@code ["ADMIN"]} → {@code ROLE_ADMIN}. */
    static Converter<Jwt, AbstractAuthenticationToken> authenticationConverter() {
        return jwt -> {
            List<SimpleGrantedAuthority> authorities = JwtClaims.parseRoles(jwt.getClaim(JwtClaims.ROLES))
                    .orElseThrow()
                    .stream()
                    .map(r -> new SimpleGrantedAuthority(r.authority()))
                    .toList();
            return new JwtAuthenticationToken(jwt, authorities, jwt.getSubject());
        };
    }

    private static String[] names(Role... roles) {
        return Arrays.stream(roles).map(Role::name).toArray(String[]::new);
    }
}
