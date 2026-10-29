package com.moveinsync.scheduler.app.api;

import static com.moveinsync.scheduler.app.security.Role.ADMIN;
import static com.moveinsync.scheduler.app.security.Role.EMPLOYEE;
import static com.moveinsync.scheduler.app.security.Role.OPERATOR;
import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.api.support.ApiIntegrationTest;
import com.moveinsync.scheduler.app.security.Role;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.nimbusds.jwt.SignedJWT;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The role matrix of PLAN.md section 13 (R14), endpoint by endpoint: allowed roles succeed (never
 * 401/403), every other role gets exactly 403 {@code FORBIDDEN}, a missing token 401
 * {@code UNAUTHORIZED} with {@code WWW-Authenticate: Bearer}; and any malformed, expired, unsigned
 * or wrongly signed token is a 401. Health stays open.
 */
class RoleMatrixIT extends ApiIntegrationTest {

    /** Fresh resources for one call: a tenant, an enabled job and a dead-lettered run of it. */
    record Fixture(String tenant, UUID job, long deadRun) {
    }

    record Call(String method, String path, String body) {
    }

    record Endpoint(String name, Set<Role> allowed, int okStatus, Function<Fixture, Call> call) {
        @Override
        public String toString() {
            return name;
        }
    }

    static final List<Endpoint> ENDPOINTS = List.of(
            new Endpoint("POST /tenants", EnumSet.of(OPERATOR), 201, f -> new Call("POST", "/api/v1/tenants",
                    "{\"id\":\"" + f.tenant() + "-new\",\"timeZone\":\"Asia/Kolkata\",\"cap\":1}")),
            new Endpoint("POST /tenants/{t}/pause", EnumSet.of(ADMIN, OPERATOR), 200,
                    f -> new Call("POST", "/api/v1/tenants/" + f.tenant() + "/pause", null)),
            new Endpoint("POST /tenants/{t}/resume", EnumSet.of(ADMIN, OPERATOR), 200,
                    f -> new Call("POST", "/api/v1/tenants/" + f.tenant() + "/resume", null)),
            new Endpoint("PUT /tenants/{t}/cap", EnumSet.of(ADMIN, OPERATOR), 200,
                    f -> new Call("PUT", "/api/v1/tenants/" + f.tenant() + "/cap", "{\"cap\":3}")),
            new Endpoint("POST /jobs", EnumSet.of(ADMIN), 201,
                    f -> new Call("POST", "/api/v1/jobs", oneOffBody(Instant.now().plusSeconds(3600)))),
            new Endpoint("GET /jobs/{id}", EnumSet.of(ADMIN, EMPLOYEE, OPERATOR), 200,
                    f -> new Call("GET", "/api/v1/jobs/" + f.job(), null)),
            new Endpoint("GET /jobs/{id}/next-fire-times", EnumSet.of(ADMIN, EMPLOYEE), 200,
                    f -> new Call("GET", "/api/v1/jobs/" + f.job() + "/next-fire-times?count=3", null)),
            new Endpoint("POST /jobs/{id}/trigger", EnumSet.of(ADMIN, OPERATOR), 202,
                    f -> new Call("POST", "/api/v1/jobs/" + f.job() + "/trigger", null)),
            new Endpoint("DELETE /jobs/{id}", EnumSet.of(ADMIN), 200,
                    f -> new Call("DELETE", "/api/v1/jobs/" + f.job(), null)),
            new Endpoint("GET /jobs/{id}/runs", EnumSet.of(ADMIN, OPERATOR), 200,
                    f -> new Call("GET", "/api/v1/jobs/" + f.job() + "/runs", null)),
            new Endpoint("GET /dead-letters?tenant=", EnumSet.of(ADMIN, OPERATOR), 200,
                    f -> new Call("GET", "/api/v1/dead-letters?tenant=" + f.tenant(), null)),
            new Endpoint("POST /dead-letters/{runId}/replay", EnumSet.of(OPERATOR), 200,
                    f -> new Call("POST", "/api/v1/dead-letters/" + f.deadRun() + "/replay", null)),
            new Endpoint("GET /cluster", EnumSet.of(OPERATOR), 200,
                    f -> new Call("GET", "/api/v1/cluster", null)));

    /** Every endpoint with every role, plus {@code null} for "no token". */
    static Stream<Arguments> matrix() {
        List<Arguments> out = new ArrayList<>();
        for (Endpoint e : ENDPOINTS) {
            for (Role r : Role.values()) {
                out.add(Arguments.of(e, r));
            }
            out.add(Arguments.of(e, null));
        }
        return out.stream();
    }

    @ParameterizedTest(name = "{0} as {1}")
    @MethodSource("matrix")
    void roleMatrix(Endpoint endpoint, Role role) {
        Fixture f = fixture();
        Resp r = call(endpoint, f, role == null ? null : tokenFor(f, role));

        if (role == null) {
            assertThat(r.status()).as(r.toString()).isEqualTo(401);
            assertThat(r.code()).isEqualTo("UNAUTHORIZED");
            assertThat(r.header("WWW-Authenticate")).isEqualTo("Bearer");
        } else if (endpoint.allowed().contains(role)) {
            assertThat(r.status()).as(r.toString()).isNotIn(401, 403).isEqualTo(endpoint.okStatus());
        } else {
            assertThat(r.status()).as(r.toString()).isEqualTo(403);
            assertThat(r.code()).isEqualTo("FORBIDDEN");
            assertThat(r.header("Content-Type")).startsWith("application/problem+json");
        }
    }

    @Test
    void wrongRoleIs403EvenForResourcesThatDoNotExist() {
        // role check comes before any lookup, so a 403 reveals nothing about existence
        String t = newTenant(1);
        Resp nft = get("/api/v1/jobs/" + UUID.randomUUID() + "/next-fire-times", operatorToken());
        assertThat(nft.status()).isEqualTo(403);
        Resp del = delete("/api/v1/jobs/" + UUID.randomUUID(), token(t, EMPLOYEE));
        assertThat(del.status()).isEqualTo(403);
        Resp replay = post("/api/v1/dead-letters/999999999/replay", token(t, ADMIN), null);
        assertThat(replay.status()).isEqualTo(403);
        Resp pause = post("/api/v1/tenants/" + uniqueTenantId("ghost") + "/pause", token(t, EMPLOYEE), null);
        assertThat(pause.status()).isEqualTo(403);
    }

    // ------------------------------------------------------------------ token validation

    static Stream<Arguments> badTokens() {
        return Stream.of(
                Arguments.of("expired", (Consumer<JWTClaimsSet.Builder>) b -> b
                        .issueTime(Date.from(Instant.now().minusSeconds(3600)))
                        .expirationTime(Date.from(Instant.now().minusSeconds(600))), false),
                Arguments.of("missing tenant claim", (Consumer<JWTClaimsSet.Builder>) b -> b.claim("tenant", null), false),
                Arguments.of("blank tenant claim", (Consumer<JWTClaimsSet.Builder>) b -> b.claim("tenant", " "), false),
                Arguments.of("unknown role", (Consumer<JWTClaimsSet.Builder>) b -> b.claim("roles", List.of("ROOT")), false),
                Arguments.of("known plus unknown role",
                        (Consumer<JWTClaimsSet.Builder>) b -> b.claim("roles", List.of("ADMIN", "ROOT")), false),
                Arguments.of("lower-case role",
                        (Consumer<JWTClaimsSet.Builder>) b -> b.claim("roles", List.of("admin")), false),
                Arguments.of("empty roles", (Consumer<JWTClaimsSet.Builder>) b -> b.claim("roles", List.of()), false),
                Arguments.of("roles not a list", (Consumer<JWTClaimsSet.Builder>) b -> b.claim("roles", "ADMIN"), false),
                Arguments.of("missing roles", (Consumer<JWTClaimsSet.Builder>) b -> b.claim("roles", null), false),
                Arguments.of("missing sub", (Consumer<JWTClaimsSet.Builder>) b -> b.subject(null), false),
                Arguments.of("missing exp", (Consumer<JWTClaimsSet.Builder>) b -> b.expirationTime(null), false),
                Arguments.of("wrong signature", (Consumer<JWTClaimsSet.Builder>) b -> { }, true));
    }

    @ParameterizedTest(name = "{0} -> 401")
    @MethodSource("badTokens")
    void invalidTokensAre401(String name, Consumer<JWTClaimsSet.Builder> mutate, boolean otherSecret)
            throws JOSEException {
        String t = newTenant(1);
        UUID job = createOneOffJob(t);
        JWTClaimsSet.Builder claims = validClaims(t);
        mutate.accept(claims);
        String secret = otherSecret ? "another-secret-that-is-long-enough-0123456789-abcdef" : props.security().jwtHmacSecret();
        String jwt = sign(claims.build(), secret);

        assertUnauthorized(get("/api/v1/jobs/" + job, jwt));
    }

    @Test
    void validHandBuiltTokenIsAccepted() throws JOSEException {
        // control for the cases above: the same builder without a mutation works
        String t = newTenant(1);
        UUID job = createOneOffJob(t);
        Resp r = get("/api/v1/jobs/" + job, sign(validClaims(t).build(), props.security().jwtHmacSecret()));
        assertThat(r.status()).as(r.toString()).isEqualTo(200);
    }

    @Test
    void unsignedAlgNoneTokenIs401() {
        String t = newTenant(1);
        UUID job = createOneOffJob(t);
        String plain = new PlainJWT(validClaims(t).build()).serialize();
        assertThat(plain).endsWith(".");
        assertUnauthorized(get("/api/v1/jobs/" + job, plain));
    }

    @Test
    void garbageAndTamperedTokensAre401() throws JOSEException {
        String t = newTenant(1);
        UUID job = createOneOffJob(t);
        assertUnauthorized(get("/api/v1/jobs/" + job, "not-a-jwt"));

        // valid EMPLOYEE token whose payload is swapped for an ADMIN one, signature kept
        String employee = sign(validClaims(t).claim("roles", List.of("EMPLOYEE")).build(),
                props.security().jwtHmacSecret());
        String admin = sign(validClaims(t).build(), props.security().jwtHmacSecret());
        String[] e = employee.split("\\.");
        String[] a = admin.split("\\.");
        assertUnauthorized(get("/api/v1/jobs/" + job, e[0] + "." + a[1] + "." + e[2]));
    }

    @Test
    void healthIsOpenWithoutToken() {
        Resp r = get("/actuator/health", null);
        assertThat(r.status()).isEqualTo(200);
        assertThat(r.raw()).contains("UP");
    }

    // ------------------------------------------------------------------ helpers

    private Fixture fixture() {
        String tenant = newTenant(2);
        UUID job = createOneOffJob(tenant);
        long run = insertRun(job, tenant, "SCHEDULED");
        deadLetter(run, tenant, "HANDLER_FAILED");
        return new Fixture(tenant, job, run);
    }

    private String tokenFor(Fixture f, Role role) {
        return role == OPERATOR ? operatorToken() : token(f.tenant(), role);
    }

    private Resp call(Endpoint endpoint, Fixture f, String token) {
        Call c = endpoint.call().apply(f);
        return send(c.method(), c.path(), token, c.body(), "Idempotency-Key", "rm-" + UUID.randomUUID());
    }

    private void assertUnauthorized(Resp r) {
        assertThat(r.status()).as(r.toString()).isEqualTo(401);
        assertThat(r.code()).isEqualTo("UNAUTHORIZED");
        assertThat(r.header("WWW-Authenticate")).isEqualTo("Bearer");
        assertThat(r.json().has("traceId")).isTrue();
    }

    private static JWTClaimsSet.Builder validClaims(String tenant) {
        Instant now = Instant.now();
        return new JWTClaimsSet.Builder()
                .subject("mallory")
                .claim("tenant", tenant)
                .claim("roles", List.of("ADMIN"))
                .issueTime(Date.from(now))
                .expirationTime(Date.from(now.plusSeconds(600)));
    }

    private static String sign(JWTClaimsSet claims, String secret) throws JOSEException {
        SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
        jwt.sign(new MACSigner(secret.getBytes(StandardCharsets.UTF_8)));
        return jwt.serialize();
    }
}
