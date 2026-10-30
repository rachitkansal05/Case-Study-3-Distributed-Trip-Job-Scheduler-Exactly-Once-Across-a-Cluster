package com.moveinsync.scheduler.app.security;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.api.support.ApiIntegrationTest;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

/**
 * {@code POST /dev/token} in the {@code dev} profile (docs/design/architecture.md section 13): open without a token,
 * mints a JWT that the resource server accepts with the requested tenant and role, and validates
 * its body like any other endpoint (field name in {@code errors}, R14).
 */
@ActiveProfiles({"test", "dev"})
class DevTokenIT extends ApiIntegrationTest {

    @Test
    void mintsATokenThatWorksForTheTenant() {
        String tenant = newTenant(1);
        UUID job = createOneOffJob(tenant);

        Resp minted = post("/dev/token", null, """
                {"tenant":"%s","role":"ADMIN","subject":"alice"}""".formatted(tenant));
        assertThat(minted.status()).as(minted.toString()).isEqualTo(200);
        assertThat(minted.json().get("tokenType").asText()).isEqualTo("Bearer");
        assertThat(minted.json().get("expiresAt").asText()).isNotBlank();
        String token = minted.json().get("token").asText();
        assertThat(token.split("\\.")).hasSize(3);

        Resp r = get("/api/v1/jobs/" + job, token);
        assertThat(r.status()).as(r.toString()).isEqualTo(200);
        assertThat(r.json().get("tenantId").asText()).isEqualTo(tenant);
        assertThat(r.json().get("payloadRedacted").asBoolean()).isFalse();
    }

    @Test
    void mintedTokenIsConfinedToItsTenant() {
        String tenant = newTenant(1);
        String other = newTenant(1);
        UUID otherJob = createOneOffJob(other);

        Resp minted = post("/dev/token", null, """
                {"tenant":"%s","role":"EMPLOYEE"}""".formatted(tenant));
        assertThat(minted.status()).isEqualTo(200);
        String token = minted.json().get("token").asText();

        assertThat(get("/api/v1/jobs/" + otherJob, token).status()).isEqualTo(404);
        assertThat(post("/api/v1/jobs", token, oneOffBody(java.time.Instant.now().plusSeconds(3600))).status())
                .isEqualTo(403);
    }

    @Test
    void missingRoleIs400NamingTheField() {
        Resp r = post("/dev/token", null, """
                {"tenant":"%s","subject":"alice"}""".formatted(uniqueTenantId("dev")));
        assertThat(r.status()).as(r.toString()).isEqualTo(400);
        assertThat(r.code()).isEqualTo("VALIDATION_FAILED");
        assertThat(r.json().get("errors").findValuesAsText("field")).contains("role");
    }

    @Test
    void unknownRoleIs400() {
        Resp r = post("/dev/token", null, """
                {"tenant":"%s","role":"ROOT"}""".formatted(uniqueTenantId("dev")));
        assertThat(r.status()).as(r.toString()).isEqualTo(400);
        assertThat(r.json().get("errors").findValuesAsText("field")).contains("role");
        assertThat(r.raw()).doesNotContain("ROOT");
    }
}
