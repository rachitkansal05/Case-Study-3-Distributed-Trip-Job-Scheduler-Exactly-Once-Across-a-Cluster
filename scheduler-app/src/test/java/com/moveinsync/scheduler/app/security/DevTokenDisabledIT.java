package com.moveinsync.scheduler.app.security;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.api.support.ApiIntegrationTest;
import org.junit.jupiter.api.Test;

/**
 * Outside the {@code dev} profile {@code POST /dev/token} does not exist and is not opened in the
 * security chain (PLAN.md section 13): an anonymous call is a plain 401 and mints nothing.
 */
class DevTokenDisabledIT extends ApiIntegrationTest {

    @Test
    void devTokenIsNotOpenOutsideDevProfile() {
        Resp r = post("/dev/token", null, """
                {"tenant":"%s","role":"OPERATOR","subject":"mallory"}""".formatted(uniqueTenantId("dev")));
        assertThat(r.status()).as(r.toString()).isEqualTo(401);
        assertThat(r.code()).isEqualTo("UNAUTHORIZED");
        assertThat(r.header("WWW-Authenticate")).isEqualTo("Bearer");
        assertThat(r.raw()).doesNotContain("\"token\"");
    }

    @Test
    void devTokenIsNotFoundEvenWithAValidToken() {
        Resp r = post("/dev/token", operatorToken(), """
                {"tenant":"%s","role":"OPERATOR"}""".formatted(uniqueTenantId("dev")));
        assertThat(r.status()).as(r.toString()).isEqualTo(404);
        assertThat(r.raw()).doesNotContain("\"token\"");
    }
}
