package com.moveinsync.scheduler.app.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

/** The public dev key is accepted only under the dev or test profile. No database needed. */
class JwtSecretGuardTest {

    private static MockEnvironment env(String... profiles) {
        MockEnvironment env = new MockEnvironment();
        env.setActiveProfiles(profiles);
        return env;
    }

    @Test
    void defaultSecretWithoutDevProfileRefusesToStart() {
        assertThatThrownBy(() -> JwtSecurityConfig.requireRealSecret(JwtSecurityConfig.DEV_ONLY_SECRET, env("json")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SCHEDULER_JWT_HMAC_SECRET");
        assertThatThrownBy(() -> JwtSecurityConfig.requireRealSecret(JwtSecurityConfig.DEV_ONLY_SECRET, env()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void defaultSecretIsAllowedUnderDevOrTest() {
        assertThatCode(() -> JwtSecurityConfig.requireRealSecret(JwtSecurityConfig.DEV_ONLY_SECRET, env("json", "dev")))
                .doesNotThrowAnyException();
        assertThatCode(() -> JwtSecurityConfig.requireRealSecret(JwtSecurityConfig.DEV_ONLY_SECRET, env("test")))
                .doesNotThrowAnyException();
    }

    @Test
    void guardKnowsTheFallbackInApplicationYml() throws Exception {
        try (var in = getClass().getResourceAsStream("/application.yml")) {
            assertThat(new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8))
                    .contains("${SCHEDULER_JWT_HMAC_SECRET:" + JwtSecurityConfig.DEV_ONLY_SECRET + "}");
        }
    }

    @Test
    void realSecretStartsInAnyProfile() {
        assertThatCode(() -> JwtSecurityConfig.requireRealSecret("a-real-secret-that-is-long-enough-0123456789", env()))
                .doesNotThrowAnyException();
    }
}
