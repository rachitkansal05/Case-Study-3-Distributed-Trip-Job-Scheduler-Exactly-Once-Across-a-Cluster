package com.moveinsync.scheduler.loadgen;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;

class LoadGenArgsTest {

    @Test
    void defaults() {
        LoadGenArgs a = LoadGenArgs.parse(new String[0], Map.of(), 36_000L * 1000);
        assertThat(a.apis()).containsExactly(URI.create("http://localhost:8081"), URI.create("http://localhost:8082"),
                URI.create("http://localhost:8083"));
        assertThat(a.jobs()).isEqualTo(100_000);
        assertThat(a.tenants()).isEqualTo(50);
        assertThat(a.cap()).isEqualTo(32);
        assertThat(a.windowSec()).isEqualTo(600);
        assertThat(a.leadSec()).isEqualTo(120);
        assertThat(a.scenario()).isEqualTo("spread");
        assertThat(a.jdbc()).isEqualTo("jdbc:postgresql://localhost:5432/scheduler?user=scheduler&password=scheduler");
        assertThat(a.concurrency()).isEqualTo(64);
        assertThat(a.prefix()).isEqualTo("lg" + Long.toString(36_000L, 36));
        assertThat(a.hmacSecret()).isNull();
        assertThat(a.totalJobs()).isEqualTo(100_000);
        assertThat(a.tenantIds()).hasSize(50).startsWith(a.prefix() + "-t00").endsWith(a.prefix() + "-t49");
    }

    @Test
    void bothOptionFormsAndAJdbcUrlWithAmpersands() {
        LoadGenArgs a = LoadGenArgs.parse(new String[] {"--api", "http://h1:8081/, http://h2:8082", "--jobs=1_000",
                "--tenants", "5", "--window", "60", "--lead=30", "--scenario", "BURST", "--burst-a", "500",
                "--burst-b=7", "--jdbc", "jdbc:postgresql://db:5432/s?user=u&password=p=x", "--prefix", "run1",
                "--hmac-secret", "s3cret", "--concurrency", "8", "--wait", "0", "--out", "x/y"}, Map.of(), 0);
        assertThat(a.apis()).containsExactly(URI.create("http://h1:8081"), URI.create("http://h2:8082"));
        assertThat(a.jobs()).isEqualTo(1000);
        assertThat(a.leadSec()).isEqualTo(30);
        assertThat(a.scenario()).isEqualTo("burst");
        assertThat(a.jdbc()).isEqualTo("jdbc:postgresql://db:5432/s?user=u&password=p=x");
        assertThat(a.totalJobs()).isEqualTo(507);
        assertThat(a.tenantIds()).containsExactly("run1-a", "run1-b");
        assertThat(a.hmacSecret()).isEqualTo("s3cret");
        assertThat(a.waitSec()).isZero();
        assertThat(a.outDir()).isEqualTo("x/y");
    }

    @Test
    void secretFallsBackToTheEnvironment() {
        assertThat(LoadGenArgs.parse(new String[0], Map.of("SCHEDULER_JWT_HMAC_SECRET", "env-secret"), 0).hmacSecret())
                .isEqualTo("env-secret");
    }

    @Test
    void rejectsBadInput() {
        assertThatThrownBy(() -> parse("--jobs")).hasMessageContaining("needs a value");
        assertThatThrownBy(() -> parse("--bogus", "1")).hasMessageContaining("unknown option --bogus");
        assertThatThrownBy(() -> parse("jobs")).hasMessageContaining("unexpected argument");
        assertThatThrownBy(() -> parse("--jobs", "ten")).hasMessageContaining("must be an integer");
        assertThatThrownBy(() -> parse("--jobs", "0")).hasMessageContaining("--jobs must be >= 1");
        assertThatThrownBy(() -> parse("--scenario", "chaos")).hasMessageContaining("spread or burst");
        assertThatThrownBy(() -> parse("--api", "localhost:8081")).hasMessageContaining("not an absolute URL");
        assertThatThrownBy(() -> parse("--prefix", "bad prefix")).hasMessageContaining("--prefix");
    }

    @Test
    void localTokenIsAValidHs256JwtInTheSchedulerClaimFormat() throws Exception {
        Instant now = Instant.parse("2026-10-30T10:00:00Z");
        String jwt = LoadGenMain.Hs256.token("loadgen-admin", "acme", "ADMIN", now, Duration.ofHours(1), "k3y");
        String[] parts = jwt.split("\\.");
        assertThat(parts).hasSize(3);
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec("k3y".getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        String sig = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(mac.doFinal((parts[0] + "." + parts[1]).getBytes(StandardCharsets.US_ASCII)));
        assertThat(parts[2]).isEqualTo(sig);
        ObjectMapper m = new ObjectMapper();
        assertThat(m.readTree(Base64.getUrlDecoder().decode(parts[0])).path("alg").asText()).isEqualTo("HS256");
        JsonNode claims = m.readTree(Base64.getUrlDecoder().decode(parts[1]));
        assertThat(claims.path("sub").asText()).isEqualTo("loadgen-admin");
        assertThat(claims.path("tenant").asText()).isEqualTo("acme");
        assertThat(List.of(claims.path("roles").get(0).asText())).containsExactly("ADMIN");
        assertThat(claims.path("iat").asLong()).isEqualTo(now.getEpochSecond());
        assertThat(claims.path("exp").asLong()).isEqualTo(now.getEpochSecond() + 3600);
    }

    private static LoadGenArgs parse(String... args) {
        return LoadGenArgs.parse(args, Map.of(), 0);
    }
}
