package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.TenantId;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.CountAtBucket;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Metric gaps found by the Observability team: buckets, eager series, dead letters, key hash. */
class EngineMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final EngineMetrics metrics = new EngineMetrics(registry, "n1");

    @Test
    void takeoverHistogramHasExactBucketsForTheTakeoverBounds() {
        metrics.onOrphanTakeover(null, Duration.ofMillis(10_500));
        Timer t = registry.find("scheduler.orphan.takeover.time").timer();
        assertThat(t).isNotNull();
        double[] bounds = Arrays.stream(t.takeSnapshot().histogramCounts())
                .mapToDouble(b -> b.bucket(TimeUnit.SECONDS)).toArray();
        for (double expected : new double[] {5.0, 10.7, 11.0, 11.2, 30.0}) {
            assertThat(Arrays.stream(bounds).anyMatch(b -> Math.abs(b - expected) < 1e-6))
                    .as("bucket %s s", expected).isTrue();
        }
        CountAtBucket at10_7 = Arrays.stream(t.takeSnapshot().histogramCounts())
                .filter(b -> Math.abs(b.bucket(TimeUnit.SECONDS) - 10.7) < 1e-6).findFirst().orElseThrow();
        assertThat(at10_7.count()).isEqualTo(1.0);
    }

    @Test
    void fireLagIsExportedForEveryHandlerBeforeTheFirstRun() {
        for (HandlerType h : HandlerType.values()) {
            assertThat(registry.find("scheduler.fire.lag").tag("handler", h.name()).timer())
                    .as("fire lag for %s", h).isNotNull();
        }
    }

    @Test
    void deadLettersAreCountedByErrorCodeAndFencingConflictIsEager() {
        assertThat(registry.find("scheduler.dead.letters").tag("error_code", "FENCING_CONFLICT").counter())
                .isNotNull();
        metrics.onDeadLettered(7, "FENCING_CONFLICT");
        metrics.onDeadLettered(8, "INVALID_PAYLOAD");
        metrics.onDeadLettered(9, "INVALID_PAYLOAD");
        assertThat(metrics.deadLetterCount("FENCING_CONFLICT")).isEqualTo(1.0);
        assertThat(metrics.deadLetterCount("INVALID_PAYLOAD")).isEqualTo(2.0);
    }

    @Test
    void capRejectionsAreRegisteredPerKnownTenantOnTheCapRefresh() {
        metrics.tenantCaps(Map.of("acme", new int[] {32, 32}, "globex", new int[] {8, 8}));
        Counter acme = registry.find("scheduler.tenant.cap.rejections").tag("tenant", "acme").counter();
        assertThat(acme).isNotNull();
        assertThat(acme.count()).isZero();
        metrics.onCapRejected(TenantId.of("acme"));
        assertThat(acme.count()).isEqualTo(1.0);
        assertThat(registry.find("scheduler.tenant.cap.rejections").tag("tenant", "globex").counter()).isNotNull();
    }

    @Test
    void idemKeyHashIsTheSha256PrefixNeverTheKey() {
        String key = "3f2a7c1e-0000-4000-8000-000000000001:2026-10-30T08:00:00Z";
        String hash = RunExecutor.idemKeyHash(key);
        assertThat(hash).hasSize(16).matches("[0-9a-f]{16}").doesNotContain("2026");
        assertThat(RunExecutor.idemKeyHash(key)).isEqualTo(hash);
        // SHA-256("abc") = ba7816bf8f01cfea...
        assertThat(RunExecutor.idemKeyHash("abc")).isEqualTo("ba7816bf8f01cfea");
    }
}
