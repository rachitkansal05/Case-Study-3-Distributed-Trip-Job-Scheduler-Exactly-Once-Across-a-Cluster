package com.moveinsync.scheduler.core.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DomainTypesTest {

    @Test
    void tenantIdValidation() {
        assertThat(TenantId.of("acme").value()).isEqualTo("acme");
        assertThatThrownBy(() -> TenantId.of(" ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TenantId.of(" acme")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TenantId.of("x".repeat(65))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TenantId.of(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void runStateClassification() {
        assertThat(RunState.SCHEDULED.isClaimable()).isTrue();
        assertThat(RunState.RETRY_WAIT.isClaimable()).isTrue();
        assertThat(RunState.RUNNING.isClaimable()).isFalse();
        assertThat(RunState.SUCCEEDED.isTerminal()).isTrue();
        assertThat(RunState.RUNNING.isTerminal()).isFalse();
        assertThat(RunState.PAUSED.isTerminal()).isFalse();
    }

    @Test
    void itemKeyAppendsItemId() {
        RunContext ctx = new RunContext(7, UUID.randomUUID(), TenantId.of("acme"), 1, "job:2026-01-01T00:00:00Z",
                new FencingToken(1, 1), null, Instant.EPOCH);
        assertThat(ctx.itemKey("emp-42")).isEqualTo("job:2026-01-01T00:00:00Z:emp-42");
    }

    @Test
    void retryRejectsNegativeDelay() {
        assertThatThrownBy(() -> new RunFailure.Retry(Duration.ofSeconds(-1), "TIMEOUT", "timed out"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void deliveryOutcomeSuccess() {
        assertThat(DeliveryOutcome.DELIVERED.isSuccess()).isTrue();
        assertThat(DeliveryOutcome.ALREADY_DELIVERED.isSuccess()).isTrue();
        assertThat(DeliveryOutcome.STALE_TOKEN.isSuccess()).isFalse();
    }

    @Test
    void leaseLostMetricTagsAreDistinct() {
        assertThat(Arrays.stream(LeaseLostReason.values()).map(LeaseLostReason::metricTag).distinct())
                .hasSize(LeaseLostReason.values().length);
    }
}
