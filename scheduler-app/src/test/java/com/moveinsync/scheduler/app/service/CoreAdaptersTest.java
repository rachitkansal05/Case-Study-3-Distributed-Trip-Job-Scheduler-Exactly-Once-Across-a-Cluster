package com.moveinsync.scheduler.app.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.app.error.ValidationException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class CoreAdaptersTest {

    private final CoreFireTimeService fireTimes = new CoreFireTimeService();

    @Test
    void springForwardGapFiresWhenTheGapEnds() {
        var times = fireTimes.nextFireTimes("30 2 * * *", ZoneId.of("America/New_York"),
                Instant.parse("2026-03-07T12:00:00Z"), 2);

        assertThat(times.get(0).instant()).isEqualTo(Instant.parse("2026-03-08T07:00:00Z"));
        assertThat(times.get(0).localTime()).isEqualTo(LocalDateTime.parse("2026-03-08T03:00"));
        assertThat(times.get(1).instant()).isEqualTo(Instant.parse("2026-03-09T06:30:00Z"));
    }

    @Test
    void fallBackOverlapFiresOnceAtTheFirstOccurrence() {
        var times = fireTimes.nextFireTimes("30 1 * * *", ZoneId.of("America/New_York"),
                Instant.parse("2026-10-31T12:00:00Z"), 2);

        assertThat(times.get(0).instant()).isEqualTo(Instant.parse("2026-11-01T05:30:00Z"));
        assertThat(times.get(1).instant()).isEqualTo(Instant.parse("2026-11-02T06:30:00Z"));
    }

    @Test
    void invalidCronBecomesValidationError() {
        assertThatThrownBy(() -> fireTimes.validateCron("61 * * * *"))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void placementMatchesCorePinnedVectors() {
        var placement = new CorePartitionPlacement(128);

        assertThat(placement.partitionFor(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"))).isEqualTo(34);
        assertThat(placement.partitionFor(UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479"))).isEqualTo(10);
    }
}
