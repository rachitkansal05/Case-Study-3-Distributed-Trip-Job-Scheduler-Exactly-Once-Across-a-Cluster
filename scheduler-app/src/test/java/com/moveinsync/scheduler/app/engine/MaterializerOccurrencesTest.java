package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.core.time.CronSchedule;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Pure occurrence planning of {@link Materializer} (no database). */
class MaterializerOccurrencesTest {

    private static final ZoneId NY = ZoneId.of("America/New_York");
    private static final ZoneId UTC = ZoneId.of("UTC");

    private static Instant at(String s) {
        return Instant.parse(s);
    }

    @Test
    void springForwardGapFiresAtGapEnd() {
        Materializer.Plan p = Materializer.plan(CronSchedule.parse("30 2 * * *"), NY,
                at("2026-03-07T07:30:00Z"), at("2030-01-01T00:00:00Z"), 3);
        assertThat(p.occurrences()).containsExactly(
                at("2026-03-07T07:30:00Z"), at("2026-03-08T07:00:00Z"), at("2026-03-09T06:30:00Z"));
        assertThat(p.nextCursor()).isEqualTo(at("2026-03-10T06:30:00Z"));
    }

    @Test
    void fallBackOverlapFiresOnceAtEarlierOffset() {
        Materializer.Plan p = Materializer.plan(CronSchedule.parse("30 1 * * *"), NY,
                at("2026-10-31T05:30:00Z"), at("2030-01-01T00:00:00Z"), 3);
        assertThat(p.occurrences()).containsExactly(
                at("2026-10-31T05:30:00Z"), at("2026-11-01T05:30:00Z"), at("2026-11-02T06:30:00Z"));
    }

    @Test
    void limitIsInclusiveAndCursorIsFirstBeyond() {
        Materializer.Plan p = Materializer.plan(CronSchedule.parse("*/5 * * * *"), UTC,
                at("2026-01-01T00:00:00Z"), at("2026-01-01T00:15:00Z"), 100);
        assertThat(p.occurrences()).hasSize(4).last().isEqualTo(at("2026-01-01T00:15:00Z"));
        assertThat(p.nextCursor()).isEqualTo(at("2026-01-01T00:20:00Z"));
    }

    @Test
    void maxBoundsCatchUp() {
        Materializer.Plan p = Materializer.plan(CronSchedule.parse("0 * * * *"), UTC,
                at("2026-01-01T00:00:00Z"), at("2026-01-02T00:00:00Z"), 2);
        assertThat(p.occurrences()).containsExactly(at("2026-01-01T00:00:00Z"), at("2026-01-01T01:00:00Z"));
        assertThat(p.nextCursor()).isEqualTo(at("2026-01-01T02:00:00Z"));
    }

    @Test
    void cursorBeyondLimitIsKept() {
        Materializer.Plan p = Materializer.plan(CronSchedule.parse("0 * * * *"), UTC,
                at("2026-01-01T05:00:00Z"), at("2026-01-01T04:59:00Z"), 10);
        assertThat(p.occurrences()).isEmpty();
        assertThat(p.nextCursor()).isEqualTo(at("2026-01-01T05:00:00Z"));
    }

    @Test
    void idemKeyIsJobIdColonUtcIso() {
        UUID id = UUID.fromString("00000000-0000-0000-0000-000000000001");
        assertThat(Materializer.idemKey(id, at("2026-11-01T05:30:00Z")))
                .isEqualTo("00000000-0000-0000-0000-000000000001:2026-11-01T05:30:00Z");
        assertThat(List.of(Materializer.idemKey(id, at("2026-03-08T07:00:00Z"))))
                .containsExactly(id + ":2026-03-08T07:00:00Z");
    }
}
