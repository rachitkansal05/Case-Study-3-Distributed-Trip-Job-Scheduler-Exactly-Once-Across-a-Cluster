package com.moveinsync.scheduler.loadgen;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class LagReportTest {

    @Test
    void percentilesOfOneToTenThousandMillis() {
        LagReport r = new LagReport();
        for (int ms = 1; ms <= 10_000; ms++) {
            r.recordMillis(ms);
        }
        assertThat(r.count()).isEqualTo(10_000);
        // 3 significant digits: each value within 0.1 %
        assertThat(r.percentileMillis(50)).isCloseTo(5_000, within(5.0));
        assertThat(r.percentileMillis(95)).isCloseTo(9_500, within(10.0));
        assertThat(r.percentileMillis(99)).isCloseTo(9_900, within(10.0));
        assertThat(r.percentileMillis(99.9)).isCloseTo(9_990, within(10.0));
        assertThat(r.maxMillis()).isCloseTo(10_000, within(10.0));
        assertThat(r.meanMillis()).isCloseTo(5_000.5, within(5.0));
    }

    @Test
    void subMillisecondResolutionNegativeClampAndEmpty() {
        LagReport r = new LagReport();
        assertThat(r.percentileMillis(99)).isZero();
        assertThat(r.maxMillis()).isZero();
        r.recordMicros(-5);
        r.recordMillis(0.25);
        assertThat(r.count()).isEqualTo(2);
        assertThat(r.percentileMillis(0)).isZero();
        assertThat(r.maxMillis()).isCloseTo(0.25, within(0.001));
    }

    @Test
    void addMergesAndRowsRender() {
        LagReport a = new LagReport();
        LagReport b = new LagReport();
        a.recordMillis(100);
        b.recordMillis(300);
        a.add(b);
        assertThat(a.count()).isEqualTo(2);
        MarkdownTable t = LagReport.table("runs");
        a.row(t, "all");
        new LagReport().row(t, "none");
        String s = t.toString();
        assertThat(s.lines().toList()).hasSize(4);
        assertThat(s).startsWith("| runs | count | p50 ms | p95 ms | p99 ms | p99.9 ms | max ms |");
        assertThat(s).contains("| all  | 2     | 100.0  | 300.0  | 300.0  | 300.0    | 300.0  |");
        assertThat(s).contains("| none | 0     | -");
    }

    @Test
    void runTablesSummaryPerMinuteAndBurst() {
        long t0 = 1_000_000_000_000L; // epoch us
        List<RunSample> s = new ArrayList<>();
        // minute 0: two runs, lag 100 and 300 ms, each takes 50 ms
        s.add(new RunSample(1, "a", 0, t0, t0 + 100_000, t0 + 150_000, "SUCCEEDED", 1, 0));
        s.add(new RunSample(2, "b", 1, t0 + 1_000_000, t0 + 1_300_000, t0 + 1_350_000, "SUCCEEDED", 2, 1));
        // minute 1: one dead letter, one never started
        s.add(new RunSample(3, "a", 2, t0 + 61_000_000, t0 + 61_000_000, t0 + 62_000_000, "DEAD_LETTERED", 5, 0));
        s.add(new RunSample(4, "a", 3, t0 + 62_000_000, RunSample.NONE, RunSample.NONE, "SCHEDULED", 0, 0));

        RunTables.Outcome o = RunTables.outcome(s);
        assertThat(o.runs()).isEqualTo(4);
        assertThat(o.succeeded()).isEqualTo(2);
        assertThat(o.deadLettered()).isEqualTo(1);
        assertThat(o.notTerminal()).isEqualTo(1);
        assertThat(o.started()).isEqualTo(3);
        assertThat(o.retried()).isEqualTo(2);
        assertThat(o.leaseLost()).isEqualTo(1);
        assertThat(o.busySeconds()).isCloseTo(61.9, within(1e-9));

        assertThat(RunTables.lag(s).count()).isEqualTo(3);
        assertThat(RunTables.lag(s, r -> r.tenant().equals("b")).percentileMillis(50)).isCloseTo(300, within(0.5));

        String minutes = RunTables.perMinute(s, t0).toString();
        assertThat(minutes).contains("| 0      | 2                    | 2      | 2           | 100.0      | 300.0");
        assertThat(minutes).contains("| 1      | 2                    | 1      | 0           | 0.0");
        assertThat(RunTables.summary(s, 60).toString()).contains("| runs with lease_lost_count > 0 ").contains("| 1 ");

        String burst = RunTables.burst(s, "a", "b", t0);
        assertThat(burst).contains("Tenant B (1 runs), every lag in ms, sorted: 300.0");
        assertThat(burst).contains("| drain time: max finished_at - T (s)");
    }
}
