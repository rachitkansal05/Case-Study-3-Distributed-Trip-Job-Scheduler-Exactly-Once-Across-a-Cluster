package com.moveinsync.scheduler.loadgen;

import static com.moveinsync.scheduler.loadgen.MarkdownTable.f1;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.function.Predicate;

/**
 * The result tables shared by the in-process benchmark and the compose-mode load generator. Every
 * time is DB clock (from {@code job_run}); lag is {@code started_at - scheduled_at}.
 */
public final class RunTables {

    private RunTables() {
    }

    /** Lag distribution of the started runs among {@code samples} that match {@code filter}. */
    public static LagReport lag(Collection<RunSample> samples, Predicate<RunSample> filter) {
        LagReport r = new LagReport();
        for (RunSample s : samples) {
            if (s.started() && filter.test(s)) {
                r.recordMicros(s.lagUs());
            }
        }
        return r;
    }

    public static LagReport lag(Collection<RunSample> samples) {
        return lag(samples, s -> true);
    }

    /** Counts of what happened to the runs. */
    public record Outcome(long runs, long succeeded, long deadLettered, long otherTerminal, long notTerminal,
                          long started, long retried, long leaseLost, long firstStartUs, long lastStartUs,
                          long lastFinishUs) {

        /** Seconds from the first start to the last finish. */
        public double busySeconds() {
            return firstStartUs == RunSample.NONE || lastFinishUs == RunSample.NONE ? 0
                    : (lastFinishUs - firstStartUs) / 1e6;
        }

        /** Completed (succeeded) per second between the first start and the last finish. */
        public double throughput() {
            double s = busySeconds();
            return s <= 0 ? 0 : succeeded / s;
        }
    }

    public static Outcome outcome(Collection<RunSample> samples) {
        long succeeded = 0;
        long dead = 0;
        long other = 0;
        long open = 0;
        long started = 0;
        long retried = 0;
        long leaseLost = 0;
        long firstStart = Long.MAX_VALUE;
        long lastStart = Long.MIN_VALUE;
        long lastFinish = Long.MIN_VALUE;
        for (RunSample s : samples) {
            switch (s.state()) {
                case "SUCCEEDED" -> succeeded++;
                case "DEAD_LETTERED" -> dead++;
                case "SKIPPED", "CANCELLED" -> other++;
                default -> open++;
            }
            if (s.started()) {
                started++;
                firstStart = Math.min(firstStart, s.startedUs());
                lastStart = Math.max(lastStart, s.startedUs());
            }
            if (s.finished()) {
                lastFinish = Math.max(lastFinish, s.finishedUs());
            }
            if (s.attempt() > 1) {
                retried++;
            }
            if (s.leaseLostCount() > 0) {
                leaseLost++;
            }
        }
        return new Outcome(samples.size(), succeeded, dead, other, open, started, retried, leaseLost,
                started == 0 ? RunSample.NONE : firstStart, started == 0 ? RunSample.NONE : lastStart,
                lastFinish == Long.MIN_VALUE ? RunSample.NONE : lastFinish);
    }

    /** Run counts and throughput; {@code windowSec} is the arrival window (completed / window too). */
    public static MarkdownTable summary(Collection<RunSample> samples, double windowSec) {
        Outcome o = outcome(samples);
        MarkdownTable t = new MarkdownTable("metric", "value");
        t.row("runs", o.runs());
        t.row("succeeded", o.succeeded());
        t.row("dead-lettered", o.deadLettered());
        t.row("skipped or cancelled", o.otherTerminal());
        t.row("not terminal at the end", o.notTerminal());
        t.row("runs with attempt > 1", o.retried());
        t.row("runs with lease_lost_count > 0", o.leaseLost());
        t.row("first start to last finish (s)", f1(o.busySeconds()));
        t.row("throughput: succeeded / (max finished_at - min started_at) (runs/s)", f1(o.throughput()));
        if (windowSec > 0) {
            t.row("succeeded / arrival window (runs/s)", f1(o.succeeded() / windowSec));
        }
        return t;
    }

    /**
     * Per minute after {@code t0Us}: runs scheduled in that minute (arrivals), runs started and
     * finished in that minute, and the lag of the runs scheduled in that minute.
     */
    public static MarkdownTable perMinute(Collection<RunSample> samples, long t0Us) {
        TreeMap<Long, long[]> counts = new TreeMap<>();
        TreeMap<Long, LagReport> lags = new TreeMap<>();
        for (RunSample s : samples) {
            long m = Math.floorDiv(s.scheduledUs() - t0Us, 60_000_000L);
            counts.computeIfAbsent(m, k -> new long[3])[0]++;
            if (s.started()) {
                counts.computeIfAbsent(Math.floorDiv(s.startedUs() - t0Us, 60_000_000L), k -> new long[3])[1]++;
                lags.computeIfAbsent(m, k -> new LagReport()).recordMicros(s.lagUs());
            }
            if (s.finished() && "SUCCEEDED".equals(s.state())) {
                counts.computeIfAbsent(Math.floorDiv(s.finishedUs() - t0Us, 60_000_000L), k -> new long[3])[2]++;
            }
        }
        MarkdownTable t = new MarkdownTable("minute", "arrivals (scheduled)", "starts", "completions",
                "p50 lag ms", "p99 lag ms", "max lag ms");
        counts.forEach((m, c) -> {
            LagReport l = lags.get(m);
            boolean has = l != null && l.count() > 0;
            t.row(m, c[0], c[1], c[2], has ? f1(l.percentileMillis(50)) : "-", has ? f1(l.percentileMillis(99)) : "-",
                    has ? f1(l.maxMillis()) : "-");
        });
        return t;
    }

    /**
     * Burst + fairness: {@code a} floods, {@code b} is small, both due at {@code tUs}. Returns the
     * Markdown text (several tables).
     */
    public static String burst(Collection<RunSample> samples, String a, String b, long tUs) {
        List<RunSample> as = new ArrayList<>();
        List<RunSample> bs = new ArrayList<>();
        for (RunSample s : samples) {
            if (a.equals(s.tenant())) {
                as.add(s);
            } else if (b.equals(s.tenant())) {
                bs.add(s);
            }
        }
        StringBuilder sb = new StringBuilder();
        List<Double> bLags = new ArrayList<>();
        for (RunSample s : bs) {
            if (s.started()) {
                bLags.add(s.lagUs() / 1000.0);
            }
        }
        bLags.sort(Double::compare);
        sb.append("Tenant B (").append(bs.size()).append(" runs), every lag in ms, sorted: ");
        List<String> txt = new ArrayList<>();
        for (double v : bLags) {
            txt.add(f1(v));
        }
        sb.append(String.join(", ", txt)).append("\n\n");

        MarkdownTable lagT = LagReport.table("tenant");
        lag(as, s -> true).row(lagT, "A (" + a + ")");
        lag(bs, s -> true).row(lagT, "B (" + b + ")");
        sb.append(lagT).append('\n');

        Outcome oa = outcome(as);
        Outcome ob = outcome(bs);
        double drainS = oa.lastFinishUs() == RunSample.NONE ? 0 : (oa.lastFinishUs() - tUs) / 1e6;
        MarkdownTable d = new MarkdownTable("metric", "A", "B");
        d.row("runs", oa.runs(), ob.runs());
        d.row("succeeded", oa.succeeded(), ob.succeeded());
        d.row("not terminal at the end", oa.notTerminal(), ob.notTerminal());
        d.row("dead-lettered", oa.deadLettered(), ob.deadLettered());
        d.row("drain time: max finished_at - T (s)", f1(drainS),
                ob.lastFinishUs() == RunSample.NONE ? "-" : f1((ob.lastFinishUs() - tUs) / 1e6));
        d.row("drain rate: succeeded / drain time (runs/s)", drainS <= 0 ? "-" : f1(oa.succeeded() / drainS), "-");
        d.row("max lag ms", f1(lag(as).maxMillis()), f1(lag(bs).maxMillis()));
        sb.append(d);
        return sb.toString();
    }
}
