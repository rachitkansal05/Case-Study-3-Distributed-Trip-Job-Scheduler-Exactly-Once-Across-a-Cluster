package com.moveinsync.scheduler.loadgen;

import org.HdrHistogram.Histogram;

/**
 * Fire-lag distribution ({@code started_at - scheduled_at}, DB clock) in an HdrHistogram with 3
 * significant digits. Values are recorded in microseconds and reported in milliseconds; negative
 * inputs (impossible for a correct claim, but clock arithmetic can round) are recorded as 0.
 */
public final class LagReport {

    /** Columns of {@link #row}: label, count, p50, p95, p99, p99.9, max (ms). */
    public static final String[] COLUMNS = {"count", "p50 ms", "p95 ms", "p99 ms", "p99.9 ms", "max ms"};

    private final Histogram histogram = new Histogram(3); // auto-resizing

    public void recordMicros(long micros) {
        histogram.recordValue(Math.max(0, micros));
    }

    public void recordMillis(double millis) {
        recordMicros(Math.round(millis * 1000.0));
    }

    public void add(LagReport other) {
        histogram.add(other.histogram);
    }

    public long count() {
        return histogram.getTotalCount();
    }

    /** Value at {@code percentile} (0..100) in ms; 0 when empty. */
    public double percentileMillis(double percentile) {
        return count() == 0 ? 0 : histogram.getValueAtPercentile(percentile) / 1000.0;
    }

    /** Maximum in ms, to 3 significant digits (HdrHistogram bucket resolution); 0 when empty. */
    public double maxMillis() {
        return count() == 0 ? 0 : histogram.getMaxValue() / 1000.0;
    }

    public double meanMillis() {
        return count() == 0 ? 0 : histogram.getMean() / 1000.0;
    }

    /** A table with a first column {@code labelColumn} followed by {@link #COLUMNS}. */
    public static MarkdownTable table(String labelColumn) {
        String[] h = new String[COLUMNS.length + 1];
        h[0] = labelColumn;
        System.arraycopy(COLUMNS, 0, h, 1, COLUMNS.length);
        return new MarkdownTable(h);
    }

    /** Appends this distribution as one row of a {@link #table}. */
    public MarkdownTable row(MarkdownTable table, String label) {
        if (count() == 0) {
            return table.row(label, 0, "-", "-", "-", "-", "-");
        }
        return table.row(label, count(), MarkdownTable.f1(percentileMillis(50)), MarkdownTable.f1(percentileMillis(95)),
                MarkdownTable.f1(percentileMillis(99)), MarkdownTable.f1(percentileMillis(99.9)),
                MarkdownTable.f1(maxMillis()));
    }
}
