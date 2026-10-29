package com.moveinsync.scheduler.loadgen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/** A GitHub-flavoured Markdown table; columns are padded so the raw text lines up too. */
public final class MarkdownTable {

    private final List<String> header;
    private final List<List<String>> rows = new ArrayList<>();

    public MarkdownTable(String... header) {
        this.header = List.of(header);
    }

    /** Adds a row; {@code null} cells print as empty, other objects with {@link String#valueOf}. */
    public MarkdownTable row(Object... cells) {
        if (cells.length != header.size()) {
            throw new IllegalArgumentException("expected " + header.size() + " cells, got " + Arrays.toString(cells));
        }
        List<String> r = new ArrayList<>(cells.length);
        for (Object c : cells) {
            r.add(c == null ? "" : String.valueOf(c).replace("|", "\\|"));
        }
        rows.add(r);
        return this;
    }

    public int size() {
        return rows.size();
    }

    @Override
    public String toString() {
        int[] width = new int[header.size()];
        for (int i = 0; i < width.length; i++) {
            width[i] = Math.max(3, header.get(i).length());
            for (List<String> r : rows) {
                width[i] = Math.max(width[i], r.get(i).length());
            }
        }
        StringBuilder sb = new StringBuilder();
        line(sb, header, width);
        sb.append('|');
        for (int w : width) {
            sb.append(' ').append("-".repeat(w)).append(" |");
        }
        sb.append('\n');
        for (List<String> r : rows) {
            line(sb, r, width);
        }
        return sb.toString();
    }

    private static void line(StringBuilder sb, List<String> cells, int[] width) {
        sb.append('|');
        for (int i = 0; i < cells.size(); i++) {
            String c = cells.get(i);
            sb.append(' ').append(c).append(" ".repeat(width[i] - c.length())).append(" |");
        }
        sb.append('\n');
    }

    /** {@code 1234.5} as {@code "1234.5"} (one decimal, locale independent). */
    public static String f1(double v) {
        return String.format(Locale.ROOT, "%.1f", v);
    }

    /** Zero decimals, locale independent. */
    public static String f0(double v) {
        return String.format(Locale.ROOT, "%.0f", v);
    }
}
