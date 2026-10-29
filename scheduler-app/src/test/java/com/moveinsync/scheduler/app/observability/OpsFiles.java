package com.moveinsync.scheduler.app.observability;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Test access to the repository-level ops files ({@code ops/}, {@code docs/RUNBOOK.md},
 * {@code docker-compose.yml}) and a small PromQL scanner that extracts the metric names an
 * expression references.
 */
final class OpsFiles {

    /** Section of {@code ops/metric-names.txt} -> series names. */
    static final String SCHEDULER = "scheduler";
    static final String SINK = "sink";
    static final String PROMETHEUS = "prometheus";

    private static final Set<String> KEYWORDS = Set.of(
            "and", "or", "unless", "bool", "offset", "by", "without", "on", "ignoring", "group_left",
            "group_right", "inf", "nan", "atan2");

    private static final Pattern STRING = Pattern.compile("\"(?:[^\"\\\\]|\\\\.)*\"|'(?:[^'\\\\]|\\\\.)*'");
    private static final Pattern LABEL_MATCHERS = Pattern.compile("\\{[^{}]*}");
    private static final Pattern RANGE = Pattern.compile("\\[[^\\[\\]]*]");
    private static final Pattern GROUPING = Pattern.compile(
            "\\b(?:by|without|on|ignoring|group_left|group_right)\\s*\\([^()]*\\)");
    private static final Pattern VARIABLE = Pattern.compile("\\$\\{?[A-Za-z_][A-Za-z0-9_]*}?");
    private static final Pattern IDENT = Pattern.compile("(?<![\\w.])[A-Za-z_:][A-Za-z0-9_:]*");
    private static final Pattern LABEL_VALUES = Pattern.compile("label_values\\(\\s*([A-Za-z_:][A-Za-z0-9_:]*)");

    private OpsFiles() {
    }

    /** The repository root: the first ancestor of the working directory holding {@code ops/metric-names.txt}. */
    static Path root() {
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null) {
            if (Files.isRegularFile(dir.resolve("ops/metric-names.txt"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("ops/metric-names.txt not found above " + Path.of("").toAbsolutePath());
    }

    static String read(String relative) {
        try {
            return Files.readString(root().resolve(relative), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@code ops/metric-names.txt} parsed into sections. */
    static Map<String, Set<String>> metricNames() {
        Map<String, Set<String>> sections = new LinkedHashMap<>();
        Set<String> current = null;
        for (String raw : read("ops/metric-names.txt").split("\\R")) {
            String line = raw.strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            if (line.startsWith("[") && line.endsWith("]")) {
                current = sections.computeIfAbsent(line.substring(1, line.length() - 1), k -> new LinkedHashSet<>());
            } else if (current == null) {
                throw new IllegalStateException("metric name outside a section: " + line);
            } else {
                current.add(line);
            }
        }
        return sections;
    }

    /** Every name of every section. */
    static Set<String> allMetricNames() {
        Set<String> all = new LinkedHashSet<>();
        metricNames().values().forEach(all::addAll);
        return all;
    }

    /**
     * Metric names referenced by a PromQL expression (or a Grafana {@code label_values(...)}
     * variable query): identifiers that are not functions, keywords, label names in grouping
     * clauses, label matchers, ranges, string literals or dashboard variables.
     */
    static Set<String> metricNamesIn(String promql) {
        Set<String> names = new LinkedHashSet<>();
        Matcher lv = LABEL_VALUES.matcher(promql);
        if (lv.find()) {
            names.add(lv.group(1));
            return names;
        }
        String s = STRING.matcher(promql).replaceAll(" ");
        s = VARIABLE.matcher(s).replaceAll(" ");
        s = RANGE.matcher(s).replaceAll(" ");
        String previous;
        do {
            previous = s;
            s = LABEL_MATCHERS.matcher(s).replaceAll(" ");
        } while (!s.equals(previous));
        s = GROUPING.matcher(s).replaceAll(" ");
        Matcher m = IDENT.matcher(s);
        while (m.find()) {
            String ident = m.group();
            if (KEYWORDS.contains(ident.toLowerCase())) {
                continue;
            }
            int next = m.end();
            while (next < s.length() && Character.isWhitespace(s.charAt(next))) {
                next++;
            }
            if (next < s.length() && s.charAt(next) == '(') {
                continue; // function call
            }
            names.add(ident);
        }
        return names;
    }

    /** Contents of every fenced code block with the given info string (e.g. {@code promql}). */
    static List<String> fencedBlocks(String markdown, String language) {
        List<String> blocks = new ArrayList<>();
        Matcher m = Pattern.compile("(?m)^[ \\t]*```" + Pattern.quote(language) + "[ \\t]*\\R(.*?)^[ \\t]*```",
                Pattern.DOTALL).matcher(markdown);
        while (m.find()) {
            blocks.add(m.group(1));
        }
        return blocks;
    }

    /** GitHub-style heading anchors of a markdown document. */
    static Set<String> anchors(String markdown) {
        Set<String> anchors = new LinkedHashSet<>();
        Matcher m = Pattern.compile("(?m)^#{1,6}\\s+(.+?)\\s*$").matcher(markdown);
        while (m.find()) {
            anchors.add(m.group(1).toLowerCase().replaceAll("[^a-z0-9 _-]", "").replace(' ', '-'));
        }
        return anchors;
    }
}
