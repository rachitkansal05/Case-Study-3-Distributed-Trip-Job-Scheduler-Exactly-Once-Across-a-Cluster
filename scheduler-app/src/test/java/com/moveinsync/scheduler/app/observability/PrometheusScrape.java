package com.moveinsync.scheduler.app.observability;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A parsed Prometheus text exposition: one {@link Sample} per series line. */
record PrometheusScrape(List<Sample> samples) {

    /** One series: name, labels, value. */
    record Sample(String name, Map<String, String> labels, double value) {
    }

    private static final Pattern LINE = Pattern.compile("^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\\{(.*)})?\\s+(\\S+).*$");
    private static final Pattern LABEL = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)=\"((?:[^\"\\\\]|\\\\.)*)\"");

    static PrometheusScrape parse(String text) {
        List<Sample> samples = new ArrayList<>();
        for (String line : text.split("\\R")) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            Matcher m = LINE.matcher(line);
            if (!m.matches()) {
                throw new IllegalArgumentException("not an exposition line: " + line);
            }
            Map<String, String> labels = new LinkedHashMap<>();
            if (m.group(2) != null) {
                Matcher l = LABEL.matcher(m.group(2));
                while (l.find()) {
                    labels.put(l.group(1), l.group(2));
                }
            }
            samples.add(new Sample(m.group(1), labels, parseValue(m.group(3))));
        }
        return new PrometheusScrape(samples);
    }

    Set<String> names() {
        Set<String> names = new LinkedHashSet<>();
        samples.forEach(s -> names.add(s.name()));
        return names;
    }

    List<Sample> series(String name) {
        return samples.stream().filter(s -> s.name().equals(name)).toList();
    }

    /** Values of {@code label} over every series of {@code name}. */
    Set<String> labelValues(String name, String label) {
        Set<String> values = new LinkedHashSet<>();
        series(name).forEach(s -> {
            if (s.labels().containsKey(label)) {
                values.add(s.labels().get(label));
            }
        });
        return values;
    }

    /** The single value of {@code name} whose labels include {@code match}, or NaN if absent. */
    double value(String name, Map<String, String> match) {
        return series(name).stream().filter(s -> s.labels().entrySet().containsAll(match.entrySet()))
                .mapToDouble(Sample::value).findFirst().orElse(Double.NaN);
    }

    private static double parseValue(String v) {
        return switch (v) {
            case "+Inf" -> Double.POSITIVE_INFINITY;
            case "-Inf" -> Double.NEGATIVE_INFINITY;
            case "NaN" -> Double.NaN;
            default -> Double.parseDouble(v);
        };
    }
}
