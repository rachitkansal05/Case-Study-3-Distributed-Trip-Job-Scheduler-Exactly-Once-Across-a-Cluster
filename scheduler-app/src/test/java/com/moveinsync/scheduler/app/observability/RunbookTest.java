package com.moveinsync.scheduler.app.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * {@code docs/RUNBOOK.md}: every PromQL block uses known metric names, metric-looking words in
 * the prose exist too, the mandatory sections are there, and no example contains personal data.
 */
class RunbookTest {

    private static final String RUNBOOK = OpsFiles.read("docs/RUNBOOK.md");

    /** Names of the shape of our metrics that are tables or columns, not metrics. */
    private static final Set<String> NOT_METRICS = Set.of("scheduler_node");

    @Test
    void everyPromQlBlockReferencesOnlyKnownMetrics() {
        Set<String> known = OpsFiles.allMetricNames();
        List<String> blocks = OpsFiles.fencedBlocks(RUNBOOK, "promql");
        assertThat(blocks).hasSizeGreaterThan(20);
        for (String block : blocks) {
            assertThat(OpsFiles.metricNamesIn(block)).as("metrics in %s", block).isNotEmpty().isSubsetOf(known);
        }
    }

    @Test
    void metricLookingWordsAnywhereAreKnownMetrics() {
        Set<String> known = OpsFiles.allMetricNames();
        Matcher m = Pattern.compile("\\b(?:scheduler|sink|hikaricp|jvm|cache)_[a-z0-9_]+\\b").matcher(RUNBOOK);
        while (m.find()) {
            String word = m.group();
            if (!NOT_METRICS.contains(word)) {
                assertThat(known).as("'%s' in RUNBOOK.md is a known metric", word).contains(word);
            }
        }
    }

    @Test
    void hasTheOnCallSections() {
        Set<String> anchors = OpsFiles.anchors(RUNBOOK);
        assertThat(anchors).contains("first-five-minutes", "key-sql-queries", "replaying-dead-letters");
        assertThat(OpsFiles.fencedBlocks(RUNBOOK, "sql")).isNotEmpty();
        assertThat(RUNBOOK).contains("FENCING_CONFLICT", "/api/v1/dead-letters", "clock_timestamp()");
    }

    @Test
    void containsNoPersonalData() {
        assertThat(PiiMasker.mask(RUNBOOK)).as("nothing in RUNBOOK.md looks like an email, phone or coordinate")
                .isEqualTo(RUNBOOK);
    }
}
