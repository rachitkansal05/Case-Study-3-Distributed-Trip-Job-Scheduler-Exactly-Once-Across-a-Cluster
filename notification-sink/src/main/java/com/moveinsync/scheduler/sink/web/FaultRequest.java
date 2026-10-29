package com.moveinsync.scheduler.sink.web;

import com.moveinsync.scheduler.sink.domain.FaultSettings;
import com.moveinsync.scheduler.sink.web.SinkProblemException.FieldViolation;
import java.util.ArrayList;
import java.util.List;

/**
 * Body of {@code PUT /admin/faults}. Every field is optional; a missing field means "no fault of
 * that kind" ({@code failureStatus} defaults to 503). The PUT replaces the whole setting.
 */
public record FaultRequest(Long latencyMs, Long latencyJitterMs, Double failureRate, Integer failureStatus) {

    public List<FieldViolation> violations() {
        List<FieldViolation> out = new ArrayList<>();
        if (latencyMs != null && (latencyMs < 0 || latencyMs > FaultSettings.MAX_LATENCY_MS)) {
            out.add(new FieldViolation("latencyMs", "must be between 0 and " + FaultSettings.MAX_LATENCY_MS));
        }
        if (latencyJitterMs != null && (latencyJitterMs < 0 || latencyJitterMs > FaultSettings.MAX_LATENCY_MS)) {
            out.add(new FieldViolation("latencyJitterMs", "must be between 0 and " + FaultSettings.MAX_LATENCY_MS));
        }
        if (failureRate != null && !(failureRate >= 0.0 && failureRate <= 1.0)) {
            out.add(new FieldViolation("failureRate", "must be between 0 and 1"));
        }
        if (failureStatus != null && failureStatus != 500 && failureStatus != 503) {
            out.add(new FieldViolation("failureStatus", "must be 500 or 503"));
        }
        return out;
    }

    /** The settings; call only after {@link #violations()} returned empty. */
    public FaultSettings toSettings() {
        return new FaultSettings(
                latencyMs == null ? 0 : latencyMs,
                latencyJitterMs == null ? 0 : latencyJitterMs,
                failureRate == null ? 0.0 : failureRate,
                failureStatus == null ? FaultSettings.DEFAULT_FAILURE_STATUS : failureStatus);
    }
}
