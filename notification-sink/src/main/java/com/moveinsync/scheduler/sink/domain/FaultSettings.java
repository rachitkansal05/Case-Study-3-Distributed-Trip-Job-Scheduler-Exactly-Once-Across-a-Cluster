package com.moveinsync.scheduler.sink.domain;

/**
 * Injected faults for chaos and benchmark tests. Applied to {@code POST /api/v1/deliveries} only,
 * after request validation and before any database access.
 *
 * @param latencyMs fixed delay added to every delivery, {@code [0, 60000]}
 * @param latencyJitterMs extra uniformly random delay in {@code [0, latencyJitterMs]}, {@code [0, 60000]}
 * @param failureRate probability in {@code [0, 1]} that a delivery fails (after the delay)
 * @param failureStatus HTTP status of an injected failure: 500 or 503
 */
public record FaultSettings(long latencyMs, long latencyJitterMs, double failureRate, int failureStatus) {

    public static final long MAX_LATENCY_MS = 60_000;
    public static final int DEFAULT_FAILURE_STATUS = 503;

    /** No faults. */
    public static final FaultSettings NONE = new FaultSettings(0, 0, 0.0, DEFAULT_FAILURE_STATUS);

    public FaultSettings {
        if (latencyMs < 0 || latencyMs > MAX_LATENCY_MS) {
            throw new IllegalArgumentException("latencyMs out of range");
        }
        if (latencyJitterMs < 0 || latencyJitterMs > MAX_LATENCY_MS) {
            throw new IllegalArgumentException("latencyJitterMs out of range");
        }
        if (!(failureRate >= 0.0 && failureRate <= 1.0)) {
            throw new IllegalArgumentException("failureRate out of range");
        }
        if (failureStatus != 500 && failureStatus != 503) {
            throw new IllegalArgumentException("failureStatus must be 500 or 503");
        }
    }
}
