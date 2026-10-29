package com.moveinsync.scheduler.sink.service;

import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Delivery meters, exported on {@code /actuator/prometheus} as
 * {@code sink_deliveries_total{outcome}} and {@code sink_delivery_latency_seconds{outcome}}.
 * Outcomes are the three {@link DeliveryOutcome}s plus {@code INJECTED_FAULT} and {@code ERROR}.
 * Requests rejected by validation (400) are not counted.
 */
@Component
public class SinkMetrics {

    /** Extra outcome label values beyond {@link DeliveryOutcome}. */
    public enum Failure { INJECTED_FAULT, ERROR }

    private final Map<DeliveryOutcome, Counter> outcomeCounters = new EnumMap<>(DeliveryOutcome.class);
    private final Map<DeliveryOutcome, Timer> outcomeTimers = new EnumMap<>(DeliveryOutcome.class);
    private final Map<Failure, Counter> failureCounters = new EnumMap<>(Failure.class);
    private final Map<Failure, Timer> failureTimers = new EnumMap<>(Failure.class);

    public SinkMetrics(MeterRegistry registry) {
        for (DeliveryOutcome o : DeliveryOutcome.values()) {
            outcomeCounters.put(o, counter(registry, o.name()));
            outcomeTimers.put(o, timer(registry, o.name()));
        }
        for (Failure f : Failure.values()) {
            failureCounters.put(f, counter(registry, f.name()));
            failureTimers.put(f, timer(registry, f.name()));
        }
    }

    public void record(DeliveryOutcome outcome, long elapsedNanos) {
        outcomeCounters.get(outcome).increment();
        outcomeTimers.get(outcome).record(Duration.ofNanos(elapsedNanos));
    }

    public void record(Failure failure, long elapsedNanos) {
        failureCounters.get(failure).increment();
        failureTimers.get(failure).record(Duration.ofNanos(elapsedNanos));
    }

    private static Counter counter(MeterRegistry registry, String outcome) {
        return Counter.builder("sink.deliveries")
                .description("Delivery requests by outcome")
                .tag("outcome", outcome)
                .register(registry);
    }

    private static Timer timer(MeterRegistry registry, String outcome) {
        return Timer.builder("sink.delivery.latency")
                .description("Delivery handling time, including injected latency")
                .tag("outcome", outcome)
                .publishPercentileHistogram()
                .register(registry);
    }
}
