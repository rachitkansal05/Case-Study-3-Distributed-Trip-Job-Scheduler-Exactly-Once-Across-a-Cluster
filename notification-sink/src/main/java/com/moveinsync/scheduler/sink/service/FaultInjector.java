package com.moveinsync.scheduler.sink.service;

import com.moveinsync.scheduler.sink.domain.FaultSettings;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.stereotype.Component;

/**
 * Process-local fault switch ({@code PUT/DELETE /admin/faults}). Not persisted: a restart clears it,
 * and with several sink instances each must be configured.
 */
@Component
public class FaultInjector {

    private final AtomicReference<FaultSettings> settings = new AtomicReference<>(FaultSettings.NONE);

    public FaultSettings current() {
        return settings.get();
    }

    public void set(FaultSettings next) {
        settings.set(Objects.requireNonNull(next, "next"));
    }

    public void clear() {
        settings.set(FaultSettings.NONE);
    }

    /**
     * Sleeps for the configured latency, then fails with the configured probability. Call before
     * any database access, so that an injected failure never delivers anything.
     *
     * @throws InjectedFaultException when the request is chosen to fail
     */
    public void apply() {
        FaultSettings s = settings.get();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long delay = s.latencyMs() + (s.latencyJitterMs() > 0 ? random.nextLong(s.latencyJitterMs() + 1) : 0);
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InjectedFaultException(s.failureStatus());
            }
        }
        if (s.failureRate() > 0 && (s.failureRate() >= 1.0 || random.nextDouble() < s.failureRate())) {
            throw new InjectedFaultException(s.failureStatus());
        }
    }
}
