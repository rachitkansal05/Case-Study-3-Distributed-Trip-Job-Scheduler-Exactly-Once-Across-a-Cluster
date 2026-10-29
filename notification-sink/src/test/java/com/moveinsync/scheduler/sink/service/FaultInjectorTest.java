package com.moveinsync.scheduler.sink.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.sink.domain.FaultSettings;
import org.junit.jupiter.api.Test;

class FaultInjectorTest {

    private final FaultInjector faults = new FaultInjector();

    @Test
    void noFaultsByDefault() {
        assertThat(faults.current()).isEqualTo(FaultSettings.NONE);
        for (int i = 0; i < 1_000; i++) {
            assertThatNoException().isThrownBy(faults::apply);
        }
    }

    @Test
    void failureRateOneAlwaysFailsWithConfiguredStatus() {
        faults.set(new FaultSettings(0, 0, 1.0, 500));
        for (int i = 0; i < 1_000; i++) {
            assertThatThrownBy(faults::apply).isInstanceOfSatisfying(InjectedFaultException.class,
                    e -> assertThat(e.status()).isEqualTo(500));
        }
    }

    @Test
    void failureRateIsApproximatelyHonoured() {
        faults.set(new FaultSettings(0, 0, 0.3, 503));
        int failed = 0;
        int n = 20_000;
        for (int i = 0; i < n; i++) {
            try {
                faults.apply();
            } catch (InjectedFaultException e) {
                failed++;
            }
        }
        assertThat(failed / (double) n).isBetween(0.27, 0.33);
    }

    @Test
    void latencyIsAddedAndClearRemovesIt() {
        faults.set(new FaultSettings(40, 20, 0.0, 503));
        long start = System.nanoTime();
        faults.apply();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs).isGreaterThanOrEqualTo(40);

        faults.clear();
        assertThat(faults.current()).isEqualTo(FaultSettings.NONE);
    }

    @Test
    void settingsAreValidated() {
        assertThatIllegalArgumentException().isThrownBy(() -> new FaultSettings(-1, 0, 0, 503));
        assertThatIllegalArgumentException().isThrownBy(() -> new FaultSettings(0, 60_001, 0, 503));
        assertThatIllegalArgumentException().isThrownBy(() -> new FaultSettings(0, 0, 1.5, 503));
        assertThatIllegalArgumentException().isThrownBy(() -> new FaultSettings(0, 0, Double.NaN, 503));
        assertThatIllegalArgumentException().isThrownBy(() -> new FaultSettings(0, 0, 0.5, 502));
    }
}
