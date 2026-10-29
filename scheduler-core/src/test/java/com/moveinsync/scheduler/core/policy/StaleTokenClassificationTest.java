package com.moveinsync.scheduler.core.policy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.core.domain.RunFailure;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Plan section 16, R4 StaleTokenClassificationTest: a lease loss (409 stale token, fenced write 0
 * rows) does not count as an attempt; the 20th lease loss dead-letters with FENCING_CONFLICT.
 */
class StaleTokenClassificationTest {

    private final RetryDecider decider =
            new RetryDecider(attempt -> Duration.ofSeconds(attempt), new FailureClassifier());

    @Test
    void nineteenLeaseLossesStayInTheRetryLoop() {
        for (int lost = 0; lost < 20; lost++) {
            assertThat(decider.onLeaseLost(lost)).as("lease loss %d", lost).isEmpty();
        }
    }

    @Test
    void twentiethLeaseLossDeadLettersWithFencingConflict() {
        Optional<RunFailure.DeadLetter> dl = decider.onLeaseLost(20);
        assertThat(dl).isPresent();
        assertThat(dl.get().errorCode()).isEqualTo(RetryDecider.FENCING_CONFLICT).isEqualTo("FENCING_CONFLICT");
        assertThat(decider.onLeaseLost(21)).isPresent();
    }

    @Test
    void leaseLossesDoNotChangeTheAttemptBasedDecision() {
        RetryableJobException timeout = new RetryableJobException("TIMEOUT", "t");
        RunFailure before = decider.onFailure(timeout, 2, 5);
        for (int lost = 1; lost <= 19; lost++) {
            decider.onLeaseLost(lost);
        }
        // Still attempt 2 of 5: nineteen lease losses consumed no attempts.
        assertThat(decider.onFailure(timeout, 2, 5)).isEqualTo(before).isInstanceOf(RunFailure.Retry.class);
    }

    @Test
    void customLimit() {
        RetryDecider strict = new RetryDecider(attempt -> Duration.ZERO, new FailureClassifier(), 3);
        assertThat(strict.onLeaseLost(2)).isEmpty();
        assertThat(strict.onLeaseLost(3)).isPresent();
        assertThatThrownBy(() -> strict.onLeaseLost(-1)).isInstanceOf(IllegalArgumentException.class);
    }
}
