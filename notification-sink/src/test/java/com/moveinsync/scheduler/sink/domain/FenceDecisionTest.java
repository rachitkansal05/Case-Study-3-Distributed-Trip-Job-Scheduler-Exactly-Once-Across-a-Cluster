package com.moveinsync.scheduler.sink.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.moveinsync.scheduler.core.domain.FencingToken;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Token comparison as the sink applies it: pair order, epoch first, equal tokens accepted. */
class FenceDecisionTest {

    @ParameterizedTest(name = "fence {0}, incoming {1} -> {2}")
    @CsvSource({
            "7.41, 7.41, ACCEPT_EQUAL",
            "7.41, 7.42, ACCEPT_RAISE",
            "7.41, 7.40, REJECT_STALE",
            "7.999999, 8.1, ACCEPT_RAISE",
            "8.1, 7.999999, REJECT_STALE",
            "8.0, 8.0, ACCEPT_EQUAL",
            "0.0, 0.1, ACCEPT_RAISE",
            // epoch above 2^32 (cluster generation 1, timeline 1) beats any generation-0 epoch
            "4294967295.9223372036854775807, 4294967297.0, ACCEPT_RAISE",
            "4294967297.0, 4294967295.9223372036854775807, REJECT_STALE",
            "9223372036854775807.9223372036854775807, 9223372036854775807.9223372036854775806, REJECT_STALE",
    })
    void decidesByPairOrder(String fence, String incoming, FenceDecision expected) {
        assertThat(FenceDecision.decide(FencingToken.parse(fence), FencingToken.parse(incoming))).isEqualTo(expected);
    }

    @Test
    void onlyStaleIsRejected() {
        assertThat(FenceDecision.REJECT_STALE.accepted()).isFalse();
        assertThat(FenceDecision.ACCEPT_EQUAL.accepted()).isTrue();
        assertThat(FenceDecision.ACCEPT_RAISE.accepted()).isTrue();
    }

    @Test
    void rejectsNulls() {
        FencingToken t = new FencingToken(1, 1);
        assertThatNullPointerException().isThrownBy(() -> FenceDecision.decide(null, t));
        assertThatNullPointerException().isThrownBy(() -> FenceDecision.decide(t, null));
    }
}
