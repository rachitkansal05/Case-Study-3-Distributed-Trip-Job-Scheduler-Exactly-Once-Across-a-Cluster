package com.moveinsync.scheduler.sink.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.core.domain.FencingToken;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class RequestHeadersTest {

    @Test
    void parsesFencingToken() {
        assertThat(RequestHeaders.fencingToken("7.41")).isEqualTo(new FencingToken(7, 41));
        assertThat(RequestHeaders.fencingToken("0.0")).isEqualTo(new FencingToken(0, 0));
        assertThat(RequestHeaders.fencingToken("4294967297.12"))
                .isEqualTo(new FencingToken(FencingToken.epochOf(1, 1), 12));
        assertThat(RequestHeaders.fencingToken("9223372036854775807.9223372036854775807"))
                .isEqualTo(new FencingToken(Long.MAX_VALUE, Long.MAX_VALUE));
    }

    @ParameterizedTest
    @NullAndEmptySource
    void missingFencingToken(String raw) {
        assertThatThrownBy(() -> RequestHeaders.fencingToken(raw))
                .isInstanceOfSatisfying(SinkProblemException.class,
                        e -> assertThat(e.code()).isEqualTo(ProblemCode.MISSING_FENCING_TOKEN));
    }

    @ParameterizedTest
    @ValueSource(strings = {"7", "7.", ".41", "7.41.1", "-7.41", "7.-41", "+7.41", " 7.41", "7.41 ", "a.b",
            "7,41", "7..41", "9223372036854775808.1", "1.9223372036854775808", "٧.٤١", "0x7.1"})
    void malformedFencingToken(String raw) {
        assertThatThrownBy(() -> RequestHeaders.fencingToken(raw))
                .isInstanceOfSatisfying(SinkProblemException.class, e -> {
                    assertThat(e.code()).isEqualTo(ProblemCode.INVALID_FENCING_TOKEN);
                    assertThat(e.status().value()).isEqualTo(400);
                });
    }

    @Test
    void acceptsIdempotencyKeys() {
        assertThat(RequestHeaders.idempotencyKey("77:emp-7")).isEqualTo("77:emp-7");
        assertThat(RequestHeaders.idempotencyKey("job-9:2026-03-08T07:00:00Z:item-1"))
                .isEqualTo("job-9:2026-03-08T07:00:00Z:item-1");
        String longest = "k".repeat(RequestHeaders.MAX_IDEMPOTENCY_KEY_LENGTH);
        assertThat(RequestHeaders.idempotencyKey(longest)).isEqualTo(longest);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void missingIdempotencyKey(String raw) {
        assertThatThrownBy(() -> RequestHeaders.idempotencyKey(raw))
                .isInstanceOfSatisfying(SinkProblemException.class,
                        e -> assertThat(e.code()).isEqualTo(ProblemCode.MISSING_IDEMPOTENCY_KEY));
    }

    @ParameterizedTest
    @ValueSource(strings = {"has space", "tab\there", "new\nline", "naïve", "\u007f"})
    void invalidIdempotencyKey(String raw) {
        assertThatThrownBy(() -> RequestHeaders.idempotencyKey(raw))
                .isInstanceOfSatisfying(SinkProblemException.class, e -> {
                    assertThat(e.code()).isEqualTo(ProblemCode.INVALID_IDEMPOTENCY_KEY);
                    assertThat(e.getMessage()).doesNotContain(raw);
                });
    }

    @Test
    void tooLongIdempotencyKey() {
        String tooLong = "k".repeat(RequestHeaders.MAX_IDEMPOTENCY_KEY_LENGTH + 1);
        assertThatThrownBy(() -> RequestHeaders.idempotencyKey(tooLong))
                .isInstanceOfSatisfying(SinkProblemException.class,
                        e -> assertThat(e.code()).isEqualTo(ProblemCode.INVALID_IDEMPOTENCY_KEY));
    }
}
