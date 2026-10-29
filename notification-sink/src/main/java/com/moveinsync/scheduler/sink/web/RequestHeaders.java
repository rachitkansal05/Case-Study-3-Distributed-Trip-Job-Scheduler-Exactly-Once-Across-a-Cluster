package com.moveinsync.scheduler.sink.web;

import com.moveinsync.scheduler.core.domain.FencingToken;

/** Parses and validates the delivery headers. Error details never echo the header value. */
public final class RequestHeaders {

    /** Header carrying the effect's idempotency key. */
    public static final String IDEMPOTENCY_KEY = "Idempotency-Key";

    /** Longest accepted idempotency key, in characters. */
    public static final int MAX_IDEMPOTENCY_KEY_LENGTH = 256;

    private RequestHeaders() {
    }

    /**
     * Validates an {@code Idempotency-Key}: 1 to {@value #MAX_IDEMPOTENCY_KEY_LENGTH} characters,
     * each a visible ASCII character ({@code 0x21}–{@code 0x7E}; no spaces or control characters).
     *
     * @throws SinkProblemException {@code MISSING_IDEMPOTENCY_KEY} or {@code INVALID_IDEMPOTENCY_KEY}
     */
    public static String idempotencyKey(String raw) {
        if (raw == null || raw.isEmpty()) {
            throw new SinkProblemException(ProblemCode.MISSING_IDEMPOTENCY_KEY,
                    "The Idempotency-Key header is required.");
        }
        if (raw.length() > MAX_IDEMPOTENCY_KEY_LENGTH) {
            throw new SinkProblemException(ProblemCode.INVALID_IDEMPOTENCY_KEY,
                    "The Idempotency-Key header must be at most " + MAX_IDEMPOTENCY_KEY_LENGTH + " characters.");
        }
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (c < 0x21 || c > 0x7E) {
                throw new SinkProblemException(ProblemCode.INVALID_IDEMPOTENCY_KEY,
                        "The Idempotency-Key header must contain only visible ASCII characters.");
            }
        }
        return raw;
    }

    /**
     * Parses a {@code Fencing-Token: <epoch>.<seq>} header (plain non-negative decimals that fit a
     * signed 64-bit integer, no sign or whitespace).
     *
     * @throws SinkProblemException {@code MISSING_FENCING_TOKEN} or {@code INVALID_FENCING_TOKEN}
     */
    public static FencingToken fencingToken(String raw) {
        if (raw == null || raw.isEmpty()) {
            throw new SinkProblemException(ProblemCode.MISSING_FENCING_TOKEN,
                    "The Fencing-Token header is required.");
        }
        try {
            return FencingToken.parse(raw);
        } catch (IllegalArgumentException e) {
            throw new SinkProblemException(ProblemCode.INVALID_FENCING_TOKEN,
                    "The Fencing-Token header must be <epoch>.<seq> with non-negative 64-bit decimal parts.");
        }
    }
}
