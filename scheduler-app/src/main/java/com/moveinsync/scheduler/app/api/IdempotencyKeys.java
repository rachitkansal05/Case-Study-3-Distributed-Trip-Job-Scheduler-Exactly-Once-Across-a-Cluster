package com.moveinsync.scheduler.app.api;

import com.moveinsync.scheduler.app.error.ValidationException;
import java.util.regex.Pattern;

/** The {@code Idempotency-Key} request header: 1-128 printable ASCII characters, no spaces. */
public final class IdempotencyKeys {

    public static final String HEADER = "Idempotency-Key";
    private static final Pattern VALID = Pattern.compile("[\\x21-\\x7E]{1,128}");

    private IdempotencyKeys() {
    }

    /** An optional header: null stays null, anything else must be valid. */
    public static String optional(String key) {
        return key == null ? null : requireValid(key);
    }

    /** @throws ValidationException naming the header (never echoing its value) when malformed */
    public static String requireValid(String key) {
        if (key == null || !VALID.matcher(key).matches()) {
            throw ValidationException.field(HEADER, "must be 1-128 printable ASCII characters without spaces");
        }
        return key;
    }
}
