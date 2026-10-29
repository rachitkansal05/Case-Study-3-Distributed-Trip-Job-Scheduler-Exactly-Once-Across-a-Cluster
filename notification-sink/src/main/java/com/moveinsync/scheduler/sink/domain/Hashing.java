package com.moveinsync.scheduler.sink.domain;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** SHA-256 helpers. Used so that payloads and idempotency keys never reach storage or logs in clear. */
public final class Hashing {

    /** Length of {@link #logHash(String)}: enough to correlate log lines, too short to be a lookup key. */
    public static final int LOG_HASH_LENGTH = 16;

    private Hashing() {
    }

    /** Lower-case hex SHA-256 of {@code bytes} (64 characters). */
    public static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every JRE", e);
        }
    }

    /** First {@value #LOG_HASH_LENGTH} hex characters of the SHA-256 of {@code value} (UTF-8), for logs. */
    public static String logHash(String value) {
        return sha256Hex(value.getBytes(StandardCharsets.UTF_8)).substring(0, LOG_HASH_LENGTH);
    }
}
