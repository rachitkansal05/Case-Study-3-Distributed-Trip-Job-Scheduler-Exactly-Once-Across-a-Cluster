package com.moveinsync.scheduler.core.domain;

import java.util.Objects;

/**
 * Fencing token of one claim (plan section 07): the pair {@code (job_run.fencing_epoch,
 * job_run.fencing_seq)}, compared epoch first, then sequence. There is no bit-packing of the
 * sequence, so neither part has a range limit beyond {@code long}.
 *
 * <ul>
 *   <li>{@code epoch = cluster_generation * 2^32 + timeline}, where {@code timeline} is the
 *       Postgres timeline ID (unsigned 32-bit, rises on every promotion) and {@code
 *       cluster_generation} is in {@code [0, 2^31 - 1]}. See {@link #epochOf(int, long)}.
 *   <li>{@code seq = nextval('fence_seq')}, taken on every claim.
 * </ul>
 *
 * <p>Sent on every outbound effect as header {@value #HEADER}{@code : <epoch>.<seq>}. A token is
 * used only after its claim commit is acknowledged.
 *
 * @param epoch non-negative epoch
 * @param seq non-negative sequence value
 */
public record FencingToken(long epoch, long seq) implements Comparable<FencingToken> {

    /** HTTP header carrying the token. */
    public static final String HEADER = "Fencing-Token";

    /** Largest accepted cluster generation (matches the {@code cluster_meta} CHECK). */
    public static final int MAX_CLUSTER_GENERATION = Integer.MAX_VALUE;

    /** Largest Postgres timeline ID (unsigned 32-bit). */
    public static final long MAX_TIMELINE = 0xFFFF_FFFFL;

    public FencingToken {
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch must be >= 0");
        }
        if (seq < 0) {
            throw new IllegalArgumentException("seq must be >= 0");
        }
    }

    /**
     * Computes {@code clusterGeneration * 2^32 + timeline}.
     *
     * @param clusterGeneration {@code cluster_meta.cluster_generation}, in {@code [0, 2^31 - 1]}
     * @param timeline Postgres timeline ID, in {@code [1, 2^32 - 1]}; a missing or non-positive
     *     timeline must make the claim fail closed, which this method enforces by throwing
     * @throws IllegalArgumentException if either argument is out of range
     */
    public static long epochOf(int clusterGeneration, long timeline) {
        if (clusterGeneration < 0) {
            throw new IllegalArgumentException("cluster generation must be >= 0");
        }
        if (timeline < 1 || timeline > MAX_TIMELINE) {
            throw new IllegalArgumentException("timeline must be in [1, 2^32 - 1]: " + timeline);
        }
        return ((long) clusterGeneration << 32) + timeline;
    }

    /** Epoch first, then sequence. */
    @Override
    public int compareTo(FencingToken other) {
        int byEpoch = Long.compare(epoch, other.epoch);
        return byEpoch != 0 ? byEpoch : Long.compare(seq, other.seq);
    }

    /** True if this token is strictly newer than {@code other}. */
    public boolean isNewerThan(FencingToken other) {
        return compareTo(Objects.requireNonNull(other, "other")) > 0;
    }

    /** Header value {@code "<epoch>.<seq>"}, both in plain decimal. */
    public String toHeader() {
        return epoch + "." + seq;
    }

    /**
     * Parses a header value {@code "<epoch>.<seq>"} (plain non-negative decimals, no sign, no
     * whitespace).
     *
     * @throws IllegalArgumentException if the value is malformed or out of range
     */
    public static FencingToken parse(String header) {
        Objects.requireNonNull(header, "header");
        int dot = header.indexOf('.');
        if (dot <= 0 || dot != header.lastIndexOf('.') || dot == header.length() - 1) {
            throw new IllegalArgumentException("fencing token must be <epoch>.<seq>");
        }
        return new FencingToken(parsePart(header, 0, dot), parsePart(header, dot + 1, header.length()));
    }

    private static long parsePart(String s, int from, int to) {
        for (int i = from; i < to; i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                throw new IllegalArgumentException("fencing token must be <epoch>.<seq>");
            }
        }
        try {
            return Long.parseLong(s, from, to, 10);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("fencing token part out of range", e);
        }
    }

    @Override
    public String toString() {
        return toHeader();
    }
}
