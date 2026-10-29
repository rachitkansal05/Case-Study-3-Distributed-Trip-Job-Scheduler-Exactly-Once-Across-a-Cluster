package com.moveinsync.scheduler.core.cluster;

import java.util.Objects;
import java.util.UUID;

/**
 * Maps a job id to its fixed partition (plan section 06): {@code partition_id =
 * floorMod(murmur3_32(uuidBytes(jobId), 0), 128)}. The API computes it once at job creation and
 * runs inherit it, so every component that computes a partition must produce exactly these values.
 *
 * <h2>Byte layout</h2>
 *
 * The UUID is hashed as its 16 bytes in big-endian order: {@link UUID#getMostSignificantBits()}
 * first (byte 0 is its top byte), then {@link UUID#getLeastSignificantBits()}. This is the
 * RFC 4122 byte order, i.e. the hex digits of the canonical string read left to right (the same
 * bytes as Postgres {@code uuid_send}). Seed 0. The 32-bit hash is interpreted as a signed Java
 * {@code int} and reduced with {@link Math#floorMod(int, int)}; for a power-of-two partition count
 * this equals the unsigned hash modulo the count (its low 7 bits for 128).
 *
 * <h2>Hash</h2>
 *
 * {@link #murmur3_32(byte[], int)} is standard MurmurHash3 x86_32: 4-byte blocks read
 * little-endian, mixed with {@code c1 = 0xcc9e2d51}, {@code c2 = 0x1b873593}, rotations 15 and 13,
 * {@code h = h * 5 + 0xe6546b64}; then the 1-3 byte tail; then {@code h ^= length} and {@code
 * fmix32}. Test vectors (unsigned hex):
 *
 * <ul>
 *   <li>{@code ""}, seed 0 &rarr; {@code 0x00000000}
 *   <li>{@code ""}, seed 1 &rarr; {@code 0x514E28B7}
 *   <li>{@code ""}, seed {@code 0xFFFFFFFF} &rarr; {@code 0x81F16F39}
 *   <li>{@code "hello"} (UTF-8), seed 0 &rarr; {@code 0x248BFA47}
 *   <li>{@code "The quick brown fox jumps over the lazy dog"}, seed 0 &rarr; {@code 0x2E4FF723}
 *   <li>{@code "Hello, world!"}, seed 1234 &rarr; {@code 0xFAF6CDB3}
 * </ul>
 *
 * <h2>Pinned partition vectors (128 partitions)</h2>
 *
 * <table>
 *   <caption>UUID to hash and partition</caption>
 *   <tr><th>job id</th><th>murmur3_32</th><th>partition</th></tr>
 *   <tr><td>{@code 00000000-0000-0000-0000-000000000000}</td><td>{@code 0x8134CDF8}</td><td>120</td></tr>
 *   <tr><td>{@code ffffffff-ffff-ffff-ffff-ffffffffffff}</td><td>{@code 0xE519346E}</td><td>110</td></tr>
 *   <tr><td>{@code 123e4567-e89b-12d3-a456-426614174000}</td><td>{@code 0x5F6B5122}</td><td>34</td></tr>
 *   <tr><td>{@code 550e8400-e29b-41d4-a716-446655440000}</td><td>{@code 0xB68E1D22}</td><td>34</td></tr>
 *   <tr><td>{@code f47ac10b-58cc-4372-a567-0e02b2c3d479}</td><td>{@code 0x9F88FA8A}</td><td>10</td></tr>
 * </table>
 *
 * <p>Stateless and thread-safe.
 */
public final class PartitionHasher {

    /** Number of fixed partitions ({@code scheduler.partitions}, rows in {@code partition_lease}). */
    public static final int PARTITIONS = 128;

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private PartitionHasher() {
    }

    /** Partition of {@code jobId} among {@link #PARTITIONS} partitions. */
    public static int partitionOf(UUID jobId) {
        return partitionOf(jobId, PARTITIONS);
    }

    /**
     * Partition of {@code jobId} among {@code partitions} partitions: {@code
     * floorMod(murmur3_32(uuidBytes, 0), partitions)}.
     *
     * @throws IllegalArgumentException if {@code partitions < 1}
     */
    public static int partitionOf(UUID jobId, int partitions) {
        Objects.requireNonNull(jobId, "jobId");
        if (partitions < 1) {
            throw new IllegalArgumentException("partitions must be >= 1: " + partitions);
        }
        return Math.floorMod(murmur3_32(toBytes(jobId), 0), partitions);
    }

    /**
     * Standard MurmurHash3 x86_32 of {@code data} with {@code seed} (see the class Javadoc for the
     * algorithm and vectors). The result is the unsigned 32-bit hash stored in an {@code int}.
     */
    public static int murmur3_32(byte[] data, int seed) {
        Objects.requireNonNull(data, "data");
        int h = seed;
        int length = data.length;
        int blockEnd = length & ~3;
        for (int i = 0; i < blockEnd; i += 4) {
            int k = (data[i] & 0xff)
                    | (data[i + 1] & 0xff) << 8
                    | (data[i + 2] & 0xff) << 16
                    | (data[i + 3] & 0xff) << 24;
            h ^= mixK(k);
            h = Integer.rotateLeft(h, 13);
            h = h * 5 + 0xe6546b64;
        }
        int k = 0;
        switch (length & 3) {
            case 3:
                k ^= (data[blockEnd + 2] & 0xff) << 16;
                // fall through
            case 2:
                k ^= (data[blockEnd + 1] & 0xff) << 8;
                // fall through
            case 1:
                k ^= data[blockEnd] & 0xff;
                h ^= mixK(k);
                break;
            default:
                break;
        }
        h ^= length;
        return fmix32(h);
    }

    /** The UUID's 16 bytes, most significant long first, each long big-endian. */
    static byte[] toBytes(UUID uuid) {
        byte[] out = new byte[16];
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        for (int i = 0; i < 8; i++) {
            out[i] = (byte) (msb >>> (56 - 8 * i));
            out[8 + i] = (byte) (lsb >>> (56 - 8 * i));
        }
        return out;
    }

    private static int mixK(int k) {
        k *= C1;
        k = Integer.rotateLeft(k, 15);
        return k * C2;
    }

    private static int fmix32(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }
}
