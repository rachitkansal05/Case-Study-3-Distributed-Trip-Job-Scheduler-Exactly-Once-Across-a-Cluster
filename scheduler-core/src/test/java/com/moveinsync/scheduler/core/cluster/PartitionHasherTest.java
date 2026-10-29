package com.moveinsync.scheduler.core.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Plan section 06: {@code partition_id = murmur3(job_id) mod 128}, vectors pinned for other teams. */
class PartitionHasherTest {

    private static int hash(String s, int seed) {
        return PartitionHasher.murmur3_32(s.getBytes(StandardCharsets.UTF_8), seed);
    }

    @Test
    void murmur3MatchesStandardVectors() {
        assertThat(hash("", 0)).isEqualTo(0);
        assertThat(hash("", 1)).isEqualTo(0x514E28B7);
        assertThat(hash("", 0xFFFFFFFF)).isEqualTo(0x81F16F39);
        assertThat(hash("hello", 0)).isEqualTo(0x248BFA47);
        assertThat(hash("The quick brown fox jumps over the lazy dog", 0)).isEqualTo(0x2E4FF723);
        assertThat(hash("Hello, world!", 1234)).isEqualTo(0xFAF6CDB3);
        assertThat(hash("test", 0x9747B28C)).isEqualTo(0x704B81DC);
    }

    @Test
    void murmur3CoversEveryTailLength() {
        // Cross-checked against an independent reference implementation.
        assertThat(PartitionHasher.murmur3_32(new byte[] {(byte) 0xff}, 0)).isEqualTo(0xFD6CF10D);
        assertThat(PartitionHasher.murmur3_32(new byte[] {0x21, 0x43}, 0)).isEqualTo(0xA0F7B07A);
        assertThat(PartitionHasher.murmur3_32(new byte[] {0x21, 0x43, 0x65}, 0)).isEqualTo(0x7E4A8634);
        assertThat(PartitionHasher.murmur3_32(new byte[] {0x21, 0x43, 0x65, (byte) 0x87}, 0))
                .isEqualTo(0xF55B516B);
    }

    @Test
    void uuidBytesAreBigEndianMostSignificantFirst() {
        UUID id = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
        byte[] expected = new byte[16];
        for (int i = 0; i < 16; i++) {
            expected[i] = (byte) (i * 0x11);
        }
        assertThat(PartitionHasher.toBytes(id)).containsExactly(expected);
    }

    @Test
    void pinnedPartitionVectors() {
        assertPinned("00000000-0000-0000-0000-000000000000", 0x8134CDF8, 120);
        assertPinned("ffffffff-ffff-ffff-ffff-ffffffffffff", 0xE519346E, 110);
        assertPinned("123e4567-e89b-12d3-a456-426614174000", 0x5F6B5122, 34);
        assertPinned("550e8400-e29b-41d4-a716-446655440000", 0xB68E1D22, 34);
        assertPinned("f47ac10b-58cc-4372-a567-0e02b2c3d479", 0x9F88FA8A, 10);
    }

    private static void assertPinned(String uuid, int expectedHash, int expectedPartition) {
        UUID id = UUID.fromString(uuid);
        assertThat(PartitionHasher.murmur3_32(PartitionHasher.toBytes(id), 0)).as(uuid).isEqualTo(expectedHash);
        assertThat(PartitionHasher.partitionOf(id)).as(uuid).isEqualTo(expectedPartition);
        assertThat(PartitionHasher.partitionOf(id, PartitionHasher.PARTITIONS)).isEqualTo(expectedPartition);
        assertThat(Integer.toUnsignedLong(expectedHash) % 128).as("floorMod == unsigned mod").isEqualTo(expectedPartition);
    }

    @Test
    void partitionOfRespectsCustomCountAndValidates() {
        UUID id = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");
        assertThat(PartitionHasher.partitionOf(id, 1)).isZero();
        assertThat(PartitionHasher.partitionOf(id, 7)).isEqualTo(Math.floorMod(0xE519346E, 7));
        assertThatThrownBy(() -> PartitionHasher.partitionOf(id, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PartitionHasher.partitionOf(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> PartitionHasher.murmur3_32(null, 0)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void randomUuidsSpreadUniformly() {
        Random random = new Random(7);
        int n = 100_000;
        int[] counts = new int[PartitionHasher.PARTITIONS];
        for (int i = 0; i < n; i++) {
            int p = PartitionHasher.partitionOf(new UUID(random.nextLong(), random.nextLong()));
            assertThat(p).isBetween(0, PartitionHasher.PARTITIONS - 1);
            counts[p]++;
        }
        double expected = (double) n / PartitionHasher.PARTITIONS; // 781.25, sd about 28
        double chiSquare = 0;
        for (int c : counts) {
            assertThat(c).isBetween((int) (expected * 0.85), (int) (expected * 1.15));
            chiSquare += (c - expected) * (c - expected) / expected;
        }
        // 127 degrees of freedom: mean 127, p = 0.999 at about 181.
        assertThat(chiSquare).isLessThan(181);
    }
}
