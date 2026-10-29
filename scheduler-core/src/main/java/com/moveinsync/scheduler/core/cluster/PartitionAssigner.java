package com.moveinsync.scheduler.core.cluster;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Desired partition owners by rendezvous (highest-random-weight) hashing (plan section 06):
 * {@code owner(p) = argmax over live nodes n of weight(n, p)}. Every node computes the same answer
 * from the same live set, without talking to the others.
 *
 * <h2>Rules</h2>
 *
 * <ul>
 *   <li><b>Weight.</b> {@link #weight(String, int)}: the node id's UTF-8 bytes are hashed to a
 *       64-bit key {@code (murmur3_32(bytes, 0) << 32) | murmur3_32(bytes, 0x9747B28C)}, then
 *       {@code weight = mix64(key ^ mix64((p + 1) * 0x9E3779B97F4A7C15))}, where {@code mix64} is
 *       the SplitMix64 finaliser. Weights are compared as <em>unsigned</em> 64-bit values. Only
 *       explicit hashing is used, so the result is the same on every JVM and platform.
 *   <li><b>Ties</b> (equal weights, practically never) go to the lexicographically smallest node
 *       id ({@link String#compareTo}).
 *   <li><b>Input.</b> Node ids must be non-null and non-blank; duplicates are ignored, and the
 *       order of the collection never matters.
 *   <li><b>Minimal movement</b> (exact, not statistical): removing a node moves only the
 *       partitions it owned; adding a node moves partitions only to the new node. This follows
 *       from the argmax: a partition's owner changes only if the changed node is, or becomes, the
 *       maximum.
 *   <li><b>Balance</b> is statistical: partition counts per node are multinomial, so a given set
 *       of ids may be somewhat uneven. No bounded-load correction is applied, because it would
 *       break the exact minimal-movement property.
 * </ul>
 *
 * <p>Stateless and thread-safe.
 */
public final class PartitionAssigner {

    private static final int SECOND_SEED = 0x9747b28c;
    private static final long GOLDEN = 0x9E3779B97F4A7C15L;

    /** Creates an assigner (it holds no state; instances are interchangeable). */
    public PartitionAssigner() {
    }

    /**
     * Assigns every partition {@code 0..partitions-1} to exactly one live node.
     *
     * @param partitions number of partitions, {@code >= 1}
     * @param liveNodes live node ids, in any order; duplicates ignored
     * @return an unmodifiable map sorted by node id, containing every distinct live node (with an
     *     empty set if it owns nothing), each value an unmodifiable ascending set; an empty map if
     *     {@code liveNodes} is empty
     * @throws IllegalArgumentException if {@code partitions < 1} or a node id is blank
     * @throws NullPointerException if {@code liveNodes} or a node id is null
     */
    public Map<String, Set<Integer>> assign(int partitions, Collection<String> liveNodes) {
        if (partitions < 1) {
            throw new IllegalArgumentException("partitions must be >= 1: " + partitions);
        }
        String[] nodes = distinctSorted(liveNodes);
        if (nodes.length == 0) {
            return Collections.emptyMap();
        }
        NavigableMap<String, NavigableSet<Integer>> owned = new TreeMap<>();
        for (String node : nodes) {
            owned.put(node, new TreeSet<>());
        }
        long[] keys = keys(nodes);
        for (int p = 0; p < partitions; p++) {
            owned.get(nodes[argmax(nodes, keys, p)]).add(p);
        }
        NavigableMap<String, Set<Integer>> result = new TreeMap<>();
        owned.forEach((node, set) -> result.put(node, Collections.unmodifiableNavigableSet(set)));
        return Collections.unmodifiableNavigableMap(result);
    }

    /**
     * The desired owner of one partition.
     *
     * @param partition partition id, {@code >= 0}
     * @param liveNodes live node ids, in any order; duplicates ignored
     * @throws IllegalArgumentException if {@code liveNodes} is empty (there is no owner),
     *     {@code partition < 0}, or a node id is blank
     * @throws NullPointerException if {@code liveNodes} or a node id is null
     */
    public String desiredOwner(int partition, Collection<String> liveNodes) {
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be >= 0: " + partition);
        }
        String[] nodes = distinctSorted(liveNodes);
        if (nodes.length == 0) {
            throw new IllegalArgumentException("no live nodes: partition " + partition + " has no owner");
        }
        return nodes[argmax(nodes, keys(nodes), partition)];
    }

    /**
     * Rendezvous weight of {@code nodeId} for {@code partition}, as specified in the class Javadoc.
     * Compare weights with {@link Long#compareUnsigned(long, long)}. Exposed so other components
     * can cross-check assignments.
     */
    public static long weight(String nodeId, int partition) {
        return weight(nodeKey(validNodeId(nodeId)), partition);
    }

    private static long weight(long nodeKey, int partition) {
        return mix64(nodeKey ^ mix64((partition + 1L) * GOLDEN));
    }

    private static long nodeKey(String nodeId) {
        byte[] bytes = nodeId.getBytes(StandardCharsets.UTF_8);
        long high = PartitionHasher.murmur3_32(bytes, 0) & 0xFFFF_FFFFL;
        long low = PartitionHasher.murmur3_32(bytes, SECOND_SEED) & 0xFFFF_FFFFL;
        return high << 32 | low;
    }

    /** SplitMix64 finaliser (a bijection on 64-bit values with full avalanche). */
    private static long mix64(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /** Index of the winner; {@code nodes} is sorted ascending, so the first maximum wins ties. */
    private static int argmax(String[] nodes, long[] keys, int partition) {
        int best = 0;
        long bestWeight = weight(keys[0], partition);
        for (int i = 1; i < nodes.length; i++) {
            long w = weight(keys[i], partition);
            if (Long.compareUnsigned(w, bestWeight) > 0) {
                best = i;
                bestWeight = w;
            }
        }
        return best;
    }

    private static long[] keys(String[] nodes) {
        long[] keys = new long[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            keys[i] = nodeKey(nodes[i]);
        }
        return keys;
    }

    private static String[] distinctSorted(Collection<String> liveNodes) {
        Objects.requireNonNull(liveNodes, "liveNodes");
        TreeSet<String> sorted = new TreeSet<>();
        for (String node : liveNodes) {
            sorted.add(validNodeId(node));
        }
        return sorted.toArray(new String[0]);
    }

    private static String validNodeId(String nodeId) {
        Objects.requireNonNull(nodeId, "nodeId");
        if (nodeId.isBlank()) {
            throw new IllegalArgumentException("node id must not be blank");
        }
        return nodeId;
    }
}
