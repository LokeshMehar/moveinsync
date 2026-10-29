package com.moveinsync.scheduler.core;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Rendezvous (highest-random-weight) hashing of partitions onto live nodes.
 * Every node computes the same answer from the same membership view with no coordination, and when a node
 * joins or leaves only ~1/N of the partitions move (never between two surviving nodes).
 */
public final class PartitionAssigner {

    private PartitionAssigner() {
    }

    public static int partitionOf(UUID jobId, int partitions) {
        return (int) Math.floorMod(mix(jobId.getMostSignificantBits() ^ mix(jobId.getLeastSignificantBits())), (long) partitions);
    }

    public static String ownerOf(int partition, Collection<String> liveNodes) {
        String best = null;
        long bestScore = Long.MIN_VALUE;
        for (String node : liveNodes) {
            long score = mix(mix(node.hashCode()) ^ partition * 0x9E3779B97F4A7C15L);
            if (best == null || score > bestScore || (score == bestScore && node.compareTo(best) > 0)) {
                best = node;
                bestScore = score;
            }
        }
        return best;
    }

    public static Set<Integer> desiredFor(String nodeId, Collection<String> liveNodes, int partitions) {
        Set<Integer> mine = new HashSet<>();
        for (int p = 0; p < partitions; p++) {
            if (nodeId.equals(ownerOf(p, liveNodes))) {
                mine.add(p);
            }
        }
        return mine;
    }

    /** splitmix64 finaliser. */
    static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }
}
