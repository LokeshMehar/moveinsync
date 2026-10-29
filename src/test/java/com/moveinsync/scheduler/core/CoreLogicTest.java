package com.moveinsync.scheduler.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class CoreLogicTest {

    // ----------------------------------------------------------------------------- backoff

    @Test
    void backoffGrowsExponentiallyWithinJitterBounds() {
        var backoff = new BackoffPolicy(Duration.ofSeconds(1), Duration.ofMinutes(5), new Random(7));
        for (int attempt = 1; attempt <= 12; attempt++) {
            long ceiling = backoff.ceilingAfter(attempt).toMillis();
            for (int i = 0; i < 200; i++) {
                long d = backoff.delayAfter(attempt).toMillis();
                assertThat(d).isBetween(ceiling / 2, ceiling);
            }
        }
        assertThat(backoff.ceilingAfter(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(backoff.ceilingAfter(4)).isEqualTo(Duration.ofSeconds(8));
        assertThat(backoff.ceilingAfter(40)).isEqualTo(Duration.ofMinutes(5));          // capped, no overflow
    }

    @Test
    void backoffJitterActuallySpreadsRetries() {
        var backoff = new BackoffPolicy(Duration.ofSeconds(1), Duration.ofMinutes(5), new Random(1));
        Set<Long> distinct = new java.util.HashSet<>();
        for (int i = 0; i < 100; i++) {
            distinct.add(backoff.delayAfter(5).toMillis());
        }
        assertThat(distinct.size()).isGreaterThan(50);        // thundering herd avoided
    }

    // ----------------------------------------------------------------------------- partitions

    @Test
    void everyPartitionHasExactlyOneOwnerAndLoadIsBalanced() {
        List<String> nodes = List.of("node1", "node2", "node3");
        Map<String, Integer> counts = new HashMap<>();
        for (int p = 0; p < 64; p++) {
            counts.merge(PartitionAssigner.ownerOf(p, nodes), 1, Integer::sum);
        }
        assertThat(counts.values().stream().mapToInt(i -> i).sum()).isEqualTo(64);
        assertThat(counts.values()).allSatisfy(c -> assertThat(c).isBetween(12, 32));
    }

    @Test
    void whenANodeLeavesOnlyItsPartitionsMove() {
        List<String> before = List.of("node1", "node2", "node3");
        List<String> after = List.of("node1", "node3");
        int moved = 0;
        for (int p = 0; p < 256; p++) {
            String was = PartitionAssigner.ownerOf(p, before);
            String is = PartitionAssigner.ownerOf(p, after);
            if (!was.equals(is)) {
                assertThat(was).isEqualTo("node2");           // survivors never swap partitions among themselves
                moved++;
            }
        }
        assertThat(moved).isBetween(40, 130);                  // ~1/3 of 256
    }

    @Test
    void whenANodeJoinsItOnlyTakesPartitionsFromOthers() {
        List<String> before = List.of("node1", "node2");
        List<String> after = List.of("node1", "node2", "node3");
        for (int p = 0; p < 256; p++) {
            String was = PartitionAssigner.ownerOf(p, before);
            String is = PartitionAssigner.ownerOf(p, after);
            assertThat(is.equals(was) || is.equals("node3")).isTrue();
        }
    }

    @Test
    void jobsSpreadAcrossPartitions() {
        int[] histogram = new int[64];
        for (int i = 0; i < 64_000; i++) {
            histogram[PartitionAssigner.partitionOf(UUID.randomUUID(), 64)]++;
        }
        for (int count : histogram) {
            assertThat(count).isBetween(800, 1200);
        }
    }

    // ----------------------------------------------------------------------------- misfire

    private static final Instant NOW = Instant.parse("2026-06-01T12:00:00Z");

    private List<Instant> everyMinuteFor(int minutes) {
        List<Instant> list = new ArrayList<>();
        for (int i = minutes; i >= 0; i--) {
            list.add(NOW.minus(Duration.ofMinutes(i)));
        }
        return list;
    }

    private final MisfireResolver resolver = new MisfireResolver(Duration.ofSeconds(60), 1000);

    @Test
    void fireNowRunsEveryMissedOccurrence() {
        var plan = resolver.resolve(everyMinuteFor(10), MisfirePolicy.FIRE_NOW, NOW);
        assertThat(plan.runs()).hasSize(11);
        assertThat(plan.skipped()).isZero();
    }

    @Test
    void skipDropsMissedButKeepsOnTime() {
        var plan = resolver.resolve(everyMinuteFor(10), MisfirePolicy.SKIP, NOW);
        assertThat(plan.runs()).hasSize(2);                    // NOW and NOW-60s are inside the 60 s grace window
        assertThat(plan.runs().get(1).scheduledAt()).isEqualTo(NOW);
        assertThat(plan.skipped()).isEqualTo(9);
    }

    @Test
    void coalesceCollapsesMissedIntoOneRunAtTheLatestMissedInstant() {
        var plan = resolver.resolve(everyMinuteFor(10), MisfirePolicy.COALESCE, NOW);
        assertThat(plan.runs()).hasSize(3);                    // 1 coalesced (9 missed) + 2 on time
        assertThat(plan.runs().get(0).coalescedCount()).isEqualTo(9);
        assertThat(plan.runs().get(0).scheduledAt()).isEqualTo(NOW.minus(Duration.ofMinutes(2)));
    }

    @Test
    void fireNowIsBoundedByTheCatchUpCap() {
        var capped = new MisfireResolver(Duration.ofSeconds(60), 5);
        var plan = capped.resolve(everyMinuteFor(100), MisfirePolicy.FIRE_NOW, NOW);
        assertThat(plan.runs()).hasSize(5);
        assertThat(plan.runs().get(4).scheduledAt()).isEqualTo(NOW);   // keeps the most recent ones
        assertThat(plan.skipped()).isEqualTo(96);
    }
}
