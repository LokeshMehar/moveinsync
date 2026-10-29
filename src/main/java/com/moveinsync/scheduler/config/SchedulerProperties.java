package com.moveinsync.scheduler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

@ConfigurationProperties("scheduler")
public record SchedulerProperties(
        @DefaultValue("") String nodeId,
        @DefaultValue("64") int partitions,
        @DefaultValue("15s") Duration lease,
        @DefaultValue("6s") Duration nodeTtl,
        @DefaultValue("100ms") Duration poll,
        @DefaultValue("100") int claimBatch,
        @DefaultValue("256") int workerCapacity,
        @DefaultValue("30s") Duration materializeHorizon,
        @DefaultValue("60s") Duration misfireGrace,
        @DefaultValue("1000") int maxCatchUp,
        @DefaultValue("1s") Duration retryBase,
        @DefaultValue("5m") Duration retryMax,
        @DefaultValue("false") boolean benchEnabled) {

    public static SchedulerProperties defaults(String nodeId) {
        return new SchedulerProperties(nodeId, 64, Duration.ofSeconds(15), Duration.ofSeconds(6),
                Duration.ofMillis(100), 100, 256, Duration.ofSeconds(30), Duration.ofSeconds(60), 1000,
                Duration.ofSeconds(1), Duration.ofMinutes(5), true);
    }

    public SchedulerProperties withNodeId(String id) {
        return new SchedulerProperties(id, partitions, lease, nodeTtl, poll, claimBatch, workerCapacity,
                materializeHorizon, misfireGrace, maxCatchUp, retryBase, retryMax, benchEnabled);
    }

    public SchedulerProperties withLease(Duration l, Duration ttl) {
        return new SchedulerProperties(nodeId, partitions, l, ttl, poll, claimBatch, workerCapacity,
                materializeHorizon, misfireGrace, maxCatchUp, retryBase, retryMax, benchEnabled);
    }

    public SchedulerProperties withRetry(Duration base, Duration max) {
        return new SchedulerProperties(nodeId, partitions, lease, nodeTtl, poll, claimBatch, workerCapacity,
                materializeHorizon, misfireGrace, maxCatchUp, base, max, benchEnabled);
    }
}
