package com.moveinsync.scheduler.engine;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/** Every counter an on-call engineer needs at 3 AM, in one place. All names are prefixed {@code scheduler.}. */
public class SchedulerMetrics {

    private final MeterRegistry registry;
    private final String node;

    public final Timer fireLag;
    public final Counter claimed;
    public final Counter takeovers;
    public final Counter succeeded;
    public final Counter failed;
    public final Counter retried;
    public final Counter deadLettered;
    public final Counter fencedOut;
    public final Counter leaseLost;
    public final Counter timedOut;
    public final Counter duplicateEffectsSuppressed;
    public final Counter misfireSkipped;
    public final Counter materialized;
    public final Counter loopErrors;

    private final AtomicLong inFlight = new AtomicLong();
    private final AtomicLong ownedPartitions = new AtomicLong();
    private final AtomicLong dbDead = new AtomicLong();
    private final AtomicLong dbRetrying = new AtomicLong();
    private final AtomicLong dbOldestDueMs = new AtomicLong();

    public SchedulerMetrics(MeterRegistry registry, String node) {
        this.registry = registry;
        this.node = node;
        Tags t = Tags.of("node", node);
        fireLag = Timer.builder("scheduler.fire.lag").description("actual start minus scheduled time")
                .publishPercentileHistogram(true).publishPercentiles(0.5, 0.95, 0.99)
                .serviceLevelObjectives(Duration.ofMillis(100), Duration.ofMillis(500), Duration.ofSeconds(1),
                        Duration.ofSeconds(2), Duration.ofSeconds(5), Duration.ofSeconds(30))
                .minimumExpectedValue(Duration.ofMillis(1)).maximumExpectedValue(Duration.ofMinutes(10))
                .tags(t).register(registry);
        claimed = counter("scheduler.runs.claimed", "runs leased by this node", t);
        takeovers = counter("scheduler.leases.takeovers", "expired leases of dead/frozen nodes taken over", t);
        succeeded = counter("scheduler.runs.succeeded", "runs committed", t);
        failed = counter("scheduler.runs.failed", "handler failures", t);
        retried = counter("scheduler.runs.retried", "failures rescheduled with backoff", t);
        deadLettered = counter("scheduler.runs.dead", "runs moved to the dead-letter list", t);
        fencedOut = counter("scheduler.fencing.rejections", "stale commits rejected by the fencing token", t);
        leaseLost = counter("scheduler.leases.lost", "leases lost while a handler was still running", t);
        timedOut = counter("scheduler.runs.timeout", "runs interrupted at max run time", t);
        duplicateEffectsSuppressed = counter("scheduler.effects.duplicates.suppressed", "duplicate effects absorbed by idempotency keys", t);
        misfireSkipped = counter("scheduler.misfire.skipped", "runs dropped by the SKIP misfire policy", t);
        materialized = counter("scheduler.runs.materialized", "cron occurrences planned (insert is idempotent, so a re-plan after a crash is counted again)", t);
        loopErrors = counter("scheduler.loop.errors", "unexpected errors in engine loops", t);
        gauge("scheduler.runs.running", "runs currently executing on this node", inFlight, t);
        gauge("scheduler.partitions.owned", "partitions this node owns", ownedPartitions, t);
        gauge("scheduler.deadletter.size", "runs in the dead-letter list (cluster)", dbDead, Tags.empty());
        gauge("scheduler.retrying.size", "runs waiting for a retry (cluster)", dbRetrying, Tags.empty());
        gauge("scheduler.queue.oldest.due.ms", "age of the oldest due-but-unstarted run (cluster queue depth proxy)", dbOldestDueMs, Tags.empty());
    }

    private Counter counter(String name, String description, Tags tags) {
        return Counter.builder(name).description(description).tags(tags).register(registry);
    }

    private void gauge(String name, String description, AtomicLong value, Tags tags) {
        Gauge.builder(name, value, AtomicLong::get).description(description).tags(tags).register(registry);
    }

    public String node() {
        return node;
    }

    public void inFlight(long n) {
        inFlight.set(n);
    }

    public void ownedPartitions(long n) {
        ownedPartitions.set(n);
    }

    public void clusterSnapshot(long dead, long retrying, long oldestDueMs) {
        dbDead.set(dead);
        dbRetrying.set(retrying);
        dbOldestDueMs.set(oldestDueMs);
    }
}
