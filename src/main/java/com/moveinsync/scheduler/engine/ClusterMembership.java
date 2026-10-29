package com.moveinsync.scheduler.engine;

import com.moveinsync.scheduler.config.SchedulerProperties;
import com.moveinsync.scheduler.core.PartitionAssigner;
import com.moveinsync.scheduler.store.ClusterStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Keeps this node's heartbeat and its partition leases alive.
 *
 * <p>Partition ownership is a <i>performance</i> device (it stops nodes scanning and fighting over the same rows);
 * <i>correctness</i> never depends on it, because run-level leases + fencing tokens are enforced in the database.
 * A node whose view is stale therefore can waste work but cannot cause a double effect.
 */
public class ClusterMembership {

    private static final Logger log = LoggerFactory.getLogger(ClusterMembership.class);

    private final String nodeId;
    private final SchedulerProperties props;
    private final ClusterStore store;
    private final SchedulerMetrics metrics;
    private final ProcessFreeze freeze;
    private final ScheduledExecutorService loop = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "membership-" + r.hashCode());
        t.setDaemon(true);
        return t;
    });

    private volatile Set<Integer> owned = Set.of();
    private volatile String ownedCsv = "";
    private volatile long validUntilNanos = 0;

    public ClusterMembership(String nodeId, SchedulerProperties props, ClusterStore store, SchedulerMetrics metrics, ProcessFreeze freeze) {
        this.nodeId = nodeId;
        this.props = props;
        this.store = store;
        this.metrics = metrics;
        this.freeze = freeze;
    }

    public void start() {
        store.ensurePartitions(props.partitions());
        round();
        long periodMs = Math.max(100, props.nodeTtl().toMillis() / 3);
        loop.scheduleWithFixedDelay(this::safeRound, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    private void safeRound() {
        try {
            freeze.await();
            round();
        } catch (Exception e) {
            metrics.loopErrors.increment();
            log.warn("membership round failed: {}", e.toString());
        }
    }

    synchronized void round() {
        long started = System.nanoTime();
        store.heartbeat(nodeId);
        List<String> live = new ArrayList<>(store.liveNodes(props.nodeTtl()));
        if (!live.contains(nodeId)) {
            live.add(nodeId);
        }
        Set<Integer> desired = PartitionAssigner.desiredFor(nodeId, live, props.partitions());
        store.release(nodeId, desired);
        Set<Integer> now = store.renewAndAcquire(nodeId, desired, props.nodeTtl(), props.nodeTtl());
        this.owned = now;
        this.ownedCsv = ClusterStore.csv(new TreeSet<>(now));
        // Conservative validity: measured from the START of the round, so a slow/frozen round cannot extend it.
        this.validUntilNanos = started + props.nodeTtl().toNanos();
        metrics.ownedPartitions(now.size());
    }

    /** True while this node may assume it still owns its partitions (monotonic clock, not wall clock). */
    public boolean valid() {
        return System.nanoTime() < validUntilNanos;
    }

    public String ownedCsv() {
        return ownedCsv;
    }

    public Set<Integer> owned() {
        return owned;
    }

    /** Graceful leave: hand partitions back immediately. */
    public void stop() {
        loop.shutdownNow();
        try {
            store.releaseAll(nodeId);
            store.removeNode(nodeId);
        } catch (Exception e) {
            log.warn("graceful leave failed, leases will expire on their own: {}", e.toString());
        }
        validUntilNanos = 0;
    }

    /** Test hook emulating kill -9: stop heartbeating and release nothing. */
    public void halt() {
        loop.shutdownNow();
        validUntilNanos = 0;
    }
}
