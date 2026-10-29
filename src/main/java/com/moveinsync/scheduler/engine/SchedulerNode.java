package com.moveinsync.scheduler.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.config.SchedulerProperties;
import com.moveinsync.scheduler.core.BackoffPolicy;
import com.moveinsync.scheduler.handlers.JobHandler;
import com.moveinsync.scheduler.model.Models.ClaimedRun;
import com.moveinsync.scheduler.model.Models.Tenant;
import com.moveinsync.scheduler.store.ClusterStore;
import com.moveinsync.scheduler.store.DbClock;
import com.moveinsync.scheduler.store.JobStore;
import com.moveinsync.scheduler.store.RunStore;
import com.moveinsync.scheduler.store.TenantStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.random.RandomGenerator;

/**
 * One scheduler node: membership, materialiser, dispatcher and executor wired together. It is a plain object (no
 * Spring needed) so tests can run several nodes in one JVM against one database.
 */
public class SchedulerNode {

    private static final Logger log = LoggerFactory.getLogger(SchedulerNode.class);

    private final String nodeId;
    private final SchedulerProperties props;
    private final ProcessFreeze freeze = new ProcessFreeze();
    private final SchedulerMetrics metrics;
    private final ClusterMembership membership;
    private final Materializer materializer;
    private final RunExecutor executor;
    private final RunStore runs;
    private final TenantCache tenants;

    private volatile boolean running;
    private volatile Thread dispatchThread;
    private volatile Thread materializeThread;
    private ScheduledExecutorService housekeeping;

    public SchedulerNode(DataSource dataSource, SchedulerProperties props, MeterRegistry registry,
                         List<JobHandler> handlerList, ObjectMapper mapper) {
        this.nodeId = props.nodeId();
        this.props = props;
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        TransactionTemplate tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        this.runs = new RunStore(jdbc, tx);
        ClusterStore cluster = new ClusterStore(jdbc);
        this.tenants = new TenantCache(new TenantStore(jdbc), Duration.ofSeconds(5));
        this.metrics = new SchedulerMetrics(registry, nodeId);
        this.membership = new ClusterMembership(nodeId, props, cluster, metrics, freeze);
        this.materializer = new Materializer(props, new JobStore(jdbc), runs, new DbClock(jdbc), new ScheduleCache(),
                tx, membership, metrics);
        Map<String, JobHandler> handlers = handlerList.stream().collect(Collectors.toMap(JobHandler::type, Function.identity()));
        this.executor = new RunExecutor(nodeId, props, runs, handlers,
                new BackoffPolicy(props.retryBase(), props.retryMax(), threadSafeRandom()), metrics, mapper, freeze);
    }

    /** ThreadLocalRandom is bound to its thread, so delegate per call (also avoids the jdk.random module on slim JREs). */
    private static RandomGenerator threadSafeRandom() {
        return () -> java.util.concurrent.ThreadLocalRandom.current().nextLong();
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        membership.start();
        executor.start();
        dispatchThread = Thread.ofPlatform().name("dispatcher-" + nodeId).daemon().start(this::dispatchLoop);
        materializeThread = Thread.ofPlatform().name("materializer-" + nodeId).daemon().start(this::materializeLoop);
        housekeeping = Executors.newSingleThreadScheduledExecutor(r -> Thread.ofPlatform().name("housekeeping").daemon().unstarted(r));
        housekeeping.scheduleWithFixedDelay(this::refreshClusterGauges, 1, 5, TimeUnit.SECONDS);
        log.info("scheduler node started partitions={} lease={}ms", membership.owned().size(), props.lease().toMillis());
    }

    // ------------------------------------------------------------------------------------------------ loops

    private void dispatchLoop() {
        int cursor = 0;
        while (running) {
            try {
                freeze.await();
                if (!membership.valid() || executor.free() == 0) {
                    pause(props.poll());
                    continue;
                }
                int claimedThisPass = 0;
                // TODO: this probes every tenant each pass; past a few thousand tenants keep an "active tenants" set instead
                List<Tenant> all = tenants.all();
                for (int i = 0; i < all.size() && running; i++) {
                    // Round-robin start index: no tenant is permanently first in line (fairness).
                    // paused is enforced in SQL (hasClaimableWork + claim), not from this 5 s cache, so a resume
                    // issued on another node takes effect here immediately
                    claimedThisPass += claimFor(all.get((cursor + i) % all.size()));
                }
                cursor++;
                if (claimedThisPass == 0) {
                    pause(props.poll());
                }
            } catch (Exception e) {
                metrics.loopErrors.increment();
                log.warn("dispatch pass failed: {}", e.toString());
                pause(Duration.ofMillis(500));
            }
        }
    }

    private int claimFor(Tenant tenant) {
        String partitions = membership.ownedCsv();
        if (!runs.hasClaimableWork(tenant.id(), partitions)) {
            return 0;
        }
        int reserved = executor.reserve(props.claimBatch());
        if (reserved == 0) {
            return 0;
        }
        List<ClaimedRun> claimed = List.of();
        try {
            claimed = runs.claim(tenant.id(), tenant.maxConcurrency(), partitions, reserved, nodeId, props.lease());
        } finally {
            executor.release(reserved - claimed.size());
        }
        claimed.forEach(executor::submit);
        return claimed.size();
    }

    private void materializeLoop() {
        while (running) {
            try {
                freeze.await();
                int n = materializer.tick();
                if (n < 200) {
                    pause(props.poll().multipliedBy(5));
                }
            } catch (Exception e) {
                metrics.loopErrors.increment();
                log.warn("materialize pass failed: {}", e.toString());
                pause(Duration.ofMillis(500));
            }
        }
    }

    private void refreshClusterGauges() {
        try {
            Map<String, Long> counts = runs.countsByStatus(null);
            metrics.clusterSnapshot(counts.getOrDefault("DEAD", 0L), runs.retryingCount(), runs.oldestDueAgeMillis());
        } catch (Exception e) {
            log.debug("gauge refresh failed: {}", e.toString());
        }
    }

    private static void pause(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------------------------------------ lifecycle

    /** Graceful shutdown: stop claiming, drain in-flight work, give partitions back. */
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        executor.stop(Duration.ofSeconds(10));
        membership.stop();
        if (housekeeping != null) {
            housekeeping.shutdownNow();
        }
    }

    /** Emulates kill -9: everything stops at once, nothing is committed or released. */
    public synchronized void crash() {
        running = false;
        executor.halt();
        membership.halt();
        if (housekeeping != null) {
            housekeeping.shutdownNow();
        }
        Thread.interrupted();
    }

    /** Emulates SIGSTOP / a long GC pause. */
    public void freeze() {
        freeze.freeze();
    }

    public void thaw() {
        freeze.thaw();
    }

    public String nodeId() {
        return nodeId;
    }

    public SchedulerMetrics metrics() {
        return metrics;
    }

    public ClusterMembership membership() {
        return membership;
    }

    public RunExecutor executor() {
        return executor;
    }

    public TenantCache tenantCache() {
        return tenants;
    }

    public boolean isRunning() {
        return running;
    }
}
