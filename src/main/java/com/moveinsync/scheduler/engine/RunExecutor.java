package com.moveinsync.scheduler.engine;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.config.SchedulerProperties;
import com.moveinsync.scheduler.core.BackoffPolicy;
import com.moveinsync.scheduler.core.MisfirePolicy;
import com.moveinsync.scheduler.handlers.JobHandler;
import com.moveinsync.scheduler.model.Models.ClaimedRun;
import com.moveinsync.scheduler.model.Models.Outcome;
import com.moveinsync.scheduler.store.RunStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs leased runs on virtual threads and settles them with fenced writes.
 *
 * <p>Lifecycle of one run: lease (fence token F) -> handler -> fenced commit. While the handler works, a renewer
 * thread extends the lease; if the lease is lost (this node was paused too long) the commit is rejected by the
 * database because the fence no longer matches, and the new owner's result wins.
 */
public class RunExecutor {

    private static final Logger log = LoggerFactory.getLogger(RunExecutor.class);
    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() { };

    /** handlerDone + the monitor on this object decide whether the renewer may still interrupt the worker thread. */
    private record InFlight(ClaimedRun run, Thread thread, long deadlineNanos, AtomicBoolean leaseLost,
                            AtomicBoolean timedOut, AtomicBoolean handlerDone) {
    }

    private final String nodeId;
    private final SchedulerProperties props;
    private final RunStore runs;
    private final Map<String, JobHandler> handlers;
    private final BackoffPolicy backoff;
    private final SchedulerMetrics metrics;
    private final ObjectMapper mapper;
    private final ProcessFreeze freeze;
    private final Semaphore permits;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService renewer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "lease-renewer");
        t.setDaemon(true);
        return t;
    });
    private final Map<Long, InFlight> inFlight = new ConcurrentHashMap<>();
    private volatile boolean halted;

    public RunExecutor(String nodeId, SchedulerProperties props, RunStore runs, Map<String, JobHandler> handlers,
                       BackoffPolicy backoff, SchedulerMetrics metrics, ObjectMapper mapper, ProcessFreeze freeze) {
        this.nodeId = nodeId;
        this.props = props;
        this.runs = runs;
        this.handlers = handlers;
        this.backoff = backoff;
        this.metrics = metrics;
        this.mapper = mapper;
        this.freeze = freeze;
        this.permits = new Semaphore(props.workerCapacity());
    }

    public void start() {
        long period = Math.max(50, props.lease().toMillis() / 3);
        renewer.scheduleWithFixedDelay(this::renewAll, period, period, TimeUnit.MILLISECONDS);
    }

    /** Reserves up to {@code wanted} worker slots (back-pressure: we never claim more than we can run). */
    public int reserve(int wanted) {
        int n = Math.min(wanted, permits.availablePermits());
        return n > 0 && permits.tryAcquire(n) ? n : 0;
    }

    public void release(int n) {
        if (n > 0) {
            permits.release(n);
        }
    }

    public int free() {
        return permits.availablePermits();
    }

    public int running() {
        return inFlight.size();
    }

    /** Hands an already-reserved slot to a claimed run. */
    public void submit(ClaimedRun run) {
        workers.execute(() -> execute(run));
    }

    // ------------------------------------------------------------------------------------------------ one run

    private void execute(ClaimedRun run) {
        InFlight state = new InFlight(run, Thread.currentThread(),
                System.nanoTime() + Duration.ofSeconds(run.maxRunSeconds()).toNanos(), new AtomicBoolean(), new AtomicBoolean(), new AtomicBoolean());
        inFlight.put(run.id(), state);
        metrics.inFlight(inFlight.size());
        MDC.put("node", nodeId);
        MDC.put("tenant", run.tenantId());
        MDC.put("runId", String.valueOf(run.id()));
        MDC.put("job", String.valueOf(run.jobId()));
        try {
            metrics.claimed.increment();
            if (run.takeover()) {
                metrics.takeovers.increment();
                log.info("took over expired lease attempt={} fence={}", run.attempts(), run.fence());
            }
            // neverStarted (not attempts == 1): a dead-letter replay resets attempts but is not a late first fire
            if (run.neverStarted()) {
                metrics.fireLag.record(Duration.ofMillis(Math.max(0, run.lagMillis())));
            }
            if (run.attempts() > run.maxAttempts()) {
                settleDead(run, "attempts exhausted (lease expired repeatedly)");
                return;
            }
            if (run.misfirePolicy() == MisfirePolicy.SKIP && run.neverStarted()
                    && run.lagMillis() > props.misfireGrace().toMillis()) {
                if (runs.markSkipped(run, "misfire: SKIP policy")) {
                    metrics.misfireSkipped.increment();
                }
                return;
            }
            JobHandler handler = handlers.get(run.type());
            if (handler == null) {
                settleDead(run, "no handler registered for job type");
                return;
            }
            Outcome outcome = null;
            Exception failure = null;
            try {
                outcome = handler.handle(run, parsePayload(run.payload()));
            } catch (Exception e) {
                failure = e;
            } finally {
                endHandler(state);             // from here on the renewer can no longer interrupt us mid-JDBC
            }
            if (failure instanceof InterruptedException) {
                if (state.timedOut().get()) {
                    metrics.timedOut.increment();
                    settleFailure(run, "TimeoutException: exceeded max run time");
                } else {
                    Thread.currentThread().interrupt();
                }
                return;
            }
            if (failure != null) {
                settleFailure(run, describe(failure));
                return;
            }
            if (state.timedOut().get()) {
                metrics.timedOut.increment();
                settleFailure(run, "TimeoutException: exceeded max run time");
                return;
            }
            if (halted) {
                return;                        // emulated kill -9: the process dies before committing
            }
            freeze.await();                    // emulated SIGSTOP between "work done" and "commit"
            commit(run, outcome);
        } catch (Exception e) {
            metrics.loopErrors.increment();
            log.error("unexpected error settling run: {}", e.toString());
        } finally {
            inFlight.remove(run.id());
            metrics.inFlight(inFlight.size());
            permits.release();
            MDC.clear();
        }
    }

    /**
     * Interrupting a virtual thread that is blocked on the JDBC socket can close the connection, so the timeout
     * interrupt must never land after the handler has returned. Both sides take the same monitor to make that atomic.
     */
    private void endHandler(InFlight state) {
        synchronized (state) {
            state.handlerDone().set(true);
        }
        Thread.interrupted();                  // swallow an interrupt that arrived just before we took the monitor
    }

    private boolean interruptIfStillRunning(InFlight state) {
        synchronized (state) {
            if (state.handlerDone().get()) {
                return false;
            }
            state.timedOut().set(true);
            state.thread().interrupt();
            return true;
        }
    }

    private void commit(ClaimedRun run, Outcome outcome) {
        RunStore.Commit commit = runs.complete(run, outcome.effects());
        if (commit.committed()) {
            metrics.succeeded.increment();
            metrics.duplicateEffectsSuppressed.increment(commit.effectsSuppressed());
        } else {
            metrics.fencedOut.increment();
            log.warn("commit rejected by fencing token fence={} (lease lost, another node owns this run)", run.fence());
        }
    }

    private void settleFailure(ClaimedRun run, String error) {
        if (halted) {
            return;
        }
        metrics.failed.increment();
        if (run.attempts() >= run.maxAttempts()) {
            settleDead(run, error);
            return;
        }
        Duration delay = backoff.delayAfter(run.attempts());
        if (runs.scheduleRetry(run, delay, error)) {
            metrics.retried.increment();
            log.info("run failed, retry in {} ms attempt={}/{}", delay.toMillis(), run.attempts(), run.maxAttempts());
        } else {
            metrics.fencedOut.increment();
        }
    }

    private void settleDead(ClaimedRun run, String error) {
        if (halted) {
            return;
        }
        if (runs.markDead(run, error)) {
            metrics.deadLettered.increment();
            log.warn("run moved to dead-letter after {} attempts", run.attempts());
        } else {
            metrics.fencedOut.increment();
        }
    }

    // ------------------------------------------------------------------------------------------------ renewal

    private void renewAll() {
        try {
            freeze.await();
            long now = System.nanoTime();
            for (InFlight s : inFlight.values()) {
                if (now > s.deadlineNanos() && !s.timedOut().get() && interruptIfStillRunning(s)) {
                    continue;                               // max run time exceeded: stop renewing, interrupted
                }
                if (s.timedOut().get() || s.leaseLost().get()) {
                    continue;
                }
                if (!runs.renew(s.run().id(), s.run().fence(), props.lease())) {
                    s.leaseLost().set(true);                 // cooperative: the fenced commit will be rejected
                    metrics.leaseLost.increment();
                    log.warn("lease lost while running fence={}", s.run().fence());
                }
            }
        } catch (Exception e) {
            metrics.loopErrors.increment();
            log.warn("lease renewal pass failed: {}", e.toString());
        }
    }

    // ------------------------------------------------------------------------------------------------ helpers

    private Map<String, Object> parsePayload(String payload) throws Exception {
        return payload == null || payload.isBlank() ? Map.of() : mapper.readValue(payload, MAP);
    }

    /** Class name + a short message; payloads and personal data never reach this string. */
    private static String describe(Exception e) {
        String msg = e.getMessage() == null ? "" : e.getMessage();
        return e.getClass().getSimpleName() + ": " + (msg.length() > 200 ? msg.substring(0, 200) : msg);
    }

    /** Graceful: stop renewing; in-flight runs finish or their leases expire. */
    public void stop(Duration drain) {
        long deadline = System.nanoTime() + drain.toNanos();
        while (!inFlight.isEmpty() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        renewer.shutdownNow();
        workers.shutdownNow();
    }

    /** Test hook emulating kill -9. */
    public void halt() {
        halted = true;
        renewer.shutdownNow();
        workers.shutdownNow();
    }
}
