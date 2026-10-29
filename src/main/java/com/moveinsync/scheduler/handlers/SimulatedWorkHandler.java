package com.moveinsync.scheduler.handlers;

import com.moveinsync.scheduler.model.Models.ClaimedRun;
import com.moveinsync.scheduler.model.Models.Effect;
import com.moveinsync.scheduler.model.Models.Outcome;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Template for the job types of the trip platform. The subclass names its type and the kind of effect it produces;
 * the payload may carry {@code workMillis} to simulate I/O latency (used by the failure-injection tests).
 * Payloads hold identifiers only, never personal data.
 */
public abstract class SimulatedWorkHandler implements JobHandler {

    /** Counts handler invocations (not effects): invocations > effects proves at-least-once + idempotent effect. */
    public static final AtomicLong INVOCATIONS = new AtomicLong();

    @Override
    public final Outcome handle(ClaimedRun run, Map<String, Object> payload) throws Exception {
        INVOCATIONS.incrementAndGet();
        long workMillis = asLong(payload.get("workMillis"), 0);
        if (workMillis > 0) {
            Thread.sleep(workMillis);
        }
        beforeEffect(run, payload);
        return Outcome.of(new Effect(effectKind(), "run=" + run.id()));
    }

    protected void beforeEffect(ClaimedRun run, Map<String, Object> payload) throws Exception {
    }

    protected abstract String effectKind();

    protected static long asLong(Object value, long fallback) {
        return value instanceof Number n ? n.longValue() : fallback;
    }
}
