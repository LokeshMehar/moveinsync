package com.moveinsync.scheduler.handlers;

import com.moveinsync.scheduler.model.Models.ClaimedRun;
import com.moveinsync.scheduler.model.Models.Outcome;

import java.util.Map;

/**
 * Business logic of one job type. Execution is at-least-once, so a handler must be safe to run twice: it describes
 * its externally visible result as {@link Outcome} effects, which the engine persists atomically with the fenced
 * completion under an idempotency key. A handler that calls an external system directly must pass
 * {@link ClaimedRun#idempotencyKey()} to it.
 */
public interface JobHandler {

    String type();

    Outcome handle(ClaimedRun run, Map<String, Object> payload) throws Exception;
}
