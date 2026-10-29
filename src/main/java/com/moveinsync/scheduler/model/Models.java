package com.moveinsync.scheduler.model;

import com.moveinsync.scheduler.core.MisfirePolicy;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

public final class Models {

    private Models() {
    }

    public record Tenant(String id, String name, String timeZone, int maxConcurrency, boolean paused) {
    }

    public record Job(UUID id, String tenantId, String name, String type, String cron, String timeZone,
                      Instant runAt, String payload, int priority, int maxRunSeconds, int maxAttempts,
                      MisfirePolicy misfirePolicy, int partitionId, Instant nextFireAt, boolean active,
                      Instant createdAt) {
        public boolean recurring() {
            return cron != null;
        }
    }

    /** A run row to insert. */
    public record NewRun(UUID jobId, String tenantId, int partitionId, String type, String payload, int priority,
                         int maxRunSeconds, int maxAttempts, MisfirePolicy misfirePolicy, Instant scheduledAt,
                         Instant runAt, int coalescedCount) {
        public String idempotencyKey() {
            return Models.idempotencyKey(tenantId, jobId, scheduledAt);
        }
    }

    /** A run this node has leased. {@code fence} is the token every write for this lease must present. */
    public record ClaimedRun(long id, UUID jobId, String tenantId, String type, String payload, int priority,
                             int maxRunSeconds, int maxAttempts, int attempts, long fence, Instant scheduledAt,
                             MisfirePolicy misfirePolicy, String idempotencyKey, int coalescedCount,
                             boolean takeover, boolean neverStarted, long lagMillis) {
    }

    public record Effect(String kind, String detail) {
    }

    public record Outcome(List<Effect> effects) {
        public static Outcome of(Effect... effects) {
            return new Outcome(List.of(effects));
        }
    }

    public record RunView(long id, UUID jobId, String type, String status, int attempts, Instant scheduledAt,
                          Instant runAt, Instant startedAt, Instant finishedAt, String lastError, int coalescedCount,
                          String leaseOwner) {
    }

    public static String idempotencyKey(String tenantId, UUID jobId, Instant scheduledAt) {
        return tenantId + ":" + jobId + ":" + ChronoUnit.MICROS.between(Instant.EPOCH, scheduledAt);
    }

    public static OffsetDateTime ts(Instant instant) {
        return instant == null ? null : OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    public static Instant instant(OffsetDateTime odt) {
        return odt == null ? null : odt.toInstant();
    }
}
