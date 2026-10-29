package com.moveinsync.scheduler.store;

import com.moveinsync.scheduler.core.MisfirePolicy;
import com.moveinsync.scheduler.model.Models.ClaimedRun;
import com.moveinsync.scheduler.model.Models.Effect;
import com.moveinsync.scheduler.model.Models.NewRun;
import com.moveinsync.scheduler.model.Models.RunView;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.moveinsync.scheduler.model.Models.instant;
import static com.moveinsync.scheduler.model.Models.ts;

/**
 * All run-state transitions. Every write made on behalf of a lease carries the fencing token and re-checks, in the
 * same statement, that the lease is still alive on the database clock. A zero row count means "you lost the lease".
 */
public class RunStore {

    public enum CommitResult { COMMITTED, FENCED_OUT }

    public record Commit(CommitResult result, int effectsWritten, int effectsSuppressed) {
        public boolean committed() {
            return result == CommitResult.COMMITTED;
        }
    }

    private static final RowMapper<RunView> VIEW = (rs, i) -> new RunView(rs.getLong("id"),
            rs.getObject("job_id", UUID.class), rs.getString("type"), rs.getString("status"), rs.getInt("attempts"),
            instant(rs.getObject("scheduled_at", OffsetDateTime.class)), instant(rs.getObject("run_at", OffsetDateTime.class)),
            instant(rs.getObject("first_started_at", OffsetDateTime.class)),
            instant(rs.getObject("finished_at", OffsetDateTime.class)), rs.getString("last_error"),
            rs.getInt("coalesced_count"), rs.getString("lease_owner"));

    private static final RowMapper<ClaimedRun> CLAIMED = (rs, i) -> new ClaimedRun(rs.getLong("id"),
            rs.getObject("job_id", UUID.class), rs.getString("tenant_id"), rs.getString("type"), rs.getString("payload"),
            rs.getInt("priority"), rs.getInt("max_run_seconds"), rs.getInt("max_attempts"), rs.getInt("attempts"),
            rs.getLong("fence"), instant(rs.getObject("scheduled_at", OffsetDateTime.class)),
            MisfirePolicy.valueOf(rs.getString("misfire_policy")), rs.getString("idempotency_key"),
            rs.getInt("coalesced_count"), "RUNNING".equals(rs.getString("prev_status")), rs.getBoolean("never_started"),
            rs.getLong("lag_ms"));

    private static final String CLAIM_SQL = """
            WITH cand AS (
                SELECT r.id, r.status AS prev_status, (r.first_started_at IS NULL) AS never_started FROM job_run r
                WHERE r.tenant_id = ? AND r.partition_id = ANY (string_to_array(?, ',')::int[])
                  AND ((r.status = 'PENDING' AND r.run_at <= clock_timestamp())
                    OR (r.status = 'RUNNING' AND r.lease_expires_at < clock_timestamp()))
                  AND EXISTS (SELECT 1 FROM tenant t WHERE t.id = r.tenant_id AND NOT t.paused)
                ORDER BY r.priority DESC, r.run_at
                LIMIT ? FOR UPDATE OF r SKIP LOCKED)
            UPDATE job_run r SET status = 'RUNNING', attempts = r.attempts + 1, fence = nextval('fence_seq'),
                   lease_owner = ?, lease_expires_at = clock_timestamp() + (? * interval '1 millisecond'),
                   first_started_at = COALESCE(r.first_started_at, clock_timestamp())
            FROM cand WHERE r.id = cand.id
            RETURNING r.id, r.job_id, r.tenant_id, r.type, r.payload::text AS payload, r.priority, r.max_run_seconds,
                      r.max_attempts, r.attempts, r.fence, r.scheduled_at, r.misfire_policy, r.idempotency_key,
                      r.coalesced_count, cand.prev_status, cand.never_started,
                      (EXTRACT(EPOCH FROM (clock_timestamp() - r.scheduled_at)) * 1000)::bigint AS lag_ms
            """;

    private static final String FENCED = " WHERE id = ? AND fence = ? AND status = 'RUNNING' AND lease_expires_at > clock_timestamp()";

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;

    public RunStore(JdbcTemplate jdbc, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.tx = tx;
    }

    // ---------------------------------------------------------------- creation

    /**
     * Idempotent: re-inserting an occurrence (same job + scheduled instant) is a no-op. No created-row count is
     * returned on purpose: with reWriteBatchedInserts the driver reports SUCCESS_NO_INFO for every statement.
     */
    public void insertRuns(List<NewRun> runs) {
        if (runs.isEmpty()) {
            return;
        }
        jdbc.batchUpdate("""
                INSERT INTO job_run (job_id, tenant_id, partition_id, type, payload, priority, max_run_seconds, max_attempts,
                                     misfire_policy, scheduled_at, run_at, status, coalesced_count, idempotency_key)
                VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)
                ON CONFLICT (job_id, scheduled_at) DO NOTHING
                """, runs, 500, (ps, r) -> {
            ps.setObject(1, r.jobId());
            ps.setString(2, r.tenantId());
            ps.setInt(3, r.partitionId());
            ps.setString(4, r.type());
            ps.setString(5, r.payload());
            ps.setInt(6, r.priority());
            ps.setInt(7, r.maxRunSeconds());
            ps.setInt(8, r.maxAttempts());
            ps.setString(9, r.misfirePolicy().name());
            ps.setObject(10, ts(r.scheduledAt()));
            ps.setObject(11, ts(r.runAt()));
            ps.setInt(12, r.coalescedCount());
            ps.setString(13, r.idempotencyKey());
        });
    }

    /** Looks up one occurrence by its identity (job + scheduled instant), e.g. the run a manual trigger just created. */
    public Optional<RunView> findOccurrence(UUID jobId, String tenantId, Instant scheduledAt) {
        return jdbc.query("SELECT * FROM job_run WHERE job_id = ? AND tenant_id = ? AND scheduled_at = ?",
                VIEW, jobId, tenantId, ts(scheduledAt)).stream().findFirst();
    }

    /** Called when a job is deleted: queued runs must not fire. Runs already executing finish normally. */
    public int cancelPending(UUID jobId, String tenantId) {
        return jdbc.update("UPDATE job_run SET status = 'SKIPPED', finished_at = clock_timestamp(), last_error = 'job deactivated' "
                + "WHERE job_id = ? AND tenant_id = ? AND status = 'PENDING'", jobId, tenantId);
    }

    // ---------------------------------------------------------------- claiming

    public boolean hasClaimableWork(String tenantId, String partitionCsv) {
        Boolean found = jdbc.queryForObject("""
                SELECT EXISTS (SELECT 1 FROM job_run r
                  WHERE r.tenant_id = ? AND r.partition_id = ANY (string_to_array(?, ',')::int[])
                    AND ((r.status = 'PENDING' AND r.run_at <= clock_timestamp())
                      OR (r.status = 'RUNNING' AND r.lease_expires_at < clock_timestamp()))
                    AND EXISTS (SELECT 1 FROM tenant t WHERE t.id = r.tenant_id AND NOT t.paused))
                """, Boolean.class, tenantId, partitionCsv);
        return Boolean.TRUE.equals(found);
    }

    /**
     * Leases up to {@code limit} due runs of one tenant, never exceeding the tenant's cluster-wide concurrency cap.
     * A transaction-scoped advisory lock on the tenant serialises the count+claim across nodes, so the cap is exact,
     * while different tenants never contend with each other.
     */
    public List<ClaimedRun> claim(String tenantId, int tenantCap, String partitionCsv, int limit, String nodeId, Duration lease) {
        return tx.execute(status -> {
            jdbc.queryForObject("SELECT pg_advisory_xact_lock(hashtext(?))", Object.class, "tenant-claim:" + tenantId);
            Integer running = jdbc.queryForObject("""
                    SELECT count(*) FROM job_run
                    WHERE tenant_id = ? AND status = 'RUNNING' AND lease_expires_at >= clock_timestamp()
                    """, Integer.class, tenantId);
            int room = Math.min(limit, tenantCap - (running == null ? 0 : running));
            if (room <= 0) {
                return List.<ClaimedRun>of();
            }
            return jdbc.query(CLAIM_SQL, CLAIMED, tenantId, partitionCsv, room, nodeId, lease.toMillis());
        });
    }

    /** Extends the lease. False means the lease is gone (expired or taken over): the caller must stop. */
    public boolean renew(long runId, long fence, Duration lease) {
        return jdbc.update("UPDATE job_run SET lease_expires_at = clock_timestamp() + (? * interval '1 millisecond')" + FENCED,
                lease.toMillis(), runId, fence) == 1;
    }

    // ---------------------------------------------------------------- completion (all fenced)

    /** Marks the run succeeded and records its effects in ONE transaction, only if the fence still holds. */
    public Commit complete(ClaimedRun run, List<Effect> effects) {
        return tx.execute(status -> {
            int updated = jdbc.update("UPDATE job_run SET status = 'SUCCEEDED', finished_at = clock_timestamp(), "
                    + "lease_expires_at = NULL, last_error = NULL" + FENCED, run.id(), run.fence());
            if (updated == 0) {
                status.setRollbackOnly();
                return new Commit(CommitResult.FENCED_OUT, 0, 0);
            }
            int written = 0;
            for (int i = 0; i < effects.size(); i++) {
                Effect e = effects.get(i);
                written += jdbc.update("""
                        INSERT INTO effect (idempotency_key, tenant_id, run_id, kind, detail, fence)
                        VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (idempotency_key) DO NOTHING
                        """, run.idempotencyKey() + "#" + i, run.tenantId(), run.id(), e.kind(), e.detail(), run.fence());
            }
            return new Commit(CommitResult.COMMITTED, written, effects.size() - written);
        });
    }

    public boolean scheduleRetry(ClaimedRun run, Duration delay, String error) {
        return jdbc.update("UPDATE job_run SET status = 'PENDING', run_at = clock_timestamp() + (? * interval '1 millisecond'), "
                        + "lease_owner = NULL, lease_expires_at = NULL, last_error = ?" + FENCED,
                delay.toMillis(), error, run.id(), run.fence()) == 1;
    }

    public boolean markDead(ClaimedRun run, String error) {
        return jdbc.update("UPDATE job_run SET status = 'DEAD', finished_at = clock_timestamp(), lease_owner = NULL, "
                + "lease_expires_at = NULL, last_error = ?" + FENCED, error, run.id(), run.fence()) == 1;
    }

    public boolean markSkipped(ClaimedRun run, String reason) {
        return jdbc.update("UPDATE job_run SET status = 'SKIPPED', finished_at = clock_timestamp(), lease_owner = NULL, "
                + "lease_expires_at = NULL, last_error = ?" + FENCED, reason, run.id(), run.fence()) == 1;
    }

    // ---------------------------------------------------------------- operator views

    public List<RunView> runsOfJob(UUID jobId, String tenantId, int limit) {
        return jdbc.query("SELECT * FROM job_run WHERE job_id = ? AND tenant_id = ? ORDER BY scheduled_at DESC LIMIT ?",
                VIEW, jobId, tenantId, limit);
    }

    public List<RunView> deadLetters(String tenantId, int limit) {
        return jdbc.query("SELECT * FROM job_run WHERE tenant_id = ? AND status = 'DEAD' ORDER BY finished_at DESC LIMIT ?",
                VIEW, tenantId, limit);
    }

    /** Dead-letter replay: resets attempts and makes the run claimable again. Same idempotency key => still one effect. */
    public boolean replayDead(long runId, String tenantId) {
        return jdbc.update("UPDATE job_run SET status = 'PENDING', attempts = 0, run_at = clock_timestamp(), finished_at = NULL, "
                + "last_error = NULL WHERE id = ? AND tenant_id = ? AND status = 'DEAD'", runId, tenantId) == 1;
    }

    public Map<String, Long> countsByStatus(String tenantIdOrNull) {
        String sql = "SELECT status, count(*) AS n FROM job_run " + (tenantIdOrNull == null ? "" : "WHERE tenant_id = ? ")
                + "GROUP BY status";
        Map<String, Long> out = new java.util.TreeMap<>();
        RowMapper<Object> rm = (rs, i) -> {
            out.put(rs.getString("status"), rs.getLong("n"));
            return null;
        };
        if (tenantIdOrNull == null) {
            jdbc.query(sql, rm);
        } else {
            jdbc.query(sql, rm, tenantIdOrNull);
        }
        return out;
    }

    public long retryingCount() {
        Long n = jdbc.queryForObject("SELECT count(*) FROM job_run WHERE status = 'PENDING' AND attempts > 0", Long.class);
        return n == null ? 0 : n;
    }

    /** Age in ms of the oldest due-but-unstarted run: the "are we falling behind" number for the on-call. */
    public long oldestDueAgeMillis() {
        Long n = jdbc.queryForObject("""
                SELECT COALESCE((EXTRACT(EPOCH FROM (clock_timestamp() - min(run_at))) * 1000)::bigint, 0)
                FROM job_run WHERE status = 'PENDING' AND run_at <= clock_timestamp()
                """, Long.class);
        return n == null ? 0 : Math.max(0, n);
    }
}
