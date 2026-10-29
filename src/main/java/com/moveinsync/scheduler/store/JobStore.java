package com.moveinsync.scheduler.store;

import com.moveinsync.scheduler.core.MisfirePolicy;
import com.moveinsync.scheduler.model.Models.Job;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.moveinsync.scheduler.model.Models.instant;
import static com.moveinsync.scheduler.model.Models.ts;

public class JobStore {

    private static final RowMapper<Job> MAPPER = (rs, i) -> new Job(
            rs.getObject("id", UUID.class), rs.getString("tenant_id"), rs.getString("name"), rs.getString("type"),
            rs.getString("cron"), rs.getString("time_zone"), instant(rs.getObject("run_at", OffsetDateTime.class)),
            rs.getString("payload"), rs.getInt("priority"), rs.getInt("max_run_seconds"), rs.getInt("max_attempts"),
            MisfirePolicy.valueOf(rs.getString("misfire_policy")), rs.getInt("partition_id"),
            instant(rs.getObject("next_fire_at", OffsetDateTime.class)), rs.getBoolean("active"),
            instant(rs.getObject("created_at", OffsetDateTime.class)));

    private static final String COLUMNS = "id, tenant_id, name, type, cron, time_zone, run_at, payload::text AS payload, "
            + "priority, max_run_seconds, max_attempts, misfire_policy, partition_id, next_fire_at, active, created_at";

    private final JdbcTemplate jdbc;

    public JobStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(Job j) {
        jdbc.update("""
                INSERT INTO job (id, tenant_id, name, type, cron, time_zone, run_at, payload, priority, max_run_seconds,
                                 max_attempts, misfire_policy, partition_id, next_fire_at, active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, TRUE)
                """, j.id(), j.tenantId(), j.name(), j.type(), j.cron(), j.timeZone(), ts(j.runAt()), j.payload(),
                j.priority(), j.maxRunSeconds(), j.maxAttempts(), j.misfirePolicy().name(), j.partitionId(),
                ts(j.nextFireAt()));
    }

    /** Tenant-scoped lookup: a job of another tenant is indistinguishable from a missing one. */
    public Optional<Job> find(UUID id, String tenantId) {
        return jdbc.query("SELECT " + COLUMNS + " FROM job WHERE id = ? AND tenant_id = ?", MAPPER, id, tenantId)
                .stream().findFirst();
    }

    public List<Job> list(String tenantId, int limit, int offset) {
        return jdbc.query("SELECT " + COLUMNS + " FROM job WHERE tenant_id = ? AND active ORDER BY created_at DESC LIMIT ? OFFSET ?",
                MAPPER, tenantId, limit, offset);
    }

    public boolean deactivate(UUID id, String tenantId) {
        return jdbc.update("UPDATE job SET active = FALSE, next_fire_at = NULL WHERE id = ? AND tenant_id = ? AND active", id, tenantId) > 0;
    }

    /** Locks a batch of recurring jobs whose next fire is inside the horizon. Rows locked by another node are skipped. */
    public List<Job> lockDueRecurring(String partitionCsv, Instant horizonEnd, int limit) {
        return jdbc.query("SELECT " + COLUMNS + """
                 FROM job WHERE active AND cron IS NOT NULL AND next_fire_at IS NOT NULL AND next_fire_at <= ?
                   AND partition_id = ANY (string_to_array(?, ',')::int[])
                 ORDER BY next_fire_at LIMIT ? FOR UPDATE SKIP LOCKED
                """, MAPPER, ts(horizonEnd), partitionCsv, limit);
    }

    public void setNextFire(UUID id, Instant next) {
        jdbc.update("UPDATE job SET next_fire_at = ? WHERE id = ?", ts(next), id);
    }
}
