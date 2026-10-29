package com.moveinsync.scheduler.service;

import com.moveinsync.scheduler.api.ApiException;
import com.moveinsync.scheduler.config.SchedulerProperties;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Load-test support (enabled with scheduler.bench-enabled=true, OPERATOR only). Seeds N one-off jobs whose due times
 * are spread evenly over a window, then reports fire lag computed from the database record of what actually happened.
 */
public class BenchService {

    private final JdbcTemplate jdbc;
    private final SchedulerProperties props;

    public BenchService(JdbcTemplate jdbc, SchedulerProperties props) {
        this.jdbc = jdbc;
        this.props = props;
    }

    public Map<String, Object> seed(int count, int windowSeconds, int tenants, int workMillis, int startDelaySeconds) {
        if (!props.benchEnabled()) {
            throw new ApiException(HttpStatus.NOT_FOUND, "NOT_FOUND", "resource not found");
        }
        if (count < 1 || count > 1_000_000 || windowSeconds < 1 || tenants < 1 || tenants > 1000) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_BENCH", "count 1..1e6, windowSeconds>=1, tenants 1..1000");
        }
        String batch = UUID.randomUUID().toString().substring(0, 8);
        jdbc.update("INSERT INTO tenant (id, name, max_concurrency) SELECT 'bench-' || g, 'bench ' || g, 5000 "
                + "FROM generate_series(0, ? - 1) g ON CONFLICT DO NOTHING", tenants);
        String payload = "{\"bench\":\"" + batch + "\",\"workMillis\":" + workMillis + "}";
        return jdbc.execute((java.sql.Connection c) -> {
            boolean auto = c.getAutoCommit();
            c.setAutoCommit(false);
            try (var st = c.createStatement()) {
                st.execute("CREATE TEMP TABLE bench_seed ON COMMIT DROP AS SELECT gen_random_uuid() AS id, g AS i, "
                        + "clock_timestamp() + (" + startDelaySeconds + " * interval '1 second') + (g::float8 / " + count
                        + " * " + windowSeconds + " * interval '1 second') AS due FROM generate_series(1, " + count + ") g");
                st.execute("INSERT INTO job (id, tenant_id, name, type, time_zone, run_at, payload, max_run_seconds, max_attempts, "
                        + "misfire_policy, partition_id) SELECT id, 'bench-' || (i % " + tenants + "), 'bench-" + batch
                        + "', 'PICKUP_ALERT', 'UTC', due, '" + payload + "'::jsonb, 60, 5, 'FIRE_NOW', "
                        + "(hashtext(id::text) & 2147483647) % " + props.partitions() + " FROM bench_seed");
                st.execute("INSERT INTO job_run (job_id, tenant_id, partition_id, type, payload, priority, max_run_seconds, max_attempts, "
                        + "misfire_policy, scheduled_at, run_at, status, idempotency_key) SELECT j.id, j.tenant_id, j.partition_id, j.type, "
                        + "j.payload, 5, 60, 5, 'FIRE_NOW', j.run_at, j.run_at, 'PENDING', "
                        + "j.tenant_id || ':' || j.id || ':' || (extract(epoch from j.run_at) * 1000000)::bigint "
                        + "FROM job j WHERE j.name = 'bench-" + batch + "'");
                c.commit();
            } catch (Exception e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(auto);
            }
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("batch", batch);
            out.put("jobs", count);
            out.put("windowSeconds", windowSeconds);
            out.put("startsInSeconds", startDelaySeconds);
            return out;
        });
    }

    public Map<String, Object> report(String batch) {
        if (!props.benchEnabled()) {
            throw new ApiException(HttpStatus.NOT_FOUND, "NOT_FOUND", "resource not found");
        }
        Map<String, Object> out = new LinkedHashMap<>(jdbc.queryForMap("""
                SELECT count(*) AS total,
                       count(*) FILTER (WHERE status = 'SUCCEEDED') AS succeeded,
                       count(*) FILTER (WHERE status = 'PENDING' OR status = 'RUNNING') AS unfinished,
                       count(*) FILTER (WHERE status = 'DEAD') AS dead,
                       count(*) FILTER (WHERE attempts > 1) AS retried_or_taken_over,
                       round((percentile_cont(0.50) WITHIN GROUP (ORDER BY lag))::numeric, 1) AS lag_p50_ms,
                       round((percentile_cont(0.95) WITHIN GROUP (ORDER BY lag))::numeric, 1) AS lag_p95_ms,
                       round((percentile_cont(0.99) WITHIN GROUP (ORDER BY lag))::numeric, 1) AS lag_p99_ms,
                       round(max(lag)::numeric, 1) AS lag_max_ms,
                       round((EXTRACT(EPOCH FROM (max(finished_at) - min(scheduled_at))))::numeric, 1) AS wall_seconds
                FROM (SELECT status, attempts, finished_at, scheduled_at,
                             EXTRACT(EPOCH FROM (first_started_at - scheduled_at)) * 1000 AS lag
                      FROM job_run WHERE payload->>'bench' = ?) r
                """, batch));
        out.put("effects", jdbc.queryForObject("SELECT count(*) FROM effect e JOIN job_run r ON r.id = e.run_id WHERE r.payload->>'bench' = ?", Long.class, batch));
        out.put("duplicateEffects", jdbc.queryForObject("SELECT count(*) - count(DISTINCT idempotency_key) FROM effect e JOIN job_run r ON r.id = e.run_id WHERE r.payload->>'bench' = ?", Long.class, batch));
        Map<String, Long> perNode = new LinkedHashMap<>();
        jdbc.query("SELECT lease_owner, count(*) n FROM job_run WHERE payload->>'bench' = ? GROUP BY lease_owner ORDER BY 1",
                rs -> {
                    perNode.put(String.valueOf(rs.getString(1)), rs.getLong(2));
                }, batch);
        out.put("runsPerNode", perNode);
        return out;
    }
}
