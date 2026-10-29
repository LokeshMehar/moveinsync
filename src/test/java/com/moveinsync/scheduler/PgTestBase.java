package com.moveinsync.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.config.SchedulerProperties;
import com.moveinsync.scheduler.engine.SchedulerNode;
import com.moveinsync.scheduler.handlers.JobHandler;
import com.moveinsync.scheduler.handlers.TripHandlers;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.awaitility.Awaitility.await;

/** Real PostgreSQL (Testcontainers) shared by all integration tests; several scheduler nodes run inside the JVM. */
public abstract class PgTestBase {

    protected static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16-alpine")
            .withCommand("postgres", "-c", "max_connections=300");
    protected static final HikariDataSource DS;
    static {
        PG.start();
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(PG.getJdbcUrl() + "&reWriteBatchedInserts=true");
        cfg.setUsername(PG.getUsername());
        cfg.setPassword(PG.getPassword());
        cfg.setMaximumPoolSize(60);
        DS = new HikariDataSource(cfg);
        Flyway.configure().dataSource(DS).load().migrate();
    }

    protected final JdbcTemplate jdbc = new JdbcTemplate(DS);
    protected final ObjectMapper mapper = new ObjectMapper();
    private final List<SchedulerNode> nodes = new ArrayList<>();

    @BeforeEach
    void cleanDatabase() {
        jdbc.execute("TRUNCATE effect, job_run, job, node, app_user, tenant RESTART IDENTITY CASCADE");
        jdbc.execute("UPDATE partition_lease SET owner = NULL, expires_at = '-infinity'");
        jdbc.update("INSERT INTO tenant (id, name, max_concurrency) VALUES ('acme', 'acme', 1000), ('globex', 'globex', 1000)");
    }

    @AfterEach
    void stopNodes() {
        nodes.forEach(n -> {
            n.thaw();
            n.crash();
        });
        nodes.clear();
    }

    /** Short leases so failover happens in seconds, not minutes. */
    protected SchedulerProperties fastProps(String id) {
        return SchedulerProperties.defaults(id).withLease(Duration.ofSeconds(3), Duration.ofSeconds(2))
                .withRetry(Duration.ofMillis(50), Duration.ofMillis(400));
    }

    protected SchedulerNode startNode(String id) {
        return startNode(fastProps(id));
    }

    protected SchedulerNode startNode(SchedulerProperties props) {
        List<JobHandler> handlers = List.of(new TripHandlers.PickupAlertHandler(), new TripHandlers.RosterGenerationHandler(),
                new TripHandlers.NoShowCancelHandler(), new TripHandlers.BillingCloseHandler(), new TripHandlers.FlakyHandler());
        SchedulerNode node = new SchedulerNode(DS, props, new SimpleMeterRegistry(), handlers, mapper);
        nodes.add(node);
        node.start();
        return node;
    }

    /** Waits until the given live nodes jointly own all partitions and every one of them owns a share. */
    protected void awaitBalanced(int partitions, SchedulerNode... live) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
            int total = 0;
            for (SchedulerNode n : live) {
                int owned = n.membership().owned().size();
                org.assertj.core.api.Assertions.assertThat(owned).isGreaterThan(0);
                total += owned;
            }
            org.assertj.core.api.Assertions.assertThat(total).isEqualTo(partitions);
        });
    }

    /**
     * Seeds {@code count} one-off jobs + runs for a tenant in one statement. {@code dueExpr} is a SQL timestamp
     * expression (may reference g, the 1..count row number). Returns the tag stored in payload.bench.
     */
    protected String seed(String tenant, int count, String type, String extraPayload, int maxAttempts, String policy, String dueExpr) {
        String tag = UUID.randomUUID().toString().substring(0, 8);
        String payload = "{\"bench\":\"" + tag + "\"" + (extraPayload.isEmpty() ? "" : "," + extraPayload) + "}";
        jdbc.update("WITH s AS (SELECT gen_random_uuid() AS id, " + dueExpr + " AS due FROM generate_series(1, ?) g), "
                + "j AS (INSERT INTO job (id, tenant_id, name, type, time_zone, run_at, payload, max_attempts, misfire_policy, partition_id) "
                + "  SELECT id, ?, ?, ?, 'UTC', due, ?::jsonb, ?, ?, (hashtext(id::text) & 2147483647) % 64 FROM s "
                + "  RETURNING id, tenant_id, type, payload, max_attempts, misfire_policy, partition_id, run_at) "
                + "INSERT INTO job_run (job_id, tenant_id, partition_id, type, payload, priority, max_run_seconds, max_attempts, misfire_policy, "
                + "  scheduled_at, run_at, status, idempotency_key) "
                + "SELECT id, tenant_id, partition_id, type, payload, 5, 60, max_attempts, misfire_policy, run_at, run_at, 'PENDING', "
                + "  tenant_id || ':' || id || ':' || (extract(epoch from run_at) * 1000000)::bigint FROM j",
                count, tenant, "seed-" + tag, type, payload, maxAttempts, policy);
        return tag;
    }

    protected long count(String sql, Object... args) {
        Long n = jdbc.queryForObject(sql, Long.class, args);
        return n == null ? 0 : n;
    }

    protected long succeeded(String tag) {
        return count("SELECT count(*) FROM job_run WHERE payload->>'bench' = ? AND status = 'SUCCEEDED'", tag);
    }

    protected long effects(String tag) {
        return count("SELECT count(*) FROM effect e JOIN job_run r ON r.id = e.run_id WHERE r.payload->>'bench' = ?", tag);
    }

    protected long distinctEffectKeys(String tag) {
        return count("SELECT count(DISTINCT e.idempotency_key) FROM effect e JOIN job_run r ON r.id = e.run_id WHERE r.payload->>'bench' = ?", tag);
    }
}
