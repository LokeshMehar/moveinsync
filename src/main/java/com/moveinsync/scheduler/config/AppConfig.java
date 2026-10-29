package com.moveinsync.scheduler.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.engine.ScheduleCache;
import com.moveinsync.scheduler.engine.SchedulerNode;
import com.moveinsync.scheduler.handlers.JobHandler;
import com.moveinsync.scheduler.handlers.TripHandlers;
import com.moveinsync.scheduler.security.JwtService;
import com.moveinsync.scheduler.security.SecurityProps;
import com.moveinsync.scheduler.service.BenchService;
import com.moveinsync.scheduler.service.JobService;
import com.moveinsync.scheduler.service.OpsService;
import com.moveinsync.scheduler.store.ClusterStore;
import com.moveinsync.scheduler.store.DbClock;
import com.moveinsync.scheduler.store.JobStore;
import com.moveinsync.scheduler.store.RunStore;
import com.moveinsync.scheduler.store.TenantStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.util.List;
import java.util.UUID;

@Configuration
@EnableConfigurationProperties({SchedulerProperties.class, SecurityProps.class})
public class AppConfig {

    private static String defaultNodeId() {
        try {
            return InetAddress.getLocalHost().getHostName() + "-" + UUID.randomUUID().toString().substring(0, 4);
        } catch (Exception e) {
            return "node-" + UUID.randomUUID().toString().substring(0, 8);
        }
    }

    @Bean PlatformTransactionManager txManager(DataSource ds) { return new DataSourceTransactionManager(ds); }
    @Bean TransactionTemplate transactionTemplate(PlatformTransactionManager tm) { return new TransactionTemplate(tm); }
    @Bean DbClock dbClock(JdbcTemplate jdbc) { return new DbClock(jdbc); }
    @Bean TenantStore tenantStore(JdbcTemplate jdbc) { return new TenantStore(jdbc); }
    @Bean JobStore jobStore(JdbcTemplate jdbc) { return new JobStore(jdbc); }
    @Bean RunStore runStore(JdbcTemplate jdbc, TransactionTemplate tx) { return new RunStore(jdbc, tx); }
    @Bean ClusterStore clusterStore(JdbcTemplate jdbc) { return new ClusterStore(jdbc); }
    @Bean ScheduleCache scheduleCache() { return new ScheduleCache(); }

    @Bean List<JobHandler> jobHandlers() {
        return List.of(new TripHandlers.PickupAlertHandler(), new TripHandlers.RosterGenerationHandler(),
                new TripHandlers.NoShowCancelHandler(), new TripHandlers.BillingCloseHandler(), new TripHandlers.FlakyHandler());
    }

    @Bean
    SchedulerNode schedulerNode(DataSource ds, SchedulerProperties props, MeterRegistry registry, List<JobHandler> handlers, ObjectMapper mapper) {
        SchedulerProperties resolved = props.nodeId().isBlank() ? props.withNodeId(defaultNodeId()) : props;
        return new SchedulerNode(ds, resolved, registry, handlers, mapper);
    }

    /** Starts the node after the schema is migrated and the seed data exists; drains and leaves on shutdown. */
    @Bean
    SmartLifecycle nodeLifecycle(SchedulerNode node) {
        return new SmartLifecycle() {
            private volatile boolean running;
            @Override public void start() { node.start(); running = true; }
            @Override public void stop() { node.stop(); running = false; }
            @Override public boolean isRunning() { return running; }
            @Override public int getPhase() { return Integer.MAX_VALUE; }
        };
    }

    @Bean
    JobService jobService(JobStore jobs, RunStore runs, TenantStore tenants, DbClock clock, ScheduleCache schedules,
                          SchedulerProperties props, ObjectMapper mapper, TransactionTemplate tx, List<JobHandler> handlers) {
        return new JobService(jobs, runs, tenants, clock, schedules, props, mapper, tx, handlers);
    }

    @Bean OpsService opsService(TenantStore tenants, RunStore runs, ClusterStore cluster, SchedulerNode node) {
        return new OpsService(tenants, runs, cluster, node);
    }

    @Bean BenchService benchService(JdbcTemplate jdbc, SchedulerProperties props) { return new BenchService(jdbc, props); }
    @Bean
    JwtService jwtService(SecurityProps props, Environment env) {
        // the shipped default secret is public (it is in this repo): never let a prod deployment sign tokens with it
        if (env.acceptsProfiles(Profiles.of("prod")) && props.jwtSecret().startsWith("dev-only")) {
            throw new IllegalStateException("JWT_SECRET must be set when running with the prod profile");
        }
        return new JwtService(props);
    }
}
