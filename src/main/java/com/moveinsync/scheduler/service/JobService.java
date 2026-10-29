package com.moveinsync.scheduler.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.api.ApiException;
import com.moveinsync.scheduler.config.SchedulerProperties;
import com.moveinsync.scheduler.core.MisfirePolicy;
import com.moveinsync.scheduler.core.PartitionAssigner;
import com.moveinsync.scheduler.core.TenantCronSchedule;
import com.moveinsync.scheduler.engine.ScheduleCache;
import com.moveinsync.scheduler.handlers.JobHandler;
import com.moveinsync.scheduler.model.Models.Job;
import com.moveinsync.scheduler.model.Models.NewRun;
import com.moveinsync.scheduler.model.Models.RunView;
import com.moveinsync.scheduler.model.Models.Tenant;
import com.moveinsync.scheduler.security.Caller;
import com.moveinsync.scheduler.store.DbClock;
import com.moveinsync.scheduler.store.JobStore;
import com.moveinsync.scheduler.store.RunStore;
import com.moveinsync.scheduler.store.TenantStore;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class JobService {

    public static final int MAX_BULK = 5_000;

    public record CreateJob(String name, String type, String cron, String timeZone, Instant runAt,
                            Map<String, Object> payload, Integer priority, Integer maxRunSeconds,
                            Integer maxAttempts, MisfirePolicy misfirePolicy) {
    }

    private final JobStore jobs;
    private final RunStore runs;
    private final TenantStore tenants;
    private final DbClock clock;
    private final ScheduleCache schedules;
    private final SchedulerProperties props;
    private final ObjectMapper mapper;
    private final TransactionTemplate tx;
    private final Set<String> knownTypes;

    public JobService(JobStore jobs, RunStore runs, TenantStore tenants, DbClock clock, ScheduleCache schedules,
                      SchedulerProperties props, ObjectMapper mapper, TransactionTemplate tx, List<JobHandler> handlers) {
        this.jobs = jobs;
        this.runs = runs;
        this.tenants = tenants;
        this.clock = clock;
        this.schedules = schedules;
        this.props = props;
        this.mapper = mapper;
        this.tx = tx;
        this.knownTypes = handlers.stream().map(JobHandler::type).collect(Collectors.toUnmodifiableSet());
    }

    public Job create(Caller caller, String tenantParam, CreateJob req) {
        Tenant tenant = tenantFor(caller, tenantParam);
        Job job = build(tenant, clock.now(), req);
        tx.executeWithoutResult(s -> {
            jobs.insert(job);
            if (!job.recurring()) {
                runs.insertRuns(List.of(oneOffRun(job)));
            }
        });
        return job;
    }

    public List<Job> createBulk(Caller caller, String tenantParam, List<CreateJob> reqs) {
        if (reqs.size() > MAX_BULK) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "BULK_TOO_LARGE", "at most " + MAX_BULK + " jobs per request");
        }
        Tenant tenant = tenantFor(caller, tenantParam);
        Instant now = clock.now();            // one clock read and one tenant lookup for the whole batch
        List<Job> built = reqs.stream().map(r -> build(tenant, now, r)).toList();
        if (built.stream().anyMatch(Job::recurring)) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "BULK_ONE_OFF_ONLY", "bulk submission accepts one-off jobs only");
        }
        tx.executeWithoutResult(s -> {
            built.forEach(jobs::insert);
            runs.insertRuns(built.stream().map(this::oneOffRun).toList());
        });
        return built;
    }

    public Job get(Caller caller, String tenantParam, UUID id) {
        return jobs.find(id, caller.scope(tenantParam)).orElseThrow(ApiException::notFound);
    }

    public List<Job> list(Caller caller, String tenantParam, int limit, int offset) {
        return jobs.list(caller.scope(tenantParam), Math.min(Math.max(limit, 1), 200), Math.max(offset, 0));
    }

    public void deactivate(Caller caller, String tenantParam, UUID id) {
        caller.requireWriter();
        String tenantId = caller.scope(tenantParam);
        tx.executeWithoutResult(s -> {
            if (!jobs.deactivate(id, tenantId)) {
                throw ApiException.notFound();
            }
            runs.cancelPending(id, tenantId);      // queued / already-materialised runs must not fire after a delete
        });
    }

    public List<Instant> nextFires(Caller caller, String tenantParam, UUID id, int count) {
        Job job = get(caller, tenantParam, id);
        Instant now = clock.now();
        if (!job.recurring()) {
            // a one-off that already fired has no next fire time
            return job.active() && job.runAt() != null && job.runAt().isAfter(now) ? List.of(job.runAt()) : List.of();
        }
        return schedules.get(job.cron(), job.timeZone()).nextN(now, Math.min(Math.max(count, 1), 100));
    }

    /** Manual trigger: a distinct occurrence (scheduledAt = now, microsecond precision) with its own idempotency key. */
    public RunView trigger(Caller caller, String tenantParam, UUID id) {
        caller.requireWriter();
        Job job = get(caller, tenantParam, id);
        Instant now = clock.now();
        runs.insertRuns(List.of(new NewRun(job.id(), job.tenantId(), job.partitionId(), job.type(), job.payload(),
                job.priority(), job.maxRunSeconds(), job.maxAttempts(), job.misfirePolicy(), now, now, 1)));
        // look the run up by identity: the newest run by scheduled_at may be a cron occurrence materialised ahead of time
        return runs.findOccurrence(job.id(), job.tenantId(), now).orElseThrow(ApiException::notFound);
    }

    public List<RunView> runs(Caller caller, String tenantParam, UUID id, int limit) {
        Job job = get(caller, tenantParam, id);
        return runs.runsOfJob(job.id(), job.tenantId(), Math.min(Math.max(limit, 1), 200));
    }

    // ------------------------------------------------------------------------------------------------

    private Tenant tenantFor(Caller caller, String tenantParam) {
        caller.requireWriter();
        return tenants.find(caller.scope(tenantParam)).orElseThrow(ApiException::notFound);
    }

    private Job build(Tenant tenant, Instant now, CreateJob r) {
        if (r.name() == null || r.name().isBlank() || r.name().length() > 200) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_NAME", "name is required (max 200 chars)");
        }
        if (r.type() == null || !knownTypes.contains(r.type())) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "UNKNOWN_TYPE", "unknown job type; known: " + knownTypes.stream().sorted().toList());
        }
        boolean recurring = r.cron() != null && !r.cron().isBlank();
        if (recurring && r.runAt() != null) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "CRON_XOR_RUN_AT", "give either cron or runAt, not both");
        }
        String zone = r.timeZone() != null ? r.timeZone() : tenant.timeZone();
        ZoneId zoneId;
        try {
            zoneId = ZoneId.of(zone);
        } catch (DateTimeException e) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_TIME_ZONE", "unknown time zone");
        }
        UUID id = UUID.randomUUID();
        String cron = null;
        Instant nextFire = null;
        Instant runAt = null;
        if (recurring) {
            try {
                TenantCronSchedule schedule = TenantCronSchedule.of(r.cron(), zoneId);
                cron = schedule.expression();
                nextFire = schedule.next(now).orElseThrow(() -> new ApiException(HttpStatus.BAD_REQUEST, "CRON_NEVER_FIRES", "cron never fires"));
            } catch (IllegalArgumentException e) {
                throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_CRON", "invalid cron expression (6 fields: sec min hour dom month dow)");
            }
        } else {
            runAt = r.runAt() != null ? r.runAt() : now;
        }
        MisfirePolicy policy = r.misfirePolicy() != null ? r.misfirePolicy() : (recurring ? MisfirePolicy.COALESCE : MisfirePolicy.FIRE_NOW);
        int attempts = r.maxAttempts() == null ? 5 : r.maxAttempts();
        int maxRun = r.maxRunSeconds() == null ? 60 : r.maxRunSeconds();
        if (attempts < 1 || attempts > 20 || maxRun < 1 || maxRun > 3600) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_LIMITS", "maxAttempts 1..20, maxRunSeconds 1..3600");
        }
        String payload;
        try {
            payload = mapper.writeValueAsString(r.payload() == null ? Map.of() : r.payload());
        } catch (JsonProcessingException e) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_PAYLOAD", "payload is not valid JSON");
        }
        if (payload.length() > 16_384) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "PAYLOAD_TOO_LARGE", "payload limited to 16 KiB");
        }
        return new Job(id, tenant.id(), r.name(), r.type(), cron, zoneId.getId(), runAt, payload,
                r.priority() == null ? 5 : r.priority(), maxRun, attempts, policy,
                PartitionAssigner.partitionOf(id, props.partitions()), nextFire, true, now);
    }

    private NewRun oneOffRun(Job j) {
        return new NewRun(j.id(), j.tenantId(), j.partitionId(), j.type(), j.payload(), j.priority(), j.maxRunSeconds(),
                j.maxAttempts(), j.misfirePolicy(), j.runAt(), j.runAt(), 1);
    }
}
