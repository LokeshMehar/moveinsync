package com.moveinsync.scheduler.engine;

import com.moveinsync.scheduler.config.SchedulerProperties;
import com.moveinsync.scheduler.core.MisfireResolver;
import com.moveinsync.scheduler.core.TenantCronSchedule;
import com.moveinsync.scheduler.model.Models.Job;
import com.moveinsync.scheduler.model.Models.NewRun;
import com.moveinsync.scheduler.store.DbClock;
import com.moveinsync.scheduler.store.JobStore;
import com.moveinsync.scheduler.store.RunStore;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns cron schedules into concrete, idempotently-keyed run rows a little ahead of time (the horizon), so a run is
 * claimable exactly when it is due. Job rows are locked with SKIP LOCKED and only for owned partitions, so nodes
 * never wait on each other; the (job_id, scheduled_at) unique key makes a double materialisation harmless.
 */
public class Materializer {

    private static final int JOB_BATCH = 200;
    private static final int MAX_OCCURRENCES_PER_JOB = 5_000;

    private final SchedulerProperties props;
    private final JobStore jobs;
    private final RunStore runs;
    private final DbClock clock;
    private final ScheduleCache schedules;
    private final MisfireResolver resolver;
    private final TransactionTemplate tx;
    private final ClusterMembership membership;
    private final SchedulerMetrics metrics;

    public Materializer(SchedulerProperties props, JobStore jobs, RunStore runs, DbClock clock, ScheduleCache schedules,
                        TransactionTemplate tx, ClusterMembership membership, SchedulerMetrics metrics) {
        this.props = props;
        this.jobs = jobs;
        this.runs = runs;
        this.clock = clock;
        this.schedules = schedules;
        this.resolver = new MisfireResolver(props.misfireGrace(), props.maxCatchUp());
        this.tx = tx;
        this.membership = membership;
        this.metrics = metrics;
    }

    /** @return jobs processed in this pass (the caller loops while a full batch comes back) */
    public int tick() {
        if (!membership.valid() || membership.ownedCsv().isEmpty()) {
            return 0;
        }
        Integer processed = tx.execute(status -> {
            Instant now = clock.now();
            List<Job> due = jobs.lockDueRecurring(membership.ownedCsv(), now.plus(props.materializeHorizon()), JOB_BATCH);
            for (Job job : due) {
                materialize(job, now);
            }
            return due.size();
        });
        return processed == null ? 0 : processed;
    }

    private void materialize(Job job, Instant now) {
        TenantCronSchedule schedule = schedules.get(job.cron(), job.timeZone());
        Instant horizonEnd = now.plus(props.materializeHorizon());
        List<Instant> occurrences = schedule.between(job.nextFireAt(), horizonEnd, MAX_OCCURRENCES_PER_JOB);

        List<Instant> due = new ArrayList<>();
        List<Instant> upcoming = new ArrayList<>();
        for (Instant occurrence : occurrences) {
            (occurrence.isAfter(now) ? upcoming : due).add(occurrence);
        }
        MisfireResolver.Plan plan = resolver.resolve(due, job.misfirePolicy(), now);

        List<NewRun> toCreate = new ArrayList<>();
        plan.runs().forEach(p -> toCreate.add(newRun(job, p.scheduledAt(), p.coalescedCount())));
        upcoming.forEach(i -> toCreate.add(newRun(job, i, 1)));
        runs.insertRuns(toCreate);
        metrics.materialized.increment(toCreate.size());
        metrics.misfireSkipped.increment(plan.skipped());

        Instant anchor = occurrences.isEmpty() ? job.nextFireAt() : occurrences.get(occurrences.size() - 1);
        jobs.setNextFire(job.id(), schedule.next(anchor).orElse(null));
    }

    private NewRun newRun(Job job, Instant scheduledAt, int coalesced) {
        return new NewRun(job.id(), job.tenantId(), job.partitionId(), job.type(), job.payload(), job.priority(),
                job.maxRunSeconds(), job.maxAttempts(), job.misfirePolicy(), scheduledAt, scheduledAt, coalesced);
    }
}
