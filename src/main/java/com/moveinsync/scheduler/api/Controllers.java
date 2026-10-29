package com.moveinsync.scheduler.api;

import com.moveinsync.scheduler.model.Models.Job;
import com.moveinsync.scheduler.model.Models.RunView;
import com.moveinsync.scheduler.model.Models.Tenant;
import com.moveinsync.scheduler.security.Caller;
import com.moveinsync.scheduler.service.BenchService;
import com.moveinsync.scheduler.service.JobService;
import com.moveinsync.scheduler.service.JobService.CreateJob;
import com.moveinsync.scheduler.service.OpsService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** HTTP layer only: parse, delegate, shape the response. All rules live in the services. */
public final class Controllers {

    private Controllers() {
    }

    @RestController
    @RequestMapping("/api/jobs")
    public static class JobController {
        private final JobService jobs;

        public JobController(JobService jobs) {
            this.jobs = jobs;
        }

        @PostMapping
        @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
        public ResponseEntity<Job> create(@AuthenticationPrincipal Caller caller, @RequestParam(required = false) String tenant,
                                          @RequestBody CreateJob body) {
            return ResponseEntity.status(HttpStatus.CREATED).body(jobs.create(caller, tenant, body));
        }

        @PostMapping("/bulk")
        @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
        public ResponseEntity<Map<String, Object>> bulk(@AuthenticationPrincipal Caller caller, @RequestParam(required = false) String tenant,
                                                        @RequestBody List<CreateJob> body) {
            List<Job> created = jobs.createBulk(caller, tenant, body);
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("created", created.size()));
        }

        @GetMapping
        public List<Job> list(@AuthenticationPrincipal Caller caller, @RequestParam(required = false) String tenant,
                              @RequestParam(defaultValue = "50") int limit, @RequestParam(defaultValue = "0") int offset) {
            return jobs.list(caller, tenant, limit, offset);
        }

        @GetMapping("/{id}")
        public Job get(@AuthenticationPrincipal Caller caller, @RequestParam(required = false) String tenant, @PathVariable UUID id) {
            return jobs.get(caller, tenant, id);
        }

        @GetMapping("/{id}/next-fires")
        public Map<String, Object> nextFires(@AuthenticationPrincipal Caller caller, @RequestParam(required = false) String tenant,
                                             @PathVariable UUID id, @RequestParam(defaultValue = "5") int count) {
            Job job = jobs.get(caller, tenant, id);
            List<Instant> fires = jobs.nextFires(caller, tenant, id, count);
            return Map.of("jobId", id, "timeZone", job.timeZone(), "nextFires", fires);
        }

        @GetMapping("/{id}/runs")
        public List<RunView> runs(@AuthenticationPrincipal Caller caller, @RequestParam(required = false) String tenant,
                                  @PathVariable UUID id, @RequestParam(defaultValue = "20") int limit) {
            return jobs.runs(caller, tenant, id, limit);
        }

        @PostMapping("/{id}/trigger")
        @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
        public ResponseEntity<RunView> trigger(@AuthenticationPrincipal Caller caller, @RequestParam(required = false) String tenant,
                                               @PathVariable UUID id) {
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(jobs.trigger(caller, tenant, id));
        }

        @DeleteMapping("/{id}")
        @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
        public ResponseEntity<Void> delete(@AuthenticationPrincipal Caller caller, @RequestParam(required = false) String tenant,
                                           @PathVariable UUID id) {
            jobs.deactivate(caller, tenant, id);
            return ResponseEntity.noContent().build();
        }
    }

    public record TenantRequest(String id, String name, String timeZone, Integer maxConcurrency) {
    }

    public record ConcurrencyRequest(int maxConcurrency) {
    }

    @RestController
    @RequestMapping("/api/tenants")
    public static class TenantController {
        private final OpsService ops;

        public TenantController(OpsService ops) {
            this.ops = ops;
        }

        @PostMapping
        @PreAuthorize("hasRole('OPERATOR')")
        public ResponseEntity<Tenant> create(@RequestBody TenantRequest r) {
            if (r.id() == null || !r.id().matches("[a-z0-9][a-z0-9-]{1,62}")) {
                throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_TENANT_ID", "tenant id must be lowercase alphanumeric/dash, 2-63 chars");
            }
            return ResponseEntity.status(HttpStatus.CREATED).body(ops.createTenant(r.id(), r.name() == null ? r.id() : r.name(),
                    r.timeZone() == null ? "UTC" : r.timeZone(), r.maxConcurrency() == null ? 100 : r.maxConcurrency()));
        }

        @GetMapping
        @PreAuthorize("hasRole('OPERATOR')")
        public List<Tenant> list() {
            return ops.listTenants();
        }

        @PostMapping("/{id}/pause")
        @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
        public Tenant pause(@AuthenticationPrincipal Caller caller, @PathVariable String id) {
            return ops.setPaused(caller, id, true);
        }

        @PostMapping("/{id}/resume")
        @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
        public Tenant resume(@AuthenticationPrincipal Caller caller, @PathVariable String id) {
            return ops.setPaused(caller, id, false);
        }

        @PutMapping("/{id}/concurrency")
        @PreAuthorize("hasRole('OPERATOR')")
        public Tenant concurrency(@PathVariable String id, @RequestBody ConcurrencyRequest r) {
            return ops.setConcurrency(id, r.maxConcurrency());
        }
    }

    @RestController
    @RequestMapping("/api/dead-letters")
    public static class DeadLetterController {
        private final OpsService ops;

        public DeadLetterController(OpsService ops) {
            this.ops = ops;
        }

        @GetMapping
        @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
        public List<RunView> list(@AuthenticationPrincipal Caller caller, @RequestParam(required = false) String tenant,
                                  @RequestParam(defaultValue = "100") int limit) {
            return ops.deadLetters(caller, tenant, limit);
        }

        @PostMapping("/{runId}/replay")
        @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
        public ResponseEntity<Void> replay(@AuthenticationPrincipal Caller caller, @RequestParam(required = false) String tenant,
                                           @PathVariable long runId) {
            ops.replay(caller, tenant, runId);
            return ResponseEntity.accepted().build();
        }
    }

    @RestController
    @RequestMapping("/api/ops")
    @PreAuthorize("hasRole('OPERATOR')")
    public static class OpsController {
        private final OpsService ops;

        public OpsController(OpsService ops) {
            this.ops = ops;
        }

        @GetMapping("/summary")
        public Map<String, Object> summary() {
            return ops.summary();
        }
    }

    @RestController
    @RequestMapping("/api/bench")
    @PreAuthorize("hasRole('OPERATOR')")
    public static class BenchController {
        private final BenchService bench;

        public BenchController(BenchService bench) {
            this.bench = bench;
        }

        @PostMapping("/seed")
        public Map<String, Object> seed(@RequestParam int count, @RequestParam(defaultValue = "600") int windowSeconds,
                                        @RequestParam(defaultValue = "10") int tenants, @RequestParam(defaultValue = "5") int workMillis,
                                        @RequestParam(defaultValue = "15") int startDelaySeconds) {
            return bench.seed(count, windowSeconds, tenants, workMillis, startDelaySeconds);
        }

        @GetMapping("/report")
        public Map<String, Object> report(@RequestParam String batch) {
            return bench.report(batch);
        }
    }
}
