package com.moveinsync.scheduler.service;

import com.moveinsync.scheduler.api.ApiException;
import com.moveinsync.scheduler.engine.SchedulerNode;
import com.moveinsync.scheduler.model.Models.RunView;
import com.moveinsync.scheduler.model.Models.Tenant;
import com.moveinsync.scheduler.security.Caller;
import com.moveinsync.scheduler.store.ClusterStore;
import com.moveinsync.scheduler.store.RunStore;
import com.moveinsync.scheduler.store.TenantStore;
import org.springframework.http.HttpStatus;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Tenant controls, dead-letter handling and the operator's cluster view. */
public class OpsService {

    private final TenantStore tenants;
    private final RunStore runs;
    private final ClusterStore cluster;
    private final SchedulerNode node;

    public OpsService(TenantStore tenants, RunStore runs, ClusterStore cluster, SchedulerNode node) {
        this.tenants = tenants;
        this.runs = runs;
        this.cluster = cluster;
        this.node = node;
    }

    public Tenant createTenant(String id, String name, String timeZone, int maxConcurrency) {
        requireValidCap(maxConcurrency);
        try {
            ZoneId.of(timeZone);
        } catch (DateTimeException e) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_TIME_ZONE", "unknown time zone");
        }
        if (!tenants.insertIfAbsent(id, name, timeZone, maxConcurrency)) {
            throw new ApiException(HttpStatus.CONFLICT, "TENANT_EXISTS", "tenant already exists");
        }
        node.tenantCache().invalidate();
        return tenants.find(id).orElseThrow(ApiException::notFound);
    }

    private static void requireValidCap(int max) {
        if (max < 1 || max > 100_000) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_LIMIT", "maxConcurrency must be 1..100000");
        }
    }

    public List<Tenant> listTenants() {
        return tenants.all();
    }

    /** Pause/resume every job of a tenant. Takes effect for new claims on all nodes immediately (checked in SQL). */
    public Tenant setPaused(Caller caller, String tenantParam, boolean paused) {
        caller.requireWriter();
        String id = caller.scope(tenantParam);
        if (!tenants.setPaused(id, paused)) {
            throw ApiException.notFound();
        }
        node.tenantCache().invalidate();
        return tenants.find(id).orElseThrow(ApiException::notFound);
    }

    public Tenant setConcurrency(String tenantId, int max) {
        requireValidCap(max);
        if (!tenants.setMaxConcurrency(tenantId, max)) {
            throw ApiException.notFound();
        }
        node.tenantCache().invalidate();
        return tenants.find(tenantId).orElseThrow(ApiException::notFound);
    }

    public List<RunView> deadLetters(Caller caller, String tenantParam, int limit) {
        return runs.deadLetters(caller.scope(tenantParam), Math.min(Math.max(limit, 1), 500));
    }

    public void replay(Caller caller, String tenantParam, long runId) {
        caller.requireWriter();
        if (!runs.replayDead(runId, caller.scope(tenantParam))) {
            throw ApiException.notFound();
        }
    }

    public Map<String, Object> summary() {
        Map<String, Object> out = new TreeMap<>();
        out.put("runsByStatus", runs.countsByStatus(null));
        out.put("retrying", runs.retryingCount());
        out.put("oldestDueAgeMillis", runs.oldestDueAgeMillis());
        out.put("liveNodes", cluster.liveNodes(Duration.ofSeconds(6)));
        Map<String, Integer> perNode = new TreeMap<>();
        cluster.ownership().values().forEach(o -> perNode.merge(o, 1, Integer::sum));
        out.put("partitionsPerNode", perNode);
        out.put("servingNode", node.nodeId());
        return out;
    }
}
