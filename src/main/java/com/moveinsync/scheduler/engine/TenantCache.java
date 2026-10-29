package com.moveinsync.scheduler.engine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.moveinsync.scheduler.model.Models.Tenant;
import com.moveinsync.scheduler.store.TenantStore;

import java.time.Duration;
import java.util.List;

/**
 * Hot read-mostly view of tenants used by the claim loop (read ~10x per second per node).
 *
 * <p>Staleness: a changed concurrency cap or a new tenant is seen by a node within {@code ttl} (default 5 s) at most;
 * local API writes invalidate immediately. The <b>paused</b> flag is deliberately NOT trusted from this cache: the
 * claim statement re-checks it in SQL, so a pause takes effect for new claims immediately cluster-wide.
 */
public class TenantCache {

    private final TenantStore store;
    private final Cache<String, List<Tenant>> cache;

    public TenantCache(TenantStore store, Duration ttl) {
        this.store = store;
        this.cache = Caffeine.newBuilder().expireAfterWrite(ttl).maximumSize(1).build();
    }

    public List<Tenant> all() {
        return cache.get("all", k -> store.all());
    }

    public void invalidate() {
        cache.invalidateAll();
    }
}
