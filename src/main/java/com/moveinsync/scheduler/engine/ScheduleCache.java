package com.moveinsync.scheduler.engine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.moveinsync.scheduler.core.TenantCronSchedule;

import java.time.ZoneId;

/**
 * Parsed cron expressions are immutable functions of (expression, zone), so this cache can never be stale; it is
 * bounded by size only (Caffeine's W-TinyLFU evicts the coldest entries). Zone rule changes (tzdata updates) are
 * picked up on restart because {@link ZoneId} rules are loaded by the JVM.
 */
public class ScheduleCache {

    private final Cache<String, TenantCronSchedule> cache = Caffeine.newBuilder().maximumSize(10_000).build();

    public TenantCronSchedule get(String cron, String zone) {
        return cache.get(cron + "|" + zone, k -> TenantCronSchedule.of(cron, ZoneId.of(zone)));
    }
}
