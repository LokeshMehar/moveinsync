package com.moveinsync.scheduler.security;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.List;

@ConfigurationProperties("security")
public record SecurityProps(
        @DefaultValue("dev-only-secret-change-me-dev-only-secret-change-me") String jwtSecret,
        @DefaultValue("8h") Duration jwtTtl,
        @DefaultValue List<SeedTenant> seedTenants,
        @DefaultValue List<SeedUser> seedUsers) {

    public record SeedTenant(String id, String name, String timeZone, int maxConcurrency) {
    }

    public record SeedUser(String username, String password, String role, String tenant) {
    }
}
