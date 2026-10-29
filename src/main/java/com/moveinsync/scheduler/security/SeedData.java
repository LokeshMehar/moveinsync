package com.moveinsync.scheduler.security;

import com.moveinsync.scheduler.store.TenantStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

/**
 * Creates the demo tenants and users from configuration, only where they do not exist yet. Three nodes start at the
 * same moment, so every statement here must be safe to run concurrently and must never overwrite what an operator
 * has changed since (caps, zones). Disabled under the prod profile: real users are provisioned elsewhere.
 */
@Component
public class SeedData implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(SeedData.class);

    private final SecurityProps props;
    private final TenantStore tenants;
    private final JdbcTemplate jdbc;
    private final PasswordEncoder encoder;
    private final Environment env;

    public SeedData(SecurityProps props, TenantStore tenants, JdbcTemplate jdbc, PasswordEncoder encoder, Environment env) {
        this.props = props;
        this.tenants = tenants;
        this.jdbc = jdbc;
        this.encoder = encoder;
        this.env = env;
    }

    @Override
    public void afterPropertiesSet() {
        if (env.acceptsProfiles(Profiles.of("prod"))) {
            log.info("prod profile: demo tenants and users are not seeded");
            return;
        }
        props.seedTenants().forEach(t -> tenants.insertIfAbsent(t.id(), t.name(), t.timeZone(), t.maxConcurrency()));
        props.seedUsers().forEach(u -> jdbc.update(
                "INSERT INTO app_user (username, password_hash, role, tenant_id) VALUES (?, ?, ?, ?) ON CONFLICT (username) DO NOTHING",
                u.username(), encoder.encode(u.password()), u.role(), u.tenant()));
    }
}
