package com.moveinsync.scheduler.store;

import com.moveinsync.scheduler.model.Models.Tenant;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;
import java.util.Optional;

public class TenantStore {

    private static final RowMapper<Tenant> MAPPER = (rs, i) -> new Tenant(rs.getString("id"), rs.getString("name"),
            rs.getString("time_zone"), rs.getInt("max_concurrency"), rs.getBoolean("paused"));

    private final JdbcTemplate jdbc;

    public TenantStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Never overwrites: an operator's tuned cap or zone must survive restarts and repeated calls. */
    public boolean insertIfAbsent(String id, String name, String timeZone, int maxConcurrency) {
        return jdbc.update("INSERT INTO tenant (id, name, time_zone, max_concurrency) VALUES (?, ?, ?, ?) ON CONFLICT (id) DO NOTHING",
                id, name, timeZone, maxConcurrency) == 1;
    }

    public Optional<Tenant> find(String id) {
        return jdbc.query("SELECT * FROM tenant WHERE id = ?", MAPPER, id).stream().findFirst();
    }

    public List<Tenant> all() {
        return jdbc.query("SELECT * FROM tenant ORDER BY id", MAPPER);
    }

    public boolean setPaused(String id, boolean paused) {
        return jdbc.update("UPDATE tenant SET paused = ? WHERE id = ?", paused, id) > 0;
    }

    public boolean setMaxConcurrency(String id, int max) {
        return jdbc.update("UPDATE tenant SET max_concurrency = ? WHERE id = ?", max, id) > 0;
    }
}
