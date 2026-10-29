package com.moveinsync.scheduler.store;

import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.time.OffsetDateTime;

/** The single time authority of the cluster: the database server clock. Node clocks are never used to decide anything. */
public class DbClock {

    private final JdbcTemplate jdbc;

    public DbClock(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Instant now() {
        return jdbc.queryForObject("SELECT clock_timestamp()", OffsetDateTime.class).toInstant();
    }
}
