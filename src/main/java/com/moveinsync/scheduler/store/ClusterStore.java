package com.moveinsync.scheduler.store;

import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Node heartbeats and per-partition ownership leases (one row per partition, so nodes never fight over one row). */
public class ClusterStore {

    private final JdbcTemplate jdbc;

    public ClusterStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void ensurePartitions(int partitions) {
        jdbc.update("INSERT INTO partition_lease (partition_id) SELECT g FROM generate_series(0, ? - 1) g "
                + "ON CONFLICT DO NOTHING", partitions);
    }

    public void heartbeat(String nodeId) {
        jdbc.update("INSERT INTO node (node_id, last_heartbeat) VALUES (?, clock_timestamp()) "
                + "ON CONFLICT (node_id) DO UPDATE SET last_heartbeat = clock_timestamp()", nodeId);
    }

    public List<String> liveNodes(Duration ttl) {
        return jdbc.queryForList("SELECT node_id FROM node WHERE last_heartbeat > clock_timestamp() - (? * interval '1 millisecond') "
                + "ORDER BY node_id", String.class, ttl.toMillis());
    }

    public void removeNode(String nodeId) {
        jdbc.update("DELETE FROM node WHERE node_id = ?", nodeId);
    }

    /** Gives back partitions this node no longer deserves so the rightful owner can take them immediately. */
    public int release(String nodeId, Collection<Integer> keep) {
        return jdbc.update("UPDATE partition_lease SET owner = NULL, expires_at = '-infinity' WHERE owner = ? "
                        + "AND NOT (partition_id = ANY (string_to_array(?, ',')::int[]))",
                nodeId, csv(keep));
    }

    public void releaseAll(String nodeId) {
        jdbc.update("UPDATE partition_lease SET owner = NULL, expires_at = '-infinity' WHERE owner = ?", nodeId);
    }

    /**
     * Renews partitions already owned and acquires wanted ones that are free, expired, or owned by a node that is no
     * longer live. Returns the partitions this node owns after the call.
     */
    public Set<Integer> renewAndAcquire(String nodeId, Collection<Integer> desired, Duration ttl, Duration nodeTtl) {
        List<Integer> owned = jdbc.queryForList("""
                UPDATE partition_lease p SET owner = ?, epoch = CASE WHEN p.owner = ? THEN p.epoch ELSE p.epoch + 1 END,
                       expires_at = clock_timestamp() + (? * interval '1 millisecond')
                WHERE p.partition_id = ANY (string_to_array(?, ',')::int[])
                  AND (p.owner IS NULL OR p.owner = ? OR p.expires_at < clock_timestamp()
                       OR NOT EXISTS (SELECT 1 FROM node n WHERE n.node_id = p.owner
                                      AND n.last_heartbeat > clock_timestamp() - (? * interval '1 millisecond')))
                RETURNING p.partition_id
                """, Integer.class, nodeId, nodeId, ttl.toMillis(), csv(desired), nodeId, nodeTtl.toMillis());
        return Set.copyOf(owned);
    }

    public Map<Integer, String> ownership() {
        return jdbc.query("SELECT partition_id, owner FROM partition_lease WHERE owner IS NOT NULL AND expires_at > clock_timestamp()",
                rs -> {
                    Map<Integer, String> m = new java.util.TreeMap<>();
                    while (rs.next()) {
                        m.put(rs.getInt(1), rs.getString(2));
                    }
                    return m;
                });
    }

    public static String csv(Collection<Integer> ids) {
        return ids.stream().map(String::valueOf).collect(Collectors.joining(","));
    }
}
