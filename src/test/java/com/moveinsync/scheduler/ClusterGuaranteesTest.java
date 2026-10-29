package com.moveinsync.scheduler;

import com.moveinsync.scheduler.engine.SchedulerNode;
import com.moveinsync.scheduler.handlers.SimulatedWorkHandler;
import com.moveinsync.scheduler.model.Models.ClaimedRun;
import com.moveinsync.scheduler.model.Models.Effect;
import com.moveinsync.scheduler.store.ClusterStore;
import com.moveinsync.scheduler.security.SecurityProps;
import com.moveinsync.scheduler.security.SeedData;
import com.moveinsync.scheduler.store.RunStore;
import com.moveinsync.scheduler.store.TenantStore;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** The hard rules of the case study, each proven against a real PostgreSQL with several nodes in one JVM. */
class ClusterGuaranteesTest extends PgTestBase {

    // ------------------------------------------------------------------------------------------ exactly once

    @Test
    void threeNodesFireEveryJobExactlyOnce_noLoss_noDuplicates() {
        SchedulerNode a = startNode("a"), b = startNode("b"), c = startNode("c");
        awaitBalanced(64, a, b, c);
        long invocationsBefore = SimulatedWorkHandler.INVOCATIONS.get();

        String tag = seed("acme", 6000, "PICKUP_ALERT", "\"workMillis\":2", 5, "FIRE_NOW", "clock_timestamp()");

        await().atMost(Duration.ofSeconds(60)).until(() -> succeeded(tag) == 6000);
        assertThat(effects(tag)).isEqualTo(6000);                 // one effect per job
        assertThat(distinctEffectKeys(tag)).isEqualTo(6000);       // no key used twice
        assertThat(count("SELECT count(*) FROM job_run WHERE payload->>'bench' = ? AND attempts > 1", tag)).isZero();
        assertThat(SimulatedWorkHandler.INVOCATIONS.get() - invocationsBefore).isEqualTo(6000);
        // work really was spread over the cluster (no single hot node)
        long nodesUsed = count("SELECT count(DISTINCT lease_owner) FROM job_run WHERE payload->>'bench' = ?", tag);
        assertThat(nodesUsed).isEqualTo(3);
    }

    @Test
    void concurrentSubmitOfTheSameOccurrenceIsIdempotent() {
        String tag = seed("acme", 1, "PICKUP_ALERT", "", 5, "FIRE_NOW", "clock_timestamp() + interval '1 hour'");
        // Re-inserting the same (job, scheduled_at) from many threads must create nothing new.
        var row = jdbc.queryForMap("SELECT job_id, scheduled_at FROM job_run WHERE payload->>'bench' = ?", tag);
        AtomicInteger inserted = new AtomicInteger();
        List<Thread> threads = java.util.stream.IntStream.range(0, 16).mapToObj(i -> Thread.ofPlatform().start(() ->
                inserted.addAndGet(jdbc.update("INSERT INTO job_run (job_id, tenant_id, partition_id, type, payload, priority, max_run_seconds, "
                        + "max_attempts, misfire_policy, scheduled_at, run_at, status, idempotency_key) SELECT job_id, tenant_id, partition_id, type, "
                        + "payload, priority, max_run_seconds, max_attempts, misfire_policy, scheduled_at, run_at, 'PENDING', idempotency_key "
                        + "FROM job_run WHERE job_id = ? ON CONFLICT (job_id, scheduled_at) DO NOTHING", row.get("job_id"))))).toList();
        threads.forEach(t -> { try { t.join(); } catch (InterruptedException e) { throw new RuntimeException(e); } });
        assertThat(inserted.get()).isZero();
        assertThat(count("SELECT count(*) FROM job_run WHERE job_id = ?", row.get("job_id"))).isEqualTo(1);
    }

    // ------------------------------------------------------------------------------------------ fencing

    @Test
    void staleLeaseHolderCannotCommit_fencingTokenRejectsItsWrite() throws Exception {
        String tag = seed("acme", 1, "PICKUP_ALERT", "", 5, "FIRE_NOW", "clock_timestamp()");
        var tx = new TransactionTemplate(new DataSourceTransactionManager(DS));
        RunStore store = new RunStore(jdbc, tx);
        String parts = ClusterStore.csv(java.util.stream.IntStream.range(0, 64).boxed().toList());

        ClaimedRun first = store.claim("acme", 100, parts, 10, "node-A", Duration.ofMillis(400)).get(0);   // A leases for 400 ms
        Thread.sleep(700);                                                                                     // A "pauses" past its lease
        ClaimedRun second = store.claim("acme", 100, parts, 10, "node-B", Duration.ofSeconds(30)).get(0);     // B takes over
        assertThat(second.takeover()).isTrue();
        assertThat(second.fence()).isGreaterThan(first.fence());                                               // monotonic token

        // A wakes up and tries to commit its (stale) result: rejected, and it leaves NO effect behind.
        var stale = store.complete(first, List.of(new Effect("ALERT", "from-A")));
        assertThat(stale.result()).isEqualTo(RunStore.CommitResult.FENCED_OUT);
        assertThat(effects(tag)).isZero();
        // ... and cannot renew or fail the run either
        assertThat(store.renew(first.id(), first.fence(), Duration.ofSeconds(30))).isFalse();
        assertThat(store.scheduleRetry(first, Duration.ZERO, "x")).isFalse();

        var ok = store.complete(second, List.of(new Effect("ALERT", "from-B")));
        assertThat(ok.committed()).isTrue();
        assertThat(effects(tag)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT detail FROM effect", String.class)).isEqualTo("from-B");
    }

    @Test
    void expiredLeaseHolderCannotCommitEvenIfNobodyTookOverYet() throws Exception {
        seed("acme", 1, "PICKUP_ALERT", "", 5, "FIRE_NOW", "clock_timestamp()");
        RunStore store = new RunStore(jdbc, new TransactionTemplate(new DataSourceTransactionManager(DS)));
        String parts = ClusterStore.csv(java.util.stream.IntStream.range(0, 64).boxed().toList());
        ClaimedRun run = store.claim("acme", 100, parts, 1, "node-A", Duration.ofMillis(200)).get(0);
        Thread.sleep(400);
        assertThat(store.complete(run, List.of(new Effect("ALERT", "late"))).committed()).isFalse();
    }

    @Test
    void duplicateEffectKeyIsAbsorbed_atLeastOnceExecutionStillGivesOneEffect() {
        String tag = seed("acme", 1, "PICKUP_ALERT", "", 5, "FIRE_NOW", "clock_timestamp()");
        RunStore store = new RunStore(jdbc, new TransactionTemplate(new DataSourceTransactionManager(DS)));
        String parts = ClusterStore.csv(java.util.stream.IntStream.range(0, 64).boxed().toList());
        ClaimedRun run = store.claim("acme", 100, parts, 1, "node-A", Duration.ofSeconds(30)).get(0);
        // The effect of this occurrence already exists (e.g. committed by an earlier execution that was then replayed).
        jdbc.update("INSERT INTO effect (idempotency_key, tenant_id, run_id, kind, detail, fence) VALUES (?, 'acme', ?, 'ALERT', 'first', 1)",
                run.idempotencyKey() + "#0", run.id());
        var commit = store.complete(run, List.of(new Effect("ALERT", "second")));
        assertThat(commit.committed()).isTrue();
        assertThat(commit.effectsSuppressed()).isEqualTo(1);
        assertThat(effects(tag)).isEqualTo(1);
    }

    // ------------------------------------------------------------------------------------------ failure injection

    @Test
    void killedNodeIsReplaced_everyAlertSentExactlyOnce() {
        SchedulerNode a = startNode("a"), b = startNode("b");
        awaitBalanced(64, a, b);
        // 1,200 alerts, each takes ~250 ms, tenant capped at 40 in flight, so the kill lands mid-flight.
        jdbc.update("UPDATE tenant SET max_concurrency = 40 WHERE id = 'acme'");
        a.tenantCache().invalidate();
        b.tenantCache().invalidate();
        String tag = seed("acme", 1200, "PICKUP_ALERT", "\"workMillis\":250", 5, "FIRE_NOW", "clock_timestamp()");

        await().atMost(Duration.ofSeconds(30)).until(() -> succeeded(tag) > 150 && a.executor().running() > 0);
        a.crash();                                                  // kill -9: no commit, no lease release, no goodbye
        long doneAtKill = succeeded(tag);
        assertThat(doneAtKill).isLessThan(1200);

        await().atMost(Duration.ofSeconds(90)).until(() -> succeeded(tag) == 1200);
        assertThat(effects(tag)).isEqualTo(1200);
        assertThat(distinctEffectKeys(tag)).isEqualTo(1200);
        assertThat(count("SELECT count(*) FROM job_run WHERE payload->>'bench' = ? AND attempts > 1", tag))
                .as("runs that were leased by the dead node and re-run by the survivor").isPositive();
        assertThat(b.metrics().takeovers.count()).isPositive();
        assertThat(b.membership().owned()).hasSize(64);             // survivor adopted every partition
    }

    @Test
    void frozenNodeWakesUpAfterItsLeaseExpired_itsStaleCommitIsRejected() {
        SchedulerNode a = startNode("a");
        awaitBalanced(64, a);
        String tag = seed("acme", 30, "PICKUP_ALERT", "\"workMillis\":300", 5, "FIRE_NOW", "clock_timestamp()");
        await().atMost(Duration.ofSeconds(10)).until(() -> a.executor().running() > 0);

        a.freeze();                                                 // SIGSTOP / 30 s GC pause: heartbeats, renewals and commits all stop
        SchedulerNode b = startNode("b");                           // the rest of the cluster carries on
        await().atMost(Duration.ofSeconds(60)).until(() -> succeeded(tag) == 30);
        assertThat(b.metrics().takeovers.count()).isPositive();

        a.thaw();                                                   // A wakes up and tries to commit what it computed earlier
        await().atMost(Duration.ofSeconds(15)).until(() -> a.metrics().fencedOut.count() > 0);

        assertThat(effects(tag)).as("exactly one effect per job despite two executions").isEqualTo(30);
        assertThat(distinctEffectKeys(tag)).isEqualTo(30);
        assertThat(count("SELECT count(*) FROM job_run WHERE payload->>'bench' = ? AND status = 'SUCCEEDED'", tag)).isEqualTo(30);
    }

    // ------------------------------------------------------------------------------------------ retries / DLQ

    @Test
    void failingRunsRetryWithBackoffThenSucceed() {
        SchedulerNode a = startNode("a");
        String tag = seed("acme", 5, "FLAKY", "\"failTimes\":2", 5, "FIRE_NOW", "clock_timestamp()");
        await().atMost(Duration.ofSeconds(30)).until(() -> succeeded(tag) == 5);
        assertThat(count("SELECT count(*) FROM job_run WHERE payload->>'bench' = ? AND attempts = 3", tag)).isEqualTo(5);
        assertThat(effects(tag)).isEqualTo(5);
        assertThat(a.metrics().retried.count()).isEqualTo(10);
    }

    @Test
    void exhaustedRunsLandInDeadLetterAndCanBeReplayed() {
        SchedulerNode a = startNode("a");
        String tag = seed("acme", 3, "FLAKY", "\"failTimes\":100", 3, "FIRE_NOW", "clock_timestamp()");
        await().atMost(Duration.ofSeconds(30)).until(() -> count("SELECT count(*) FROM job_run WHERE payload->>'bench' = ? AND status = 'DEAD'", tag) == 3);
        assertThat(count("SELECT count(*) FROM job_run WHERE payload->>'bench' = ? AND attempts = 3", tag)).isEqualTo(3);
        assertThat(a.metrics().deadLettered.count()).isEqualTo(3);
        assertThat(effects(tag)).isZero();

        // Operator fixes the bug (we change the payload so the handler now succeeds) and replays.
        jdbc.update("UPDATE job_run SET payload = jsonb_set(payload, '{failTimes}', '0') WHERE payload->>'bench' = ?", tag);
        RunStore store = new RunStore(jdbc, new TransactionTemplate(new DataSourceTransactionManager(DS)));
        for (var dead : store.deadLetters("acme", 10)) {
            assertThat(store.replayDead(dead.id(), "acme")).isTrue();
        }
        await().atMost(Duration.ofSeconds(30)).until(() -> succeeded(tag) == 3);
        assertThat(effects(tag)).isEqualTo(3);
        assertThat(store.replayDead(1, "globex")).as("replay is tenant scoped").isFalse();
    }

    @Test
    void unknownJobTypeGoesStraightToDeadLetter() {
        startNode("a");
        String tag = seed("acme", 1, "NO_SUCH_TYPE", "", 5, "FIRE_NOW", "clock_timestamp()");
        await().atMost(Duration.ofSeconds(15)).until(() -> count("SELECT count(*) FROM job_run WHERE payload->>'bench' = ? AND status = 'DEAD'", tag) == 1);
    }

    @Test
    void runExceedingMaxRunTimeIsInterruptedAndRetried() {
        SchedulerNode a = startNode("a");
        String tag = seed("acme", 1, "PICKUP_ALERT", "\"workMillis\":20000", 2, "FIRE_NOW", "clock_timestamp()");
        jdbc.update("UPDATE job_run SET max_run_seconds = 1 WHERE payload->>'bench' = ?", tag);
        await().atMost(Duration.ofSeconds(30)).until(() -> count("SELECT count(*) FROM job_run WHERE payload->>'bench' = ? AND status = 'DEAD'", tag) == 1);
        assertThat(a.metrics().timedOut.count()).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT last_error FROM job_run WHERE payload->>'bench' = ?", String.class, tag)).contains("Timeout");
    }

    // ------------------------------------------------------------------------------------------ fairness & caps

    @Test
    void perTenantConcurrencyCapIsNeverExceededAcrossNodes() throws Exception {
        SchedulerNode a = startNode("a"), b = startNode("b"), c = startNode("c");
        awaitBalanced(64, a, b, c);
        jdbc.update("UPDATE tenant SET max_concurrency = 7 WHERE id = 'acme'");
        for (SchedulerNode n : List.of(a, b, c)) {
            n.tenantCache().invalidate();
        }
        String tag = seed("acme", 400, "PICKUP_ALERT", "\"workMillis\":100", 5, "FIRE_NOW", "clock_timestamp()");

        AtomicBoolean done = new AtomicBoolean();
        AtomicInteger max = new AtomicInteger();
        Thread sampler = Thread.ofPlatform().start(() -> {
            JdbcTemplate j = new JdbcTemplate(DS);
            while (!done.get()) {
                Integer live = j.queryForObject("SELECT count(*)::int FROM job_run WHERE tenant_id = 'acme' AND status = 'RUNNING' AND lease_expires_at > clock_timestamp()", Integer.class);
                max.accumulateAndGet(live == null ? 0 : live, Math::max);
            }
        });
        await().atMost(Duration.ofSeconds(60)).until(() -> succeeded(tag) == 400);
        done.set(true);
        sampler.join();
        assertThat(max.get()).as("max concurrent runs observed for the tenant").isBetween(1, 7);
    }

    @Test
    void bigTenantBurstDoesNotDelayASmallTenant() {
        SchedulerNode a = startNode("a"), b = startNode("b"), c = startNode("c");
        awaitBalanced(64, a, b, c);
        jdbc.update("UPDATE tenant SET max_concurrency = 30 WHERE id = 'acme'");
        for (SchedulerNode n : List.of(a, b, c)) {
            n.tenantCache().invalidate();
        }
        // 3,000 alerts at 08:00 for the big tenant, each 100 ms of work => ~10 s of backlog even at full cap.
        String big = seed("acme", 3000, "PICKUP_ALERT", "\"workMillis\":100", 5, "FIRE_NOW", "clock_timestamp()");
        await().atMost(Duration.ofSeconds(20)).until(() -> succeeded(big) > 100);
        // ... and now 20 alerts for another client, due right now.
        String small = seed("globex", 20, "PICKUP_ALERT", "\"workMillis\":10", 5, "FIRE_NOW", "clock_timestamp()");

        await().atMost(Duration.ofSeconds(20)).until(() -> succeeded(small) == 20);
        Double maxLagMs = jdbc.queryForObject("SELECT max(EXTRACT(EPOCH FROM (first_started_at - scheduled_at)) * 1000) FROM job_run WHERE payload->>'bench' = ?", Double.class, small);
        assertThat(succeeded(big)).as("big tenant still has a backlog while the small tenant already finished").isLessThan(3000);
        assertThat(maxLagMs).as("worst fire lag of the small tenant (ms)").isLessThan(2000);
        await().atMost(Duration.ofSeconds(90)).until(() -> succeeded(big) == 3000);
    }

    // ------------------------------------------------------------------------------------------ controls

    @Test
    void pausedTenantDoesNotFire_resumeCatchesUp_otherTenantsUnaffected() throws Exception {
        SchedulerNode a = startNode("a");
        jdbc.update("UPDATE tenant SET paused = TRUE WHERE id = 'acme'");
        String paused = seed("acme", 10, "PICKUP_ALERT", "", 5, "FIRE_NOW", "clock_timestamp()");
        String other = seed("globex", 10, "PICKUP_ALERT", "", 5, "FIRE_NOW", "clock_timestamp()");
        await().atMost(Duration.ofSeconds(15)).until(() -> succeeded(other) == 10);
        Thread.sleep(1500);
        assertThat(succeeded(paused)).isZero();
        assertThat(count("SELECT count(*) FROM job_run WHERE payload->>'bench' = ? AND status = 'PENDING'", paused)).isEqualTo(10);

        jdbc.update("UPDATE tenant SET paused = FALSE WHERE id = 'acme'");
        a.tenantCache().invalidate();
        await().atMost(Duration.ofSeconds(15)).until(() -> succeeded(paused) == 10);
    }

    @Test
    void recurringJobMisfirePolicies() {
        SchedulerNode a = startNode("a");
        awaitBalanced(64, a);
        // Cron every minute; the cluster "was down" for 10 minutes: next_fire_at is 10 minutes in the past.
        for (String policy : List.of("FIRE_NOW", "SKIP", "COALESCE")) {
            jdbc.update("INSERT INTO job (id, tenant_id, name, type, cron, time_zone, payload, misfire_policy, partition_id, next_fire_at) "
                            + "SELECT gen_random_uuid(), 'acme', ?, 'PICKUP_ALERT', '0 * * * * *', 'UTC', '{}'::jsonb, ?, 5, "
                            + "date_trunc('minute', clock_timestamp()) - interval '10 minutes'",
                    "misfire-" + policy, policy);
        }
        // A partition must be owned by this node; with a single node it owns all 64.
        await().atMost(Duration.ofSeconds(20)).until(() -> count("SELECT count(*) FROM job WHERE name LIKE 'misfire-%' AND next_fire_at > clock_timestamp()") == 3);

        long fireNow = count("SELECT count(*) FROM job_run r JOIN job j ON j.id = r.job_id WHERE j.name = 'misfire-FIRE_NOW'");
        long skip = count("SELECT count(*) FROM job_run r JOIN job j ON j.id = r.job_id WHERE j.name = 'misfire-SKIP'");
        long coalesce = count("SELECT count(*) FROM job_run r JOIN job j ON j.id = r.job_id WHERE j.name = 'misfire-COALESCE'");
        assertThat(fireNow).as("FIRE_NOW: every missed minute fires").isGreaterThanOrEqualTo(10);
        assertThat(skip).as("SKIP: only the on-time occurrences").isLessThanOrEqualTo(3);
        assertThat(coalesce).as("COALESCE: one run for all the missed ones (+ on-time)").isBetween(1L, 4L);
        assertThat(count("SELECT max(coalesced_count) FROM job_run r JOIN job j ON j.id = r.job_id WHERE j.name = 'misfire-COALESCE'")).isGreaterThanOrEqualTo(8);
        assertThat(a.metrics().misfireSkipped.count()).isPositive();
    }

    @Test
    void replayedDeadRunOfASkipJobRunsInsteadOfBeingSkipped() {
        startNode("a");
        String tag = seed("acme", 1, "FLAKY", "\"failTimes\":1", 1, "SKIP", "clock_timestamp()");
        await().atMost(Duration.ofSeconds(15)).until(() -> count("SELECT count(*) FROM job_run WHERE payload->>'bench' = ? AND status = 'DEAD'", tag) == 1);

        // the fix is deployed an hour later: the occurrence is old, but a replay is not a late first fire
        jdbc.update("UPDATE job_run SET payload = jsonb_set(payload, '{failTimes}', '0'), scheduled_at = scheduled_at - interval '1 hour' "
                + "WHERE payload->>'bench' = ?", tag);
        RunStore store = new RunStore(jdbc, new TransactionTemplate(new DataSourceTransactionManager(DS)));
        assertThat(store.replayDead(store.deadLetters("acme", 5).get(0).id(), "acme")).isTrue();

        await().atMost(Duration.ofSeconds(15)).until(() -> succeeded(tag) == 1);
        assertThat(effects(tag)).isEqualTo(1);
    }

    @Test
    void seedingNeverOverwritesWhatAnOperatorChanged_andIsSafeWhenNodesStartTogether() throws Exception {
        var props = new SecurityProps("x".repeat(40), Duration.ofHours(1),
                List.of(new SecurityProps.SeedTenant("seedco", "Seed Co", "UTC", 100)),
                List.of(new SecurityProps.SeedUser("seed-admin", "pw", "ADMIN", "seedco")));
        var seeder = new SeedData(props, new TenantStore(jdbc), jdbc, new BCryptPasswordEncoder(4), new MockEnvironment());
        seeder.afterPropertiesSet();

        jdbc.update("UPDATE tenant SET max_concurrency = 7, time_zone = 'Asia/Tokyo' WHERE id = 'seedco'");
        seeder.afterPropertiesSet();                                  // a node restarts
        assertThat(count("SELECT max_concurrency FROM tenant WHERE id = 'seedco'")).isEqualTo(7);
        assertThat(jdbc.queryForObject("SELECT time_zone FROM tenant WHERE id = 'seedco'", String.class)).isEqualTo("Asia/Tokyo");

        jdbc.update("DELETE FROM app_user");                          // three nodes starting at the same instant
        List<Throwable> failures = new java.util.concurrent.CopyOnWriteArrayList<>();
        List<Thread> starters = java.util.stream.IntStream.range(0, 8).mapToObj(i -> Thread.ofPlatform().start(() -> {
            try {
                seeder.afterPropertiesSet();
            } catch (Throwable t) {
                failures.add(t);
            }
        })).toList();
        for (Thread t : starters) {
            t.join();
        }
        assertThat(failures).isEmpty();
        assertThat(count("SELECT count(*) FROM app_user WHERE username = 'seed-admin'")).isEqualTo(1);

        jdbc.update("DELETE FROM app_user");
        var prod = new MockEnvironment();
        prod.setActiveProfiles("prod");
        new SeedData(props, new TenantStore(jdbc), jdbc, new BCryptPasswordEncoder(4), prod).afterPropertiesSet();
        assertThat(count("SELECT count(*) FROM app_user")).as("no demo users under the prod profile").isZero();
    }

    @Test
    void gracefulShutdownHandsPartitionsBackImmediately() {
        SchedulerNode a = startNode("a"), b = startNode("b");
        awaitBalanced(64, a, b);
        b.stop();
        await().atMost(Duration.ofSeconds(10)).until(() -> a.membership().owned().size() == 64);
    }
}
