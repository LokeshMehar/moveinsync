package com.moveinsync.scheduler.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TenantCronScheduleTest {

    private static final ZoneId NY = ZoneId.of("America/New_York");

    @Test
    void dailyJobFiresAtLocalTimeInTenantZone() {
        var schedule = TenantCronSchedule.of("0 0 22 * * *", ZoneId.of("Asia/Kolkata"));   // 22:00 IST = 16:30 UTC
        assertThat(schedule.next(Instant.parse("2026-06-01T00:00:00Z"))).contains(Instant.parse("2026-06-01T16:30:00Z"));
    }

    @Test
    void springForwardGap_0230DoesNotExist_firesOnceAtFirstInstantAfterGap() {
        // 2026-03-08: clocks jump 02:00 EST -> 03:00 EDT. 02:30 never happens.
        var schedule = TenantCronSchedule.of("0 30 2 * * *", NY);
        List<Instant> fires = schedule.nextN(Instant.parse("2026-03-07T00:00:00Z"), 3);
        assertThat(fires).containsExactly(
                Instant.parse("2026-03-07T07:30:00Z"),   // Mar 7 02:30 EST
                Instant.parse("2026-03-08T07:00:00Z"),   // Mar 8: 03:00 EDT (gap end), exactly once
                Instant.parse("2026-03-09T06:30:00Z"));  // Mar 9 02:30 EDT
    }

    @Test
    void fallBackOverlap_0130HappensTwice_firesOnceAtFirstOccurrence() {
        // 2026-11-01: 02:00 EDT falls back to 01:00 EST, so 01:30 occurs at 05:30Z (EDT) and again at 06:30Z (EST).
        var schedule = TenantCronSchedule.of("0 30 1 * * *", NY);
        List<Instant> fires = schedule.nextN(Instant.parse("2026-10-31T12:00:00Z"), 3);
        assertThat(fires).containsExactly(
                Instant.parse("2026-11-01T05:30:00Z"),   // first 01:30 (EDT)
                Instant.parse("2026-11-02T06:30:00Z"),   // next day 01:30 EST; the repeated 01:30 did NOT fire
                Instant.parse("2026-11-03T06:30:00Z"));
    }

    @Test
    void multipleOccurrencesCollapsingIntoTheGapFireOnce() {
        var schedule = TenantCronSchedule.of("0 0,30 2 * * *", NY);
        List<Instant> fires = schedule.between(Instant.parse("2026-03-08T00:00:00Z"), Instant.parse("2026-03-08T12:00:00Z"), 10);
        assertThat(fires).containsExactly(Instant.parse("2026-03-08T07:00:00Z"));
    }

    @Test
    void firesAreStrictlyIncreasing() {
        var schedule = TenantCronSchedule.of("0 */15 * * * *", NY);
        List<Instant> fires = schedule.between(Instant.parse("2026-03-07T00:00:00Z"), Instant.parse("2026-03-10T00:00:00Z"), 1000);
        for (int i = 1; i < fires.size(); i++) {
            assertThat(fires.get(i)).isAfter(fires.get(i - 1));
        }
    }

    @Test
    void fiveFieldCronGetsZeroSeconds() {
        assertThat(TenantCronSchedule.of("30 2 * * *", ZoneId.of("UTC")).expression()).isEqualTo("0 30 2 * * *");
    }

    @Test
    void invalidCronIsRejected() {
        assertThatThrownBy(() -> TenantCronSchedule.of("not a cron", ZoneId.of("UTC"))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void betweenIsInclusiveOfBothEnds() {
        var schedule = TenantCronSchedule.of("0 0 * * * *", ZoneId.of("UTC"));
        assertThat(schedule.between(Instant.parse("2026-01-01T10:00:00Z"), Instant.parse("2026-01-01T12:00:00Z"), 10))
                .containsExactly(Instant.parse("2026-01-01T10:00:00Z"), Instant.parse("2026-01-01T11:00:00Z"), Instant.parse("2026-01-01T12:00:00Z"));
    }
}
