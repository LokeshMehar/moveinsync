package com.moveinsync.scheduler.core;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Pure decision logic: given the occurrences that are due, which runs do we create? */
public final class MisfireResolver {

    public record PlannedRun(Instant scheduledAt, int coalescedCount) {
    }

    public record Plan(List<PlannedRun> runs, int skipped) {
    }

    private final Duration grace;
    private final int maxCatchUp;

    public MisfireResolver(Duration grace, int maxCatchUp) {
        this.grace = grace;
        this.maxCatchUp = maxCatchUp;
    }

    /**
     * @param dueOccurrences ascending occurrences with {@code scheduledAt <= now}
     */
    public Plan resolve(List<Instant> dueOccurrences, MisfirePolicy policy, Instant now) {
        Instant misfireBefore = now.minus(grace);
        List<Instant> missed = new ArrayList<>();
        List<Instant> onTime = new ArrayList<>();
        for (Instant occurrence : dueOccurrences) {
            (occurrence.isBefore(misfireBefore) ? missed : onTime).add(occurrence);
        }
        List<PlannedRun> runs = new ArrayList<>();
        int skipped = 0;
        switch (policy) {
            case FIRE_NOW -> {
                List<Instant> all = new ArrayList<>(missed);
                all.addAll(onTime);
                int from = Math.max(0, all.size() - maxCatchUp);
                skipped = from;                                   // only beyond the safety cap
                all.subList(from, all.size()).forEach(i -> runs.add(new PlannedRun(i, 1)));
            }
            case SKIP -> {
                skipped = missed.size();
                onTime.forEach(i -> runs.add(new PlannedRun(i, 1)));
            }
            case COALESCE -> {
                if (!missed.isEmpty()) {
                    runs.add(new PlannedRun(missed.get(missed.size() - 1), missed.size()));
                }
                onTime.forEach(i -> runs.add(new PlannedRun(i, 1)));
            }
        }
        return new Plan(runs, skipped);
    }
}
