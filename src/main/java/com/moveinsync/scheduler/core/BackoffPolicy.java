package com.moveinsync.scheduler.core;

import java.time.Duration;
import java.util.random.RandomGenerator;

/** Exponential backoff with "equal jitter": delay = cap/2 + random(0, cap/2), cap = min(max, base * 2^(attempt-1)). */
public final class BackoffPolicy {

    private final long baseMillis;
    private final long maxMillis;
    private final RandomGenerator random;

    public BackoffPolicy(Duration base, Duration max, RandomGenerator random) {
        this.baseMillis = base.toMillis();
        this.maxMillis = max.toMillis();
        this.random = random;
    }

    /** @param failedAttempts number of attempts already made (>= 1) */
    public Duration delayAfter(int failedAttempts) {
        int exponent = Math.min(Math.max(failedAttempts - 1, 0), 30);
        long cap = Math.min(maxMillis, baseMillis * (1L << exponent));
        if (cap <= 0) {
            return Duration.ZERO;
        }
        long half = cap / 2;
        return Duration.ofMillis(half + (half == 0 ? 0 : random.nextLong(half + 1)));
    }

    public Duration ceilingAfter(int failedAttempts) {
        int exponent = Math.min(Math.max(failedAttempts - 1, 0), 30);
        return Duration.ofMillis(Math.min(maxMillis, baseMillis * (1L << exponent)));
    }
}
