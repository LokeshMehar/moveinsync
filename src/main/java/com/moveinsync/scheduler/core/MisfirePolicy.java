package com.moveinsync.scheduler.core;

/** What to do with occurrences that were due while no node was able to fire them. */
public enum MisfirePolicy {
    /** Fire every missed occurrence immediately (bounded by a catch-up cap). */
    FIRE_NOW,
    /** Drop missed occurrences; wait for the next scheduled one. */
    SKIP,
    /** Collapse all missed occurrences into a single run (scheduled at the latest missed instant). */
    COALESCE
}
