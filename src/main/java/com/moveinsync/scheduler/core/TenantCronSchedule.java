package com.moveinsync.scheduler.core;

import org.springframework.scheduling.support.CronExpression;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.zone.ZoneOffsetTransition;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A cron schedule evaluated on the tenant's <b>local wall clock</b>.
 *
 * <p>DST policy (deliberate, documented in docs/DESIGN.md):
 * <ul>
 *   <li><b>Spring-forward gap</b> (02:00 jumps to 03:00): a local time that does not exist fires once, at the
 *       first instant after the gap (03:00). Several occurrences that collapse onto that instant fire once.</li>
 *   <li><b>Fall-back overlap</b> (01:30 happens twice): a local time that occurs twice fires once, at its
 *       <i>first</i> occurrence. The repeated wall-clock hour does not fire again.</li>
 * </ul>
 * The cron fields are evaluated in a zone-less {@link LocalDateTime} space (which has no DST), and each local
 * result is then resolved to an instant. Every returned instant is strictly increasing.
 */
public final class TenantCronSchedule {

    private static final int MAX_COLLAPSE_STEPS = 16;

    private final CronExpression cron;
    private final ZoneId zone;
    private final String expression;

    private TenantCronSchedule(CronExpression cron, ZoneId zone, String expression) {
        this.cron = cron;
        this.zone = zone;
        this.expression = expression;
    }

    /** Accepts Spring's 6-field cron (sec min hour dom month dow); a 5-field cron gets "0" seconds prepended. */
    public static TenantCronSchedule of(String expression, ZoneId zone) {
        String normalised = normalise(expression);
        if (!CronExpression.isValidExpression(normalised)) {
            throw new IllegalArgumentException("invalid cron expression");
        }
        return new TenantCronSchedule(CronExpression.parse(normalised), zone, normalised);
    }

    public static String normalise(String expression) {
        String trimmed = expression == null ? "" : expression.trim().replaceAll("\\s+", " ");
        return trimmed.split(" ").length == 5 ? "0 " + trimmed : trimmed;
    }

    public String expression() {
        return expression;
    }

    public ZoneId zone() {
        return zone;
    }

    /** The first fire time strictly after {@code after}. */
    public Optional<Instant> next(Instant after) {
        LocalDateTime cursor = LocalDateTime.ofInstant(after, zone);
        for (int i = 0; i < MAX_COLLAPSE_STEPS; i++) {
            LocalDateTime local = cron.next(cursor);
            if (local == null) {
                return Optional.empty();
            }
            Instant candidate = resolve(local);
            if (candidate.isAfter(after)) {
                return Optional.of(candidate);
            }
            cursor = local;   // collapsed into a gap instant we already fired: keep walking
        }
        return Optional.empty();
    }

    /** Up to {@code count} fire times strictly after {@code after}. */
    public List<Instant> nextN(Instant after, int count) {
        List<Instant> out = new ArrayList<>(count);
        Instant cursor = after;
        while (out.size() < count) {
            Optional<Instant> n = next(cursor);
            if (n.isEmpty()) {
                break;
            }
            out.add(n.get());
            cursor = n.get();
        }
        return out;
    }

    /** Fire times in {@code [fromInclusive, toInclusive]}, at most {@code max} of them. */
    public List<Instant> between(Instant fromInclusive, Instant toInclusive, int max) {
        List<Instant> out = new ArrayList<>();
        Instant cursor = fromInclusive.minusNanos(1);
        while (out.size() < max) {
            Optional<Instant> n = next(cursor);
            if (n.isEmpty() || n.get().isAfter(toInclusive)) {
                break;
            }
            out.add(n.get());
            cursor = n.get();
        }
        return out;
    }

    private Instant resolve(LocalDateTime local) {
        ZoneOffsetTransition transition = zone.getRules().getTransition(local);
        if (transition != null && transition.isGap()) {
            return transition.getInstant();                       // gap: fire at the first valid instant
        }
        return ZonedDateTime.ofLocal(local, zone, null).toInstant(); // overlap: earlier offset (first occurrence)
    }
}
