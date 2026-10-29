package com.moveinsync.scheduler.handlers;

import com.moveinsync.scheduler.model.Models.ClaimedRun;

import java.util.Map;

/** The concrete job types named in the case study, plus a deliberately flaky one for retry / dead-letter demos. */
public final class TripHandlers {

    private TripHandlers() {
    }

    /** "Your cab is assigned" alert, 30 minutes before pickup. */
    public static class PickupAlertHandler extends SimulatedWorkHandler {
        @Override public String type() { return "PICKUP_ALERT"; }
        @Override protected String effectKind() { return "ALERT"; }
    }

    /** Generates tomorrow's rosters at 22:00 tenant-local time. */
    public static class RosterGenerationHandler extends SimulatedWorkHandler {
        @Override public String type() { return "ROSTER_GENERATION"; }
        @Override protected String effectKind() { return "ROSTER"; }
    }

    /** Auto-cancels no-shows. */
    public static class NoShowCancelHandler extends SimulatedWorkHandler {
        @Override public String type() { return "NO_SHOW_CANCEL"; }
        @Override protected String effectKind() { return "CANCEL"; }
    }

    /** Closes daily billing. */
    public static class BillingCloseHandler extends SimulatedWorkHandler {
        @Override public String type() { return "BILLING_CLOSE"; }
        @Override protected String effectKind() { return "BILLING"; }
    }

    /** Fails the first {@code failTimes} attempts, then succeeds. {@code failTimes} >= maxAttempts => dead letter. */
    public static class FlakyHandler extends SimulatedWorkHandler {
        @Override public String type() { return "FLAKY"; }
        @Override protected String effectKind() { return "FLAKY"; }

        @Override
        protected void beforeEffect(ClaimedRun run, Map<String, Object> payload) {
            long failTimes = asLong(payload.get("failTimes"), 0);
            if (run.attempts() <= failTimes) {
                throw new IllegalStateException("injected failure on attempt " + run.attempts());
            }
        }
    }
}
