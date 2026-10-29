package com.moveinsync.scheduler.engine;

import java.util.concurrent.CountDownLatch;

/**
 * In-process stand-in for SIGSTOP / a long GC pause, used by integration tests: while frozen, every engine loop and
 * every commit parks here, exactly like a stopped JVM. Production code never freezes it.
 */
public class ProcessFreeze {

    private volatile CountDownLatch gate = new CountDownLatch(0);

    public void freeze() {
        gate = new CountDownLatch(1);
    }

    public void thaw() {
        gate.countDown();
    }

    public boolean frozen() {
        return gate.getCount() > 0;
    }

    public void await() {
        try {
            gate.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
