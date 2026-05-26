package me.predatorray.candybox.common;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link Clock} whose time is set explicitly. Used by tests (including clock-skew and handover
 * cases) and by in-JVM deployments that want a deterministic clock.
 *
 * <p>The time can be moved both forwards and, deliberately, backwards — Candybox's correctness must
 * not depend on the wall clock being monotonic. Thread-safe.
 */
public final class ManualClock implements Clock {

    private final AtomicLong millis;

    public ManualClock(long initialMillis) {
        this.millis = new AtomicLong(initialMillis);
    }

    public ManualClock() {
        this(0L);
    }

    @Override
    public long currentTimeMillis() {
        return millis.get();
    }

    /** Sets the absolute time. May move backwards. */
    public void setTime(long newMillis) {
        millis.set(newMillis);
    }

    /** Advances time by {@code deltaMillis} (which may be negative). Returns the new time. */
    public long advance(long deltaMillis) {
        return millis.addAndGet(deltaMillis);
    }
}
