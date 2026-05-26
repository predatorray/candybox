package me.predatorray.candybox.common;

/**
 * The production {@link Clock}, backed by {@link System#currentTimeMillis()}.
 */
public final class SystemClock implements Clock {

    /** A shared, stateless instance. */
    public static final SystemClock INSTANCE = new SystemClock();

    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
