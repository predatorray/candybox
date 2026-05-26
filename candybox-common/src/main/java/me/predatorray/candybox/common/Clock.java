package me.predatorray.candybox.common;

/**
 * A source of physical wall-clock time, injected everywhere so tests can control time deterministically.
 *
 * <p>Implementations must be thread-safe.
 */
public interface Clock {

    /**
     * Returns the current physical time in milliseconds since the Unix epoch.
     *
     * <p>This value may move backwards (NTP step, VM migration); Candybox never trusts it for
     * ordering — see {@link HybridLogicalClock}.
     *
     * @return wall-clock time in epoch milliseconds
     */
    long currentTimeMillis();
}
