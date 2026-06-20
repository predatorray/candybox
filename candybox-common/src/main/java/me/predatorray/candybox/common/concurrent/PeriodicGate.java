/*
 * Copyright (c) 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package me.predatorray.candybox.common.concurrent;

import me.predatorray.candybox.common.Clock;

/**
 * A throttle that runs an action at most once per fixed interval, no matter how many threads call it
 * in between. This is the business-logic-free extraction of the volatile-deadline + double-checked
 * lock idiom used to rate-limit periodic side effects (such as re-stat'ing a credentials file) off
 * the hot read path.
 *
 * <p>The hot path is a single volatile read of the next-due deadline; only when the deadline has
 * passed does a caller take the lock, re-check the deadline under it (so concurrent callers do not
 * all fire), advance the deadline, and run the action <em>while still holding the lock</em>. Holding
 * the lock across the action guarantees two runs never overlap even if an action outlasts its own
 * interval, so callers must keep the action short and must not acquire locks that could invert
 * ordering with this gate's monitor.
 *
 * <p>The deadline is advanced <em>before</em> the action runs, so a throwing action still consumes
 * its interval (it will not be retried until the next interval elapses) and the exception propagates
 * to the caller that happened to run it.
 */
public final class PeriodicGate {

    private final Clock clock;
    private final long intervalMillis;
    private volatile long nextDueAtMillis;

    /**
     * @param clock          the time source governing the interval (injected for deterministic tests)
     * @param intervalMillis the minimum spacing between two runs of the action; must be non-negative
     *                       (zero runs the action on every call)
     */
    public PeriodicGate(Clock clock, long intervalMillis) {
        if (intervalMillis < 0) {
            throw new IllegalArgumentException("intervalMillis must be non-negative: " + intervalMillis);
        }
        this.clock = clock;
        this.intervalMillis = intervalMillis;
        this.nextDueAtMillis = clock.currentTimeMillis() + intervalMillis;
    }

    /**
     * Runs {@code action} if at least {@code intervalMillis} has elapsed since the last run, otherwise
     * returns without running it. At most one of any number of concurrent callers runs the action per
     * interval.
     *
     * @return {@code true} if this call ran the action, {@code false} if it was throttled
     */
    public boolean runIfDue(Runnable action) {
        if (clock.currentTimeMillis() < nextDueAtMillis) {
            return false;
        }
        synchronized (this) {
            long now = clock.currentTimeMillis();
            if (now < nextDueAtMillis) {
                return false;
            }
            nextDueAtMillis = now + intervalMillis;
            action.run();
            return true;
        }
    }
}
