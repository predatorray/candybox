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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import me.predatorray.candybox.common.ManualClock;
import org.junit.jupiter.api.Test;

class PeriodicGateTest {

    @Test
    void runsOnlyOncePerInterval() {
        ManualClock clock = new ManualClock(0);
        PeriodicGate gate = new PeriodicGate(clock, 100);
        AtomicInteger runs = new AtomicInteger();

        assertThat(gate.runIfDue(runs::incrementAndGet)).isFalse(); // not yet due
        clock.advance(100);
        assertThat(gate.runIfDue(runs::incrementAndGet)).isTrue();
        assertThat(gate.runIfDue(runs::incrementAndGet)).isFalse(); // throttled
        clock.advance(99);
        assertThat(gate.runIfDue(runs::incrementAndGet)).isFalse();
        clock.advance(1);
        assertThat(gate.runIfDue(runs::incrementAndGet)).isTrue();
        assertThat(runs).hasValue(2);
    }

    @Test
    void throwingActionStillConsumesItsInterval() {
        ManualClock clock = new ManualClock(0);
        PeriodicGate gate = new PeriodicGate(clock, 10);
        clock.advance(10);
        try {
            gate.runIfDue(() -> {
                throw new IllegalStateException("boom");
            });
        } catch (IllegalStateException expected) {
            // propagated to the caller that ran it
        }
        AtomicInteger runs = new AtomicInteger();
        // The interval was consumed despite the throw; not due again until the clock advances.
        assertThat(gate.runIfDue(runs::incrementAndGet)).isFalse();
    }

    // ---- multi-threaded -------------------------------------------------------------------------

    @Test
    void aSwarmAtTheDeadlineFiresTheActionExactlyOnce() throws Exception {
        // The crux of the double-checked-lock idiom: with the clock pinned exactly at the deadline,
        // every caller passes the lock-free outer check, but the inner re-check under the lock must
        // let exactly ONE of them run the action. A broken DCL (missing inner check) fires it
        // once-per-caller. Repeated across many fresh gates to defeat luck.
        for (int round = 0; round < 500; round++) {
            ManualClock clock = new ManualClock(0);
            PeriodicGate gate = new PeriodicGate(clock, 100);
            clock.advance(100); // exactly due
            AtomicInteger runs = new AtomicInteger();
            ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();

            int fired = ConcurrencyTestSupport.runConcurrently(16, failures, t -> {
                boolean ran = gate.runIfDue(runs::incrementAndGet);
                return ran ? 1 : 0;
            });

            assertThat(failures).isEmpty();
            assertThat(runs.get()).as("round %d action runs", round).isEqualTo(1);
            assertThat(fired).isEqualTo(1); // exactly one caller reports it ran
        }
    }
}
