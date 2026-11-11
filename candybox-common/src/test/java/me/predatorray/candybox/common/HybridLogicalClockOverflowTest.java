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
package me.predatorray.candybox.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HybridLogicalClockOverflowTest {
    @Test
    void logicalOverflowSpillsIntoNextMillisecondAndContinuesMonotonically() {
        HybridLogicalClock clock = new HybridLogicalClock(1, new ManualClock(1000), 60_000);
        Hlc previous = clock.tick();
        Hlc observed = new Hlc(1000, Integer.MAX_VALUE, 2);
        clock.observe(observed);
        Hlc spilled = clock.tick();
        Hlc next = clock.tick();

        assertThat(observed).isGreaterThan(previous);
        assertThat(spilled).isEqualTo(new Hlc(1001, 0, 1)).isGreaterThan(observed);
        assertThat(next).isEqualTo(new Hlc(1001, 1, 1)).isGreaterThan(spilled);
    }

    @Test
    void higherObservedLogicalCounterAtSamePhysicalTimeAdvancesNextTick() {
        HybridLogicalClock clock = new HybridLogicalClock(1, new ManualClock(1000), 60_000);
        clock.tick();
        clock.tick();
        assertThat(clock.tick()).isEqualTo(new Hlc(1000, 2, 1));
        Hlc observed = new Hlc(1000, 5, 2);
        clock.observe(observed);
        assertThat(clock.tick()).isEqualTo(new Hlc(1000, 6, 1)).isGreaterThan(observed);
    }
}
