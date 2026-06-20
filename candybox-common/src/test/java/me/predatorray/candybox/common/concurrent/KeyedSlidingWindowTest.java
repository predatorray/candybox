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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.Test;

class KeyedSlidingWindowTest {

    @Test
    void rejectsNonPositiveWindow() {
        assertThatThrownBy(() -> new KeyedSlidingWindow<>(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void keepsOnlyMostRecentValuesPerKey() {
        KeyedSlidingWindow<String, Integer> w = new KeyedSlidingWindow<>(3);
        for (int i = 1; i <= 5; i++) {
            w.append("a", i);
        }
        w.append("b", 99);
        assertThat(w.snapshot("a")).containsExactly(3, 4, 5);
        assertThat(w.snapshot("b")).containsExactly(99);
        assertThat(w.snapshot("missing")).isEmpty();
    }

    @Test
    void snapshotAllReturnsEveryKey() {
        KeyedSlidingWindow<String, Integer> w = new KeyedSlidingWindow<>(2);
        w.append("a", 1);
        w.append("a", 2);
        w.append("b", 3);
        assertThat(w.snapshot()).containsOnlyKeys("a", "b");
        assertThat(w.snapshot().get("a")).containsExactly(1, 2);
    }

    // ---- multi-threaded -------------------------------------------------------------------------

    @Test
    void concurrentAppendsToOneKeyStayBoundedAndUntorn() throws Exception {
        // Many threads append to the same key while others snapshot it; the window must never exceed
        // capacity and a snapshot must never throw or observe a torn deque.
        int window = 50;
        KeyedSlidingWindow<String, Integer> w = new KeyedSlidingWindow<>(window);
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();

        int maxSnapshot = ConcurrencyTestSupport.runConcurrently(8, failures, t -> {
            int localMax = 0;
            for (int i = 0; i < 50_000; i++) {
                if ((t & 1) == 0) {
                    w.append("hot", i);
                } else {
                    List<Integer> snap = w.snapshot("hot");
                    localMax = Math.max(localMax, snap.size());
                }
            }
            return localMax;
        });

        assertThat(failures).isEmpty();
        assertThat(maxSnapshot).isLessThanOrEqualTo(window);
        assertThat(w.snapshot("hot")).hasSize(window);
    }

    @Test
    void concurrentAppendsAcrossManyKeysDoNotInterfere() throws Exception {
        int window = 10;
        KeyedSlidingWindow<Integer, Integer> w = new KeyedSlidingWindow<>(window);
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();

        ConcurrencyTestSupport.runConcurrently(8, failures, t -> {
            for (int i = 0; i < 20_000; i++) {
                w.append(i % 64, i);
                if (i % 200 == 0) {
                    w.snapshot();
                }
            }
            return 0;
        });

        assertThat(failures).isEmpty();
        for (List<Integer> values : w.snapshot().values()) {
            assertThat(values).hasSizeLessThanOrEqualTo(window);
        }
    }
}
