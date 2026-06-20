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

import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.Test;

class BoundedLruCacheTest {

    @Test
    void rejectsNonPositiveCapacity() {
        assertThatThrownBy(() -> new BoundedLruCache<>(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullKeyAndValue() {
        BoundedLruCache<String, String> cache = new BoundedLruCache<>(4);
        assertThatThrownBy(() -> cache.put(null, "v")).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> cache.put("k", null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> cache.get(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void evictsLeastRecentlyUsedOnceFull() {
        BoundedLruCache<Integer, String> cache = new BoundedLruCache<>(3);
        cache.put(1, "a");
        cache.put(2, "b");
        cache.put(3, "c");
        // Touch 1 so 2 becomes the least-recently-used.
        assertThat(cache.get(1)).isEqualTo("a");
        cache.put(4, "d"); // evicts 2

        assertThat(cache.size()).isEqualTo(3);
        assertThat(cache.get(2)).isNull();
        assertThat(cache.get(1)).isEqualTo("a");
        assertThat(cache.get(3)).isEqualTo("c");
        assertThat(cache.get(4)).isEqualTo("d");
    }

    @Test
    void overwriteUpdatesValueWithoutGrowing() {
        BoundedLruCache<Integer, String> cache = new BoundedLruCache<>(2);
        cache.put(1, "a");
        assertThat(cache.put(1, "a2")).isEqualTo("a");
        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.get(1)).isEqualTo("a2");
    }

    // ---- multi-threaded -------------------------------------------------------------------------

    @Test
    void neverExceedsCapacityUnderConcurrentDistinctWrites() throws Exception {
        int capacity = 64;
        BoundedLruCache<Integer, Integer> cache = new BoundedLruCache<>(capacity);
        // Many threads hammer a key space far larger than capacity; size must stay bounded and the
        // map must never corrupt (no exceptions, no over-capacity reads).
        int threads = 8;
        int perThread = 20_000;
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        int maxObserved = ConcurrencyTestSupport.runConcurrently(threads, failures, t -> {
            int localMax = 0;
            for (int i = 0; i < perThread; i++) {
                int key = (t * 7 + i) % (capacity * 8);
                cache.put(key, i);
                if (i % 16 == 0) {
                    cache.get((t * 3 + i) % (capacity * 8));
                }
                localMax = Math.max(localMax, cache.size());
            }
            return localMax;
        });

        assertThat(failures).isEmpty();
        assertThat(maxObserved).isLessThanOrEqualTo(capacity);
        assertThat(cache.size()).isLessThanOrEqualTo(capacity);
    }

    @Test
    void concurrentReadersAndWritersOnSharedKeysStayConsistent() throws Exception {
        // Access-order LRU mutates structure on get(); if get() were not serialized this would throw
        // ConcurrentModificationException or corrupt the linked list. Drive heavy get+put on a small
        // shared key set to surface any such race.
        BoundedLruCache<Integer, Integer> cache = new BoundedLruCache<>(16);
        for (int i = 0; i < 16; i++) {
            cache.put(i, i);
        }
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        ConcurrencyTestSupport.runConcurrently(10, failures, t -> {
            for (int i = 0; i < 50_000; i++) {
                int key = i % 16;
                cache.get(key);
                cache.put(key, i);
            }
            return 0;
        });
        assertThat(failures).isEmpty();
        assertThat(cache.size()).isLessThanOrEqualTo(16);
    }
}
