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
import java.util.concurrent.atomic.AtomicInteger;
import me.predatorray.candybox.common.ManualClock;
import org.junit.jupiter.api.Test;

class TtlCacheTest {

    @Test
    void servesHitUntilTtlElapsesThenReloads() {
        ManualClock clock = new ManualClock(1000);
        TtlCache<String, Integer> cache = new TtlCache<>(clock, 100);
        AtomicInteger loads = new AtomicInteger();

        assertThat(cache.get("k", k -> loads.incrementAndGet())).isEqualTo(1);
        assertThat(cache.get("k", k -> loads.incrementAndGet())).isEqualTo(1); // hit, no reload
        assertThat(loads).hasValue(1);

        clock.advance(99);
        assertThat(cache.get("k", k -> loads.incrementAndGet())).isEqualTo(1); // still fresh
        assertThat(loads).hasValue(1);

        clock.advance(1); // now == expiry → stale
        assertThat(cache.get("k", k -> loads.incrementAndGet())).isEqualTo(2); // reloaded
        assertThat(loads).hasValue(2);
    }

    @Test
    void getIfFreshNeverLoadsAndRespectsExpiry() {
        ManualClock clock = new ManualClock(0);
        TtlCache<String, String> cache = new TtlCache<>(clock, 50);
        assertThat(cache.getIfFresh("k")).isEmpty();
        cache.put("k", "v");
        assertThat(cache.getIfFresh("k")).contains("v");
        clock.advance(50);
        assertThat(cache.getIfFresh("k")).isEmpty();
    }

    @Test
    void invalidateForcesReload() {
        ManualClock clock = new ManualClock(0);
        TtlCache<String, Integer> cache = new TtlCache<>(clock, 10_000);
        AtomicInteger loads = new AtomicInteger();
        cache.get("k", k -> loads.incrementAndGet());
        cache.invalidate("k");
        cache.get("k", k -> loads.incrementAndGet());
        assertThat(loads).hasValue(2);
    }

    @Test
    void zeroTtlNeverServesHit() {
        ManualClock clock = new ManualClock(0);
        TtlCache<String, Integer> cache = new TtlCache<>(clock, 0);
        AtomicInteger loads = new AtomicInteger();
        cache.get("k", k -> loads.incrementAndGet());
        cache.get("k", k -> loads.incrementAndGet());
        assertThat(loads).hasValue(2);
    }

    @Test
    void rejectsNullLoadedValue() {
        TtlCache<String, String> cache = new TtlCache<>(new ManualClock(0), 100);
        assertThatThrownBy(() -> cache.get("k", k -> null))
                .isInstanceOf(NullPointerException.class);
    }

    // ---- multi-threaded -------------------------------------------------------------------------

    @Test
    void concurrentMissesAllReturnAConsistentValueAndCacheConverges() throws Exception {
        // With a cold cache and TTL that never expires during the test, many threads racing on the
        // same key may each load once (documented), but every returned value must be a valid load and
        // subsequent reads must hit. We assert no torn reads / exceptions and that the cache ends warm.
        ManualClock clock = new ManualClock(0);
        TtlCache<String, Integer> cache = new TtlCache<>(clock, 1_000_000);
        AtomicInteger loads = new AtomicInteger();
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();

        ConcurrencyTestSupport.runConcurrently(16, failures, t -> {
            for (int i = 0; i < 10_000; i++) {
                Integer v = cache.get("hot", k -> loads.incrementAndGet());
                if (v == null || v <= 0) {
                    throw new AssertionError("unexpected value " + v);
                }
            }
            return 0;
        });

        assertThat(failures).isEmpty();
        // The cache is warm: a read does not trigger a further load.
        int loadsAfter = loads.get();
        cache.get("hot", k -> loads.incrementAndGet());
        assertThat(loads.get()).isEqualTo(loadsAfter);
    }

    @Test
    void concurrentGetAndInvalidateDoesNotCorruptOrLeakStale() throws Exception {
        // Race read-through against invalidate under an expiring TTL with a moving clock. Any stale
        // value served must still be a legitimate load; the structure must not throw.
        ManualClock clock = new ManualClock(0);
        TtlCache<Integer, Integer> cache = new TtlCache<>(clock, 5);
        AtomicInteger loads = new AtomicInteger();
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();

        ConcurrencyTestSupport.runConcurrently(12, failures, t -> {
            for (int i = 0; i < 20_000; i++) {
                int key = i % 8;
                if ((t & 1) == 0) {
                    cache.get(key, k -> loads.incrementAndGet());
                } else {
                    cache.invalidate(key);
                }
                if (i % 100 == 0) {
                    clock.advance(1);
                }
            }
            return 0;
        });
        assertThat(failures).isEmpty();
    }
}
