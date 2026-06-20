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
package me.predatorray.candybox.lsm.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReferenceArray;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BoxEngineConcurrencyTest {

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private final BoxName box = BoxName.of("race-box");
    private BoxEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.close();
        }
        store.close();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Two concurrent retries of the same write (same idempotency token) must apply the mutation
     * exactly once. Before the inside-the-write-lock re-check, both callers could pass the unlocked
     * fast-path check while the cache was still cold and then each take the lock and write — a double
     * apply (two HLCs, {@code puts} incremented twice). With the re-check, the second caller observes
     * the first's cached result under the lock and returns it unchanged.
     */
    @Test
    void concurrentRetriesWithSameIdempotencyTokenApplyExactlyOnce() throws Exception {
        // A ManualClock that never advances forces the two writes to differ only by HLC logical
        // counter, so a double-apply is unmistakable: the two returned metadatas would not be equal
        // and puts would jump by two.
        engine = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1, new ManualClock(1000), 1L);

        int iterations = 300;
        for (int i = 0; i < iterations; i++) {
            String key = "k/" + i;
            String token = "token-" + i;
            long putsBefore = engine.stats().puts();

            CyclicBarrier barrier = new CyclicBarrier(2);
            AtomicReferenceArray<CandyMetadata> results = new AtomicReferenceArray<>(2);
            AtomicReferenceArray<Throwable> errors = new AtomicReferenceArray<>(2);

            Runnable body0 = put(key, token, results, errors, 0, barrier);
            Runnable body1 = put(key, token, results, errors, 1, barrier);
            Thread t0 = new Thread(body0, "put-0");
            Thread t1 = new Thread(body1, "put-1");
            t0.start();
            t1.start();
            t0.join();
            t1.join();

            assertThat(errors.get(0)).as("thread 0 error at i=%d", i).isNull();
            assertThat(errors.get(1)).as("thread 1 error at i=%d", i).isNull();

            long applied = engine.stats().puts() - putsBefore;
            assertThat(applied).as("puts applied for token %s", token).isEqualTo(1);
            // Both callers observe the identical winning result (same HLC, same metadata).
            assertThat(results.get(0)).as("results agree at i=%d", i).isEqualTo(results.get(1));
        }
    }

    private Runnable put(String key, String token, AtomicReferenceArray<CandyMetadata> results,
                         AtomicReferenceArray<Throwable> errors, int slot, CyclicBarrier barrier) {
        return () -> {
            try {
                barrier.await();
                CandyMetadata result = engine.putCandy(CandyKey.of(key), bytes("payload-" + key),
                        "text/plain", Map.of(), token);
                results.set(slot, result);
            } catch (Throwable t) {
                errors.set(slot, t);
            }
        };
    }
}
