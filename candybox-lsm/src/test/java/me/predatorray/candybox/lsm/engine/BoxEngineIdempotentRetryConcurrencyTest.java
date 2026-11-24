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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.auth.ObjectAcl;
import me.predatorray.candybox.common.config.CandyboxConfig;
import org.junit.jupiter.api.Test;

class BoxEngineIdempotentRetryConcurrencyTest {
    private static final int CALLERS = 8;
    private static final int ROUNDS = 50;
    private static final CandyKey SOURCE = CandyKey.of("src");
    private static final CandyKey DESTINATION = CandyKey.of("dst");

    @Test
    void concurrentCompleteMultipartRetriesApplyExactlyOnce() throws Exception {
        repeat(engine -> {
            String upload = engine.createMultipartUpload(DESTINATION, "text/plain", Map.of());
            var first = engine.uploadPart(upload, 1, new byte[] {1, 2});
            var second = engine.uploadPart(upload, 2, new byte[] {3});
            var parts = List.of(new BoxEngine.PartCompletion(1, first.crc32c()),
                    new BoxEngine.PartCompletion(2, second.crc32c()));
            long before = engine.stats().puts();
            CandyMetadata result = race(() -> engine.completeMultipartUpload(
                    upload, parts, "tok-1", ObjectAcl.NONE));
            assertThat(engine.headCandy(DESTINATION)).isEqualTo(result);
            assertThat(engine.getCandy(DESTINATION)).containsExactly(1, 2, 3);
            assertThat(engine.listMultipartUploads()).isEmpty();
            assertThat(engine.stats().puts() - before).isEqualTo(1);
        });
    }

    @Test
    void concurrentCopyRetriesApplyExactlyOnce() throws Exception {
        repeat(engine -> {
            engine.putCandy(SOURCE, new byte[] {1, 2, 3}, "text/plain", Map.of(), null);
            long before = engine.stats().puts();
            CandyMetadata result = race(() -> engine.copyCandy(SOURCE, DESTINATION, "tok-2"));
            assertThat(engine.headCandy(DESTINATION)).isEqualTo(result);
            assertThat(engine.getCandy(DESTINATION)).containsExactly(1, 2, 3);
            // Copies share the puts counter; BoxEngineStats has no separate copy counter.
            assertThat(engine.stats().puts() - before).isEqualTo(1);
        });
    }

    @Test
    void concurrentZeroCopyPutRetriesApplyExactlyOnce() throws Exception {
        repeat(engine -> {
            engine.putCandy(SOURCE, new byte[] {1, 2, 3}, "text/plain", Map.of(), null);
            var locator = engine.resolveLocator(SOURCE);
            long before = engine.stats().puts();
            CandyMetadata result = race(() -> engine.zeroCopyPut(DESTINATION, locator.parts(),
                    locator.contentType(), locator.userMetadata(), locator.createdAtMillis(),
                    locator.acl(), "tok-3"));
            assertThat(engine.headCandy(DESTINATION)).isEqualTo(result);
            assertThat(engine.resolveLocator(DESTINATION).parts()).isEqualTo(locator.parts());
            assertThat(engine.getCandy(DESTINATION)).containsExactly(1, 2, 3);
            assertThat(engine.stats().puts() - before).isEqualTo(1);
        });
    }

    private static CandyMetadata race(Callable<CandyMetadata> operation) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(CALLERS);
        CountDownLatch ready = new CountDownLatch(CALLERS);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<CandyMetadata>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < CALLERS; i++) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(30, TimeUnit.SECONDS)) {
                        throw new AssertionError("Start gate timed out");
                    }
                    return operation.call();
                }));
            }
            assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            CandyMetadata winner = futures.get(0).get(30, TimeUnit.SECONDS);
            for (Future<CandyMetadata> future : futures) {
                assertThat(future.get(30, TimeUnit.SECONDS)).isEqualTo(winner);
            }
            return winner;
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void repeat(Scenario scenario) throws Exception {
        for (int round = 0; round < ROUNDS; round++) {
            try (var store = new InMemoryLedgerStore();
                 var engine = BoxEngine.createNew(BoxName.of("retry-box"),
                         CandyboxConfig.builder().multipartMinPartBytes(1).build(), store,
                         1, new ManualClock(1000), 1L)) {
                // Fixed physical time makes a second application differ in its logical HLC.
                scenario.run(engine);
            }
        }
    }

    @FunctionalInterface
    private interface Scenario {
        void run(BoxEngine engine) throws Exception;
    }
}
