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
package me.predatorray.candybox.bookkeeper.bk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import me.predatorray.candybox.bookkeeper.LedgerEntry;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.common.exception.StorageException;
import org.junit.jupiter.api.Test;

class ReadHandleCacheTest {
    private final List<Handle> opened = new ArrayList<>();

    private Handle open(long id) {
        Handle result = new Handle(id);
        opened.add(result);
        return result;
    }

    @Test
    void repeatedAndConcurrentBorrowsShareOnePhysicalOpen() {
        try (ReadHandleCache cache = new ReadHandleCache(2, this::open)) {
            ReadableLedger first = cache.acquire(1);
            try (ReadableLedger second = cache.acquire(1)) {
                first.close();
                first.close();
                assertThat(second.read(0).data()).containsExactly((byte) 1);
            }
            for (int i = 0; i < 100; i++) {
                try (ReadableLedger borrowed = cache.acquire(1)) {
                    assertThat(borrowed.read(0).entryId()).isZero();
                }
            }
            assertThat(opened).hasSize(1);
            assertThat(opened.get(0).closes).isZero();
        }
        assertThat(opened.get(0).closes).isEqualTo(1);
    }

    @Test
    void evictionKeepsInFlightReadsAliveAndReopensEvictedLedger() {
        try (ReadHandleCache cache = new ReadHandleCache(1, this::open)) {
            ReadableLedger first = cache.acquire(1);
            try (ReadableLedger ignored = cache.acquire(2)) {
                assertThat(opened.get(0).closes).isZero();
                assertThat(first.read(0).data()).containsExactly((byte) 1);
            }
            first.close();
            assertThat(opened.get(0).closes).isEqualTo(1);
            try (ReadableLedger ignored = cache.acquire(1)) {
                assertThat(opened).hasSize(3);
            }
            assertThat(opened.get(1).closes).isEqualTo(1);
        }
        assertThat(opened).allSatisfy(handle -> assertThat(handle.closes).isEqualTo(1));
    }

    @Test
    void invalidationAndFailedOldBorrowDoNotEvictReplacement() {
        try (ReadHandleCache cache = new ReadHandleCache(2, this::open)) {
            ReadableLedger old = cache.acquire(1);
            cache.invalidate(1);
            try (ReadableLedger replacement = cache.acquire(1)) {
                opened.get(0).fail = true;
                assertThatThrownBy(() -> old.read(0)).isInstanceOf(StorageException.class);
                old.close();
                try (ReadableLedger again = cache.acquire(1)) {
                    assertThat(again.read(0).entryId()).isZero();
                }
                assertThat(opened).hasSize(2);
            }
        }
    }

    @Test
    void failedReadIsEvictedAndCloseWaitsForOutstandingBorrow() {
        ReadHandleCache cache = new ReadHandleCache(2, this::open);
        ReadableLedger first = cache.acquire(1);
        opened.get(0).fail = true;
        assertThatThrownBy(() -> first.read(0)).isInstanceOf(StorageException.class);
        ReadableLedger replacement = cache.acquire(1);
        cache.close();
        cache.close();
        assertThatThrownBy(() -> cache.acquire(1)).isInstanceOf(StorageException.class);
        assertThat(opened).allSatisfy(handle -> assertThat(handle.closes).isZero());
        first.close();
        replacement.close();
        assertThat(opened).allSatisfy(handle -> assertThat(handle.closes).isEqualTo(1));
    }

    @Test
    void coldMetadataOpenDoesNotBlockWarmBorrowsOrReleases() throws Exception {
        CountDownLatch opening = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try (ReadHandleCache cache = new ReadHandleCache(2, id -> {
            if (id == 2) {
                opening.countDown();
                await(resume);
            }
            return open(id);
        })) {
            ReadableLedger warm = cache.acquire(1);
            var cold = executor.submit(() -> cache.acquire(2));
            try {
                assertThat(opening.await(5, TimeUnit.SECONDS)).isTrue();
                var available = executor.submit(() -> {
                    warm.close();
                    try (ReadableLedger borrowed = cache.acquire(1)) {
                        return borrowed.read(0).data();
                    }
                });
                assertThat(available.get(2, TimeUnit.SECONDS)).containsExactly((byte) 1);
            } finally {
                resume.countDown();
            }
            cold.get(5, TimeUnit.SECONDS).close();
        } finally {
            resume.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void evictionCloseDoesNotBlockWarmBorrows() throws Exception {
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try (ReadHandleCache cache = new ReadHandleCache(1, this::open)) {
            cache.acquire(1).close();
            opened.get(0).onClose = () -> {
                closing.countDown();
                await(resume);
            };
            var evicting = executor.submit(() -> cache.acquire(2));
            try {
                assertThat(closing.await(5, TimeUnit.SECONDS)).isTrue();
                var warm = executor.submit(() -> {
                    try (ReadableLedger borrowed = cache.acquire(2)) {
                        return borrowed.read(0).data();
                    }
                });
                assertThat(warm.get(2, TimeUnit.SECONDS)).containsExactly((byte) 2);
            } finally {
                resume.countDown();
            }
            evicting.get(5, TimeUnit.SECONDS).close();
        } finally {
            resume.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void invalidationDuringOpenDoesNotRepopulateTheCache() throws Exception {
        CountDownLatch opening = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        try (ReadHandleCache cache = new ReadHandleCache(2, id -> {
            opening.countDown();
            await(resume);
            return open(id);
        })) {
            var pending = executor.submit(() -> cache.acquire(1));
            assertThat(opening.await(5, TimeUnit.SECONDS)).isTrue();
            cache.invalidate(1);
            resume.countDown();
            pending.get(5, TimeUnit.SECONDS).close();
            assertThat(opened.get(0).closes).isEqualTo(1);
            cache.acquire(1).close();
            assertThat(opened).hasSize(2);
        } finally {
            resume.countDown();
            executor.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("test did not resume backend I/O");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }

    private static final class Handle implements ReadableLedger {
        private final long id;
        int closes;
        boolean fail;
        Runnable onClose = () -> { };

        Handle(long id) { this.id = id; }
        @Override public long ledgerId() { return id; }
        @Override public long lastAddConfirmed() { return 0; }
        @Override public boolean isSealed() { return true; }
        @Override public Map<String, byte[]> customMetadata() { return Map.of(); }
        @Override public void close() { onClose.run(); closes++; }
        @Override public LedgerEntry read(long entryId) {
            return readRange(entryId, entryId).get(0);
        }
        @Override public List<LedgerEntry> readRange(long first, long last) {
            if (fail || closes != 0) { throw new StorageException("unavailable"); }
            return List.of(new LedgerEntry(first, new byte[]{(byte) id}));
        }
    }
}
