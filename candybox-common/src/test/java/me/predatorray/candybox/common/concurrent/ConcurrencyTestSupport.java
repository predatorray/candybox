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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helpers for the concurrency unit tests: spin up worker threads, release them in lockstep so they
 * collide on the structure under test, and surface any thrown error to the asserting thread.
 */
final class ConcurrencyTestSupport {

    private ConcurrencyTestSupport() {
    }

    @FunctionalInterface
    interface Worker {
        /** Runs the worker body for {@code threadIndex}, returning a per-thread integer to aggregate. */
        int run(int threadIndex) throws Exception;
    }

    /**
     * Runs {@code threads} copies of {@code worker}, started together at a {@link CyclicBarrier} to
     * maximize contention. Any throwable from a worker is added to {@code failures} (the asserting
     * test decides how to react). Returns the maximum value any worker returned.
     */
    static int runConcurrently(int threads, Queue<Throwable> failures, Worker worker)
            throws InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(threads);
        AtomicInteger max = new AtomicInteger(Integer.MIN_VALUE);
        List<Thread> workers = new ArrayList<>(threads);
        for (int i = 0; i < threads; i++) {
            int index = i;
            Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    int result = worker.run(index);
                    max.accumulateAndGet(result, Math::max);
                } catch (Throwable t) {
                    failures.add(t);
                }
            }, "concurrency-test-" + i);
            workers.add(thread);
        }
        workers.forEach(Thread::start);
        for (Thread thread : workers) {
            thread.join();
        }
        return max.get();
    }
}
