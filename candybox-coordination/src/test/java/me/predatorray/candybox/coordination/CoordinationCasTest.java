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
package me.predatorray.candybox.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import org.junit.jupiter.api.Test;

class CoordinationCasTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String str(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    @Test
    void upsertCreatesWhenAbsentThenOverwrites() {
        InMemoryCoordinationService cs = new InMemoryCoordinationService();
        assertThat(CoordinationCas.upsert(cs, "k", bytes("v0"), 3)).isEqualTo(0);
        assertThat(CoordinationCas.upsert(cs, "k", bytes("v1"), 3)).isEqualTo(1);
        assertThat(cs.get("k").map(v -> str(v.value()))).contains("v1");
    }

    @Test
    void deleteIfPresentReportsWhetherItRemovedAnything() {
        InMemoryCoordinationService cs = new InMemoryCoordinationService();
        assertThat(CoordinationCas.deleteIfPresent(cs, "k", 3)).isFalse();
        cs.create("k", bytes("v"));
        assertThat(CoordinationCas.deleteIfPresent(cs, "k", 3)).isTrue();
        assertThat(cs.get("k")).isEmpty();
    }

    @Test
    void singleAttemptPropagatesConflictButOneRetryRecovers() {
        // Deterministically inject a race: between our get() and our compareAndSet(), an "interloper"
        // bumps the version. With maxRetries=0 the conflict propagates; with a retry the re-read sees
        // the new version and succeeds.
        InMemoryCoordinationService cs = new InMemoryCoordinationService();
        cs.create("k", bytes("seed")); // version 0

        AtomicInteger valueCalls = new AtomicInteger();
        Supplier<byte[]> racyValue = () -> {
            if (valueCalls.getAndIncrement() == 0) {
                // A concurrent writer wins the race exactly once, on our first attempt.
                cs.compareAndSet("k", bytes("interloper"), 0);
            }
            return bytes("mine");
        };

        assertThatThrownBy(() -> CoordinationCas.upsert(cs, "k", racyValue, 0))
                .isInstanceOf(CasConflictException.class);

        // Reset and prove a single retry absorbs the same race.
        InMemoryCoordinationService cs2 = new InMemoryCoordinationService();
        cs2.create("k", bytes("seed"));
        AtomicInteger calls2 = new AtomicInteger();
        Supplier<byte[]> racyValue2 = () -> {
            if (calls2.getAndIncrement() == 0) {
                cs2.compareAndSet("k", bytes("interloper"), 0);
            }
            return bytes("mine");
        };
        long version = CoordinationCas.upsert(cs2, "k", racyValue2, 1);
        assertThat(version).isEqualTo(2); // 0 seed, 1 interloper, 2 us
        assertThat(str(cs2.get("k").orElseThrow().value())).isEqualTo("mine");
    }

    @Test
    void rejectsNegativeMaxRetries() {
        InMemoryCoordinationService cs = new InMemoryCoordinationService();
        assertThatThrownBy(() -> CoordinationCas.upsert(cs, "k", bytes("v"), -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---- multi-threaded -------------------------------------------------------------------------

    @Test
    void concurrentUpsertsAllApplyExactlyOnceWithRetries() throws Exception {
        // Every thread upserts a distinct value to the same key, with generous retries. The decisive
        // invariant: the final coordination version equals the number of successful writes (threads),
        // i.e. no upsert was silently dropped and none applied twice. A broken retry loop would lose
        // writes and leave the version short.
        InMemoryCoordinationService cs = new InMemoryCoordinationService();
        int threads = 16;
        int maxRetries = 10_000; // bounded but far above any realistic conflict streak
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        List<Thread> workers = new ArrayList<>();
        AtomicInteger successes = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            int id = i;
            Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    CoordinationCas.upsert(cs, "shared", bytes("node-" + id), maxRetries);
                    successes.incrementAndGet();
                } catch (Throwable t) {
                    failures.add(t);
                }
            });
            workers.add(thread);
        }
        workers.forEach(Thread::start);
        for (Thread t : workers) {
            t.join();
        }

        assertThat(failures).isEmpty();
        assertThat(successes).hasValue(threads);
        Optional<VersionedValue> finalValue = cs.get("shared");
        assertThat(finalValue).isPresent();
        // create()=v0, then one compareAndSet per remaining successful writer ⇒ version == threads-1.
        assertThat(finalValue.get().version()).isEqualTo((long) threads - 1);
        assertThat(str(finalValue.get().value())).startsWith("node-");
    }

    @Test
    void concurrentUpsertAndDeleteNeverThrowWithRetries() throws Exception {
        // Mix upserts and deletes on the same key under contention; with retries, no operation should
        // surface a CasConflictException to the caller (each converges on a fresh read).
        InMemoryCoordinationService cs = new InMemoryCoordinationService();
        int threads = 12;
        int iterations = 2_000;
        // Each attempt makes progress (the fake serializes CAS, so some caller always wins a round);
        // a thread therefore wins within a bounded number of retries, but a tight per-op cap would
        // still flake under 12-way contention. Use an effectively-unbounded cap to assert "retries
        // converge without ever surfacing a conflict".
        int maxRetries = 1_000_000;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
        List<Thread> workers = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            int id = i;
            Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                    for (int n = 0; n < iterations; n++) {
                        if ((id + n) % 2 == 0) {
                            CoordinationCas.upsert(cs, "shared", bytes("w" + id + "-" + n), maxRetries);
                        } else {
                            CoordinationCas.deleteIfPresent(cs, "shared", maxRetries);
                        }
                    }
                } catch (Throwable t) {
                    failures.add(t);
                }
            });
            workers.add(thread);
        }
        workers.forEach(Thread::start);
        for (Thread t : workers) {
            t.join();
        }

        assertThat(failures).isEmpty();
    }
}
