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

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Optimistic read-modify-write helpers over a {@link CoordinationService}: the {@code get} the current
 * version, then {@code create}-if-absent-or-{@code compareAndSet}, retrying a bounded number of times
 * on {@link CasConflictException} idiom that several stores hand-rolled. Business-logic-free; callers
 * decide how to treat exhaustion (wrap, swallow, or propagate the {@link CasConflictException}).
 *
 * <p>These do not back off between attempts. They are intended for short, low-contention races (an
 * owner publishing its own state, an ACL edit); a conflict means a peer wrote concurrently, so a
 * fresh re-read and retry converges quickly. Use {@code maxRetries = 0} for a single attempt that
 * propagates the first conflict (the caller then typically swallows it and converges on a later pass).
 */
public final class CoordinationCas {

    private CoordinationCas() {
    }

    /**
     * Sets {@code key} to a freshly computed value, whether or not it already exists, retrying on a
     * concurrent-write conflict. The value is supplied lazily so a retry can recompute it against the
     * just-observed state if needed (constant values may ignore the re-read).
     *
     * @param coordination the backing service
     * @param key          the key to upsert
     * @param value        supplies the bytes to write; re-invoked on each attempt
     * @param maxRetries   how many times to retry after the first conflict (0 = single attempt)
     * @return the new version written
     * @throws CasConflictException if every attempt (1 + {@code maxRetries}) lost a race
     */
    public static long upsert(CoordinationService coordination, String key,
                              Supplier<byte[]> value, int maxRetries) {
        requireNonNegative(maxRetries);
        for (int attempt = 0; ; attempt++) {
            try {
                Optional<VersionedValue> current = coordination.get(key);
                if (current.isEmpty()) {
                    return coordination.create(key, value.get());
                }
                return coordination.compareAndSet(key, value.get(), current.get().version());
            } catch (CasConflictException raced) {
                if (attempt >= maxRetries) {
                    throw raced;
                }
            }
        }
    }

    /** {@link #upsert(CoordinationService, String, Supplier, int)} with a constant value. */
    public static long upsert(CoordinationService coordination, String key, byte[] value,
                              int maxRetries) {
        return upsert(coordination, key, () -> value, maxRetries);
    }

    /**
     * Deletes {@code key} if it currently exists, retrying on a concurrent-write conflict. A key that
     * is already absent is a no-op.
     *
     * @param maxRetries how many times to retry after the first conflict (0 = single attempt)
     * @return {@code true} if a value was deleted, {@code false} if the key was already absent
     * @throws CasConflictException if every attempt (1 + {@code maxRetries}) lost a race
     */
    public static boolean deleteIfPresent(CoordinationService coordination, String key,
                                          int maxRetries) {
        requireNonNegative(maxRetries);
        for (int attempt = 0; ; attempt++) {
            try {
                Optional<VersionedValue> current = coordination.get(key);
                if (current.isEmpty()) {
                    return false;
                }
                coordination.delete(key, current.get().version());
                return true;
            } catch (CasConflictException raced) {
                if (attempt >= maxRetries) {
                    throw raced;
                }
            }
        }
    }

    private static void requireNonNegative(int maxRetries) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be non-negative: " + maxRetries);
        }
    }
}
