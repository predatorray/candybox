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
package me.predatorray.candybox.coordination.zk;

import java.util.concurrent.TimeUnit;

/** A local upper bound; backend tokens and recover-open remain the write fence. */
final class LocalLeaseValidity {
    private final long startedNanos;
    private final long validNanos;
    private final long expiryMillis;

    LocalLeaseValidity(long startedNanos, long ttlMillis, long expiryMillis) {
        this.startedNanos = startedNanos;
        long marginMillis = Math.min(100, Math.max(1, ttlMillis / 20));
        this.validNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0, ttlMillis - marginMillis));
        this.expiryMillis = expiryMillis;
    }

    boolean isValid(long nowNanos, long nowMillis) {
        // Subtraction handles nanoTime wraparound for realistic lease durations.
        return nowNanos - startedNanos < validNanos && nowMillis < expiryMillis;
    }
}
