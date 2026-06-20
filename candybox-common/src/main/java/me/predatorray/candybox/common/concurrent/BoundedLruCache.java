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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A thread-safe, fixed-capacity cache that evicts the least-recently-used entry once it is full.
 *
 * <p>This is the business-logic-free extraction of the access-ordered {@link LinkedHashMap} idiom
 * (the {@code removeEldestEntry} override) that callers previously hand-rolled and wrapped in
 * {@link java.util.Collections#synchronizedMap}. Access order makes both {@link #get} and
 * {@link #put} structural modifications, so every operation is serialized on the instance lock —
 * unlike a plain {@code synchronizedMap}, reads here genuinely need the lock too, which is exactly
 * why the locking is encapsulated rather than left to the caller.
 *
 * <p>Null keys and null values are rejected so {@link #get} returning {@code null} unambiguously
 * means "absent".
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class BoundedLruCache<K, V> {

    private final int capacity;
    private final LinkedHashMap<K, V> map;

    /**
     * @param capacity the maximum number of entries to retain; must be positive
     */
    public BoundedLruCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        // access-order=true so iteration/eviction order tracks recency of use.
        this.map = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > BoundedLruCache.this.capacity;
            }
        };
    }

    /** The configured maximum number of entries. */
    public int capacity() {
        return capacity;
    }

    /**
     * Returns the value cached under {@code key}, marking it most-recently-used, or {@code null} if
     * absent.
     */
    public synchronized V get(K key) {
        return map.get(requireNonNull(key, "key"));
    }

    /**
     * Stores {@code value} under {@code key} as the most-recently-used entry, evicting the
     * least-recently-used entry if the cache is at capacity.
     *
     * @return the previous value mapped to {@code key}, or {@code null} if there was none
     */
    public synchronized V put(K key, V value) {
        return map.put(requireNonNull(key, "key"), requireNonNull(value, "value"));
    }

    /** Whether {@code key} currently has a cached value (and marks it most-recently-used). */
    public synchronized boolean containsKey(K key) {
        return map.get(requireNonNull(key, "key")) != null;
    }

    /** The current number of cached entries (never exceeds {@link #capacity()}). */
    public synchronized int size() {
        return map.size();
    }

    /** Removes all entries. */
    public synchronized void clear() {
        map.clear();
    }

    private static <T> T requireNonNull(T value, String what) {
        if (value == null) {
            throw new NullPointerException(what + " must not be null");
        }
        return value;
    }
}
