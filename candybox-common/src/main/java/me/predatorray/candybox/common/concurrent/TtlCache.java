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

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import me.predatorray.candybox.common.Clock;

/**
 * A thread-safe read-through cache whose entries expire a fixed time after they are written. This is
 * the business-logic-free extraction of the {@code ConcurrentHashMap<K, (value, expiresAt)>} idiom
 * that several stores hand-rolled to avoid hitting their backing service (ZooKeeper, the cluster) on
 * every request.
 *
 * <p>Staleness model: an entry is served until {@code clock.currentTimeMillis() >= expiresAt}, after
 * which it is treated as absent and reloaded. Expiry is checked lazily on access; expired entries are
 * not actively reaped, but a reload overwrites them.
 *
 * <p><b>Loaders run outside the cache's internal locks.</b> {@link #get(Object, Function)} reads the
 * (lock-free) map, and only if the entry is missing or stale does it invoke the loader, then publishes
 * the result with {@link ConcurrentMap#put}. It deliberately does <em>not</em> wrap the loader in
 * {@link ConcurrentMap#computeIfAbsent}, which would run the (potentially blocking, potentially
 * remote) loader while holding a bin lock — the very anti-pattern this class exists to avoid. The
 * trade-off is that concurrent misses for the same key may each load once; the last write wins. Null
 * values are rejected; cache "absence" of a present-but-empty result should be modelled with an
 * {@link Optional} value type.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class TtlCache<K, V> {

    private final Clock clock;
    private final long ttlMillis;
    private final ConcurrentMap<K, Entry<V>> entries = new ConcurrentHashMap<>();

    /**
     * @param clock     the time source governing expiry (injected for deterministic tests)
     * @param ttlMillis how long after a write an entry stays fresh; must be non-negative (zero means
     *                  every entry is immediately stale, i.e. the cache never serves a hit)
     */
    public TtlCache(Clock clock, long ttlMillis) {
        if (ttlMillis < 0) {
            throw new IllegalArgumentException("ttlMillis must be non-negative: " + ttlMillis);
        }
        this.clock = clock;
        this.ttlMillis = ttlMillis;
    }

    /**
     * Returns the fresh cached value for {@code key}, loading and caching it via {@code loader} on a
     * miss or expiry. The loader is invoked without holding any cache lock.
     *
     * @param loader computes the value for a key on a miss; must not return {@code null}
     */
    public V get(K key, Function<? super K, ? extends V> loader) {
        Entry<V> existing = entries.get(key);
        long now = clock.currentTimeMillis();
        if (existing != null && now < existing.expiresAtMillis) {
            return existing.value;
        }
        V loaded = loader.apply(key);
        if (loaded == null) {
            throw new NullPointerException("loader returned null for key " + key);
        }
        entries.put(key, new Entry<>(loaded, now + ttlMillis));
        return loaded;
    }

    /**
     * Returns the fresh cached value for {@code key}, or empty if absent or expired. Never loads.
     */
    public Optional<V> getIfFresh(K key) {
        Entry<V> existing = entries.get(key);
        if (existing != null && clock.currentTimeMillis() < existing.expiresAtMillis) {
            return Optional.of(existing.value);
        }
        return Optional.empty();
    }

    /** Stores {@code value} under {@code key}, fresh for the configured TTL from now. */
    public void put(K key, V value) {
        if (value == null) {
            throw new NullPointerException("value must not be null");
        }
        entries.put(key, new Entry<>(value, clock.currentTimeMillis() + ttlMillis));
    }

    /** Drops any cached entry for {@code key} (e.g. after a write the cache must not serve stale). */
    public void invalidate(K key) {
        entries.remove(key);
    }

    /** Drops every cached entry. */
    public void clear() {
        entries.clear();
    }

    private static final class Entry<V> {
        final V value;
        final long expiresAtMillis;

        Entry(V value, long expiresAtMillis) {
            this.value = value;
            this.expiresAtMillis = expiresAtMillis;
        }
    }
}
