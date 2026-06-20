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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A thread-safe store of per-key bounded sliding windows: each key maps to the most recent
 * {@code windowSize} appended values, oldest evicted first. This is the business-logic-free
 * extraction of the {@code ConcurrentHashMap<K, ArrayDeque<V>>} guarded by {@code synchronized(deque)}
 * idiom used to buffer rolling-window samples.
 *
 * <p>Concurrency contract: the outer map is a {@link ConcurrentHashMap}, and each per-key deque is
 * its own monitor — every append, eviction and snapshot of a given key's window happens while holding
 * that deque's intrinsic lock, so a snapshot never observes a torn window and a CME is impossible.
 * Different keys never contend.
 *
 * @param <K> key type
 * @param <V> value type stored in each window
 */
public final class KeyedSlidingWindow<K, V> {

    private final int windowSize;
    private final ConcurrentMap<K, Deque<V>> windows = new ConcurrentHashMap<>();

    /**
     * @param windowSize the maximum number of values retained per key; must be positive
     */
    public KeyedSlidingWindow(int windowSize) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize must be positive: " + windowSize);
        }
        this.windowSize = windowSize;
    }

    /** The configured per-key window capacity. */
    public int windowSize() {
        return windowSize;
    }

    /**
     * Appends {@code value} to {@code key}'s window, evicting the oldest values until the window is
     * within capacity.
     */
    public void append(K key, V value) {
        Deque<V> window = windows.computeIfAbsent(key, k -> new ArrayDeque<>(windowSize));
        synchronized (window) {
            window.addLast(value);
            while (window.size() > windowSize) {
                window.removeFirst();
            }
        }
    }

    /**
     * Returns an oldest-to-newest snapshot of {@code key}'s window, or an empty list if the key has no
     * values.
     */
    public List<V> snapshot(K key) {
        Deque<V> window = windows.get(key);
        if (window == null) {
            return List.of();
        }
        synchronized (window) {
            return new ArrayList<>(window);
        }
    }

    /**
     * Returns a consistent snapshot of every key's window (each list oldest-to-newest), in no
     * particular key order. Each window is copied under its own lock.
     */
    public Map<K, List<V>> snapshot() {
        Map<K, List<V>> out = new LinkedHashMap<>();
        for (Map.Entry<K, Deque<V>> e : windows.entrySet()) {
            Deque<V> window = e.getValue();
            synchronized (window) {
                out.put(e.getKey(), new ArrayList<>(window));
            }
        }
        return out;
    }

    /** Removes all windows. */
    public void clear() {
        windows.clear();
    }
}
