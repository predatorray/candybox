/*
 * Copyright (c) 2017 the original author or authors.
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

package me.predatorray.candybox.store.util;

import me.predatorray.candybox.util.Validations;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ConcurrentUnidirectionalLinkedMap<K, V> {

    private final int queueCapacity;
    private int queueSize = 0;

    private final ConcurrentMap<K, Entry<K, V>> map;

    private volatile Entry<K, V> head = null;
    private volatile Entry<K, V> tail = null;

    private volatile Entry<K, V> last = null;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    public ConcurrentUnidirectionalLinkedMap(int capacity) {
        this.queueCapacity = Validations.positive(capacity);
        this.map = new ConcurrentHashMap<>(capacity);
    }

    public ConcurrentUnidirectionalLinkedMap(Map<K, V> initialData, int capacity) {
        Validations.notNull(initialData);
        this.queueCapacity = Validations.positive(capacity);
        this.map = new ConcurrentHashMap<>(capacity);

        Map<K, Entry<K, V>> initialEntryDataMap = initialData.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Entry::new));
        this.map.putAll(initialEntryDataMap);
    }

    public boolean put(K key, V value) {
        Entry<K, V> entry = new Entry<>(key, value);

        lock.lock();
        try {
            if (queueSize >= queueCapacity) {
                return false;
            }
            if (head == null) {
                head = entry;
            }

            if (tail != null) {
                tail.setNext(entry);
            }
            tail = entry;

            ++queueSize;

            map.put(key, entry);
            last = entry;
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
        return true;
    }

    public void putSilently(K key, V value) {
        Entry<K, V> entry = new Entry<>(key, value);
        lock.lock();
        try {
            map.put(key, entry);
            last = entry;
        } finally {
            lock.unlock();
        }
    }

    public V get(K key) {
        Entry<K, V> entry = map.get(key);
        if (entry == null) {
            return null;
        }
        return entry.value;
    }

    public Entry<K, V> take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (queueSize <= 0) {
                notEmpty.await();
            }
            Entry<K, V> taken = this.head;
            this.head = taken.next.get();
            if (this.head == null) {
                this.tail = null;
            }
            return taken;
        } finally {
            lock.unlock();
        }
    }

    public Optional<Entry<K, V>> last() {
        return Optional.ofNullable(last);
    }

    public static class Entry<K, V> {

        private final K key;
        private final V value;

        private final AtomicReference<Entry<K, V>> next = new AtomicReference<>(null);

        Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        Entry(Map.Entry<K, V> mapEntry) {
            this(mapEntry.getKey(), mapEntry.getValue());
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        boolean setNext(Entry<K, V> nextEntry) {
            return next.compareAndSet(null, nextEntry);
        }
    }
}
