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
package me.predatorray.candybox.lsm.memtable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.RangeTombstone;

/**
 * The in-memory write buffer: a sorted map of CandyKey to its current {@link CandyLocator}, holding
 * at most one (the LWW winner) per key. Backed by a {@link ConcurrentSkipListMap} so reads and the
 * flush scan see a consistent key order without blocking writes.
 *
 * <p>Last-writer-wins is applied on every {@link #put(Mutation)}: a mutation only replaces an existing
 * entry if its HLC is strictly greater, so out-of-order or replayed writes can never regress a key.
 * Tombstones are stored like any other locator and resolved by the read/merge path.
 */
public final class Memtable {

    private final ConcurrentNavigableMap<CandyKey, CandyLocator> map = new ConcurrentSkipListMap<>();
    // Range tombstones accumulated by deleteRange; small in number, scanned per lookup (DESIGN §6).
    private final List<RangeTombstone> rangeTombstones = new CopyOnWriteArrayList<>();
    private final AtomicLong approximateBytes = new AtomicLong(0);

    /**
     * Applies a mutation under LWW.
     *
     * @return {@code true} if it won (was stored), {@code false} if an existing higher-HLC entry kept its place
     */
    public boolean put(Mutation mutation) {
        CandyKey key = mutation.key();
        CandyLocator incoming = mutation.locator();
        CandyLocator winner = map.merge(key, incoming,
                (existing, candidate) -> candidate.hlc().isAfter(existing.hlc()) ? candidate : existing);
        approximateBytes.addAndGet(estimateSize(key, incoming));
        return winner == incoming;
    }

    /** Returns the current locator for {@code key}, if any (may be a tombstone). */
    public Optional<CandyLocator> get(CandyKey key) {
        return Optional.ofNullable(map.get(key));
    }

    /** Records a range tombstone ({@code deleteRange}). It shadows older keys in its interval. */
    public void delete(RangeTombstone tombstone) {
        rangeTombstones.add(tombstone);
        approximateBytes.addAndGet(estimateSize(tombstone));
    }

    /**
     * The highest HLC among range tombstones covering {@code key}, or {@code null} if none cover it.
     * A point locator at {@code key} is shadowed iff its HLC is older than this value.
     */
    public Hlc maxRangeTombstoneCovering(CandyKey key) {
        Hlc max = null;
        for (RangeTombstone rt : rangeTombstones) {
            if (rt.covers(key) && (max == null || rt.hlc().isAfter(max))) {
                max = rt.hlc();
            }
        }
        return max;
    }

    /** A snapshot of the range tombstones held here (used by flush). */
    public List<RangeTombstone> rangeTombstones() {
        return List.copyOf(rangeTombstones);
    }

    /** Rough heap footprint, used to decide when to flush. Overestimates after overwrites. */
    public long approximateSizeBytes() {
        return approximateBytes.get();
    }

    public int size() {
        return map.size();
    }

    /** Whether the memtable holds nothing to flush — neither point entries nor range tombstones. */
    public boolean isEmpty() {
        return map.isEmpty() && rangeTombstones.isEmpty();
    }

    /** Iterates all entries in ascending key order as mutations (used by flush and merge). */
    public Iterator<Mutation> iterator() {
        return mutationIterator(map);
    }

    /** Iterates entries with key {@code >= startInclusive} in ascending order. */
    public Iterator<Mutation> iterator(CandyKey startInclusive) {
        return mutationIterator(map.tailMap(startInclusive, true));
    }

    /** Iterates all entries in descending key order. */
    public Iterator<Mutation> descendingIterator() {
        return mutationIterator(map.descendingMap());
    }

    /** Iterates entries with key {@code <= startInclusive} in descending order. */
    public Iterator<Mutation> descendingIterator(CandyKey startInclusive) {
        return mutationIterator(map.headMap(startInclusive, true).descendingMap());
    }

    private static Iterator<Mutation> mutationIterator(Map<CandyKey, CandyLocator> view) {
        Iterator<Map.Entry<CandyKey, CandyLocator>> it = view.entrySet().iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Mutation next() {
                Map.Entry<CandyKey, CandyLocator> e = it.next();
                return new Mutation(e.getKey(), e.getValue());
            }
        };
    }

    private static long estimateSize(RangeTombstone tombstone) {
        long size = 48L; // fixed overhead + HLC
        if (tombstone.startInclusive() != null) {
            size += tombstone.startInclusive().byteLength();
        }
        if (tombstone.endExclusive() != null) {
            size += tombstone.endExclusive().byteLength();
        }
        return size;
    }

    private static long estimateSize(CandyKey key, CandyLocator locator) {
        long size = key.byteLength() + 48L; // key + fixed locator overhead
        if (locator.contentType() != null) {
            size += locator.contentType().length();
        }
        for (Map.Entry<String, String> e : locator.userMetadata().entrySet()) {
            size += e.getKey().length() + e.getValue().length();
        }
        size += 24L * locator.segments().size();
        return size;
    }
}
