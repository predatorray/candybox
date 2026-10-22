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
package me.predatorray.candybox.lsm.sstable;

import java.util.Set;
import me.predatorray.candybox.common.CandyKey;

/**
 * Manifest-level metadata describing one SSTable ledger: where it lives, its level, the key range it
 * covers, how many entries it holds, its approximate data size, and the Syrups its locators point
 * into. The read path uses the range to skip non-overlapping tables; the compaction strategy uses the
 * level and size to pick work; GC uses {@code referencedSyrups} to find orphaned Syrups.
 *
 * @param ledgerId         the SSTable ledger id
 * @param level            LSM level (0 for freshly flushed)
 * @param minKey           smallest CandyKey in the table
 * @param maxKey           largest CandyKey in the table
 * @param entryCount       number of mutations (unique keys) in the table
 * @param sizeBytes        approximate on-ledger data size (sum of data-block bytes), for byte-size scoring
 * @param referencedSyrups Syrup ledger ids this table's locators reference (empty for an all-tombstone run)
 * @param rangeTombstoneSpan the union of the keys this table's range tombstones cover, or {@code null}
 *                         if it has none
 */
public record SSTableMeta(long ledgerId, int level, CandyKey minKey, CandyKey maxKey, long entryCount,
                          long sizeBytes, Set<Long> referencedSyrups, KeySpan rangeTombstoneSpan) {

    public SSTableMeta {
        referencedSyrups = referencedSyrups == null ? Set.of() : Set.copyOf(referencedSyrups);
    }

    /** A table without range tombstones. */
    public SSTableMeta(long ledgerId, int level, CandyKey minKey, CandyKey maxKey, long entryCount,
                       long sizeBytes, Set<Long> referencedSyrups) {
        this(ledgerId, level, minKey, maxKey, entryCount, sizeBytes, referencedSyrups, null);
    }

    /**
     * Every key this table can affect: its point keys plus the keys its range tombstones cover.
     * Compaction picks inputs by this span so a range tombstone meets the points it shadows.
     */
    public KeySpan span() {
        if (rangeTombstoneSpan == null) {
            return KeySpan.closed(minKey, maxKey);
        }
        // A range-only table's min/max are a placeholder, not data.
        return entryCount == 0 ? rangeTombstoneSpan
                : KeySpan.closed(minKey, maxKey).union(rangeTombstoneSpan);
    }

    /** Whether this table's key range overlaps {@code [from, to]} (inclusive bounds, nulls = unbounded). */
    public boolean overlaps(CandyKey from, CandyKey to) {
        if (from != null && maxKey.compareTo(from) < 0) {
            return false;
        }
        if (to != null && minKey.compareTo(to) > 0) {
            return false;
        }
        return true;
    }

    /** Whether {@code key} falls within this table's key range (necessary, not sufficient, for presence). */
    public boolean mayContain(CandyKey key) {
        return key.compareTo(minKey) >= 0 && key.compareTo(maxKey) <= 0;
    }
}
