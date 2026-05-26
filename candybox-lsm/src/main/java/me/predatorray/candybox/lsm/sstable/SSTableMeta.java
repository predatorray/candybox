package me.predatorray.candybox.lsm.sstable;

import me.predatorray.candybox.common.CandyKey;

/**
 * Manifest-level metadata describing one SSTable ledger: where it lives, its level, the key range it
 * covers, and how many entries it holds. The read path uses the range to skip non-overlapping tables
 * and the compaction strategy uses it to pick work.
 *
 * @param ledgerId   the SSTable ledger id
 * @param level      LSM level (0 for freshly flushed)
 * @param minKey     smallest CandyKey in the table
 * @param maxKey     largest CandyKey in the table
 * @param entryCount number of mutations (unique keys) in the table
 */
public record SSTableMeta(long ledgerId, int level, CandyKey minKey, CandyKey maxKey, long entryCount) {

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
