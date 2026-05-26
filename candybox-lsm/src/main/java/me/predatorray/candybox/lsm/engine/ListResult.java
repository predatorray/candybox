package me.predatorray.candybox.lsm.engine;

import java.util.List;
import me.predatorray.candybox.common.CandyKey;

/**
 * A page of {@code listCandies} results over the merged, tombstone-suppressed view.
 *
 * @param entries        the keys on this page, ascending
 * @param nextStartAfter continuation cursor (pass as {@code startAfter} to resume), or {@code null}
 *                       when the listing is exhausted
 */
public record ListResult(List<ListEntry> entries, String nextStartAfter) {

    public ListResult {
        entries = List.copyOf(entries);
    }

    public boolean isTruncated() {
        return nextStartAfter != null;
    }

    /**
     * One listing row.
     *
     * @param key             the CandyKey
     * @param contentLength   length in bytes
     * @param createdAtMillis creation time
     */
    public record ListEntry(CandyKey key, long contentLength, long createdAtMillis) {
    }
}
