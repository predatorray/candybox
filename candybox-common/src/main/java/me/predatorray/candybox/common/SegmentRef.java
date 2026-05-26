package me.predatorray.candybox.common;

/**
 * A contiguous run of entries in one Syrup holding part (or all) of a Candy's bytes.
 *
 * <p>With a fixed {@code chunkSize} and contiguous chunk entries, a whole Candy is described by an
 * O(number of Syrups) list of these — not O(number of chunks).
 *
 * @param syrupId      the Syrup (data ledger) id
 * @param firstEntryId first entry id (inclusive) holding this Candy's bytes in the Syrup
 * @param lastEntryId  last entry id (inclusive)
 */
public record SegmentRef(long syrupId, long firstEntryId, long lastEntryId) {

    public SegmentRef {
        if (firstEntryId < 0 || lastEntryId < firstEntryId) {
            throw new IllegalArgumentException("Invalid segment entry range: [" + firstEntryId + ", "
                    + lastEntryId + "]");
        }
    }

    /** Number of chunk entries in this segment. */
    public long entryCount() {
        return lastEntryId - firstEntryId + 1;
    }
}
