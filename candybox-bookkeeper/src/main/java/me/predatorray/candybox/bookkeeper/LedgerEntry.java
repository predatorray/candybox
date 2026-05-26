package me.predatorray.candybox.bookkeeper;

/**
 * A single ledger entry: its id within the ledger and its raw payload bytes.
 *
 * @param entryId zero-based entry id within the ledger
 * @param data    the entry payload (treat as read-only)
 */
public record LedgerEntry(long entryId, byte[] data) {

    public LedgerEntry {
        if (entryId < 0) {
            throw new IllegalArgumentException("entryId must be non-negative: " + entryId);
        }
        if (data == null) {
            throw new IllegalArgumentException("entry data must not be null");
        }
    }
}
