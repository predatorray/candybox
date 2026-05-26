package me.predatorray.candybox.bookkeeper;

import java.util.List;
import me.predatorray.candybox.common.exception.StorageException;

/** A read view over a ledger. Reads are bounded by {@link #lastAddConfirmed()}. */
public interface ReadableLedger extends Ledger {

    /**
     * Reads a single entry.
     *
     * @param entryId entry id in {@code [0, lastAddConfirmed()]}
     * @return the entry
     * @throws StorageException if the entry id is out of range or the read fails
     */
    LedgerEntry read(long entryId);

    /**
     * Reads an inclusive range of entries.
     *
     * @param firstEntryId first entry id (inclusive)
     * @param lastEntryId  last entry id (inclusive); must be {@code <= lastAddConfirmed()}
     * @return the entries in order
     * @throws StorageException if the range is invalid or the read fails
     */
    List<LedgerEntry> readRange(long firstEntryId, long lastEntryId);
}
