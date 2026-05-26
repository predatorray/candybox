package me.predatorray.candybox.bookkeeper;

import me.predatorray.candybox.common.exception.FencedException;
import me.predatorray.candybox.common.exception.StorageException;

/**
 * The single-writer view over a ledger. The writer may also read back entries it has added.
 *
 * <p>Once another actor recovers (fences) this ledger, {@link #append(byte[])} fails with
 * {@link FencedException} — the property the whole zombie-owner/zombie-compactor defense rests on.
 */
public interface WritableLedger extends ReadableLedger {

    /**
     * Appends an entry, blocking until it is acknowledged by the configured ack-quorum.
     *
     * @param data the entry payload
     * @return the assigned entry id
     * @throws FencedException if the ledger has been fenced/sealed by a recovery
     * @throws StorageException if ack-quorum could not be met or the append otherwise failed
     */
    long append(byte[] data);
}
