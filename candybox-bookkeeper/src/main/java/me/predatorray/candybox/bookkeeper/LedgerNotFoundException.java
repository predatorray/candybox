package me.predatorray.candybox.bookkeeper;

import me.predatorray.candybox.common.exception.StorageException;

/** Thrown when opening, reading, or deleting a ledger id that does not exist. */
public class LedgerNotFoundException extends StorageException {

    public LedgerNotFoundException(long ledgerId) {
        super("No such ledger: " + ledgerId);
    }
}
