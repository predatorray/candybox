package me.predatorray.candybox.bookkeeper;

import java.util.Map;

/**
 * A handle on a ledger, shared by the readable and writable views. A ledger has at most one writer in
 * its lifetime and becomes immutable ("sealed") once closed or recovered.
 *
 * <p>Implementations must be safe for use by a single owning thread of control; concurrent reads on a
 * {@link ReadableLedger} are permitted.
 */
public interface Ledger extends AutoCloseable {

    /** The unique ledger id. */
    long ledgerId();

    /**
     * The last-add-confirmed entry id: the highest entry id durably acknowledged. {@code -1} for an
     * empty ledger. After a {@code recover-open} this is the deterministic recovered tail.
     */
    long lastAddConfirmed();

    /** Whether the ledger is sealed (closed or recovered) and can no longer accept appends. */
    boolean isSealed();

    /** Immutable custom metadata stamped on the ledger at creation. */
    Map<String, byte[]> customMetadata();

    /** Closes this handle, sealing the ledger if this handle is the writer. Idempotent. */
    @Override
    void close();
}
