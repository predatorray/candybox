package me.predatorray.candybox.bookkeeper.bk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import me.predatorray.candybox.bookkeeper.Ledger;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerEntry;
import me.predatorray.candybox.bookkeeper.LedgerNotFoundException;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.exception.FencedException;
import me.predatorray.candybox.common.exception.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The real {@link LedgerStore}, backed by an Apache BookKeeper client. This is the only class in
 * Candybox that touches the raw BookKeeper API.
 *
 * <p>Mapping of the SPI onto BookKeeper:
 * <ul>
 *   <li>{@link #openLedger(long)} → {@code openLedgerNoRecovery} (passive, no fence).</li>
 *   <li>{@link #recoverOpen(long)} → {@code openLedger} (recovers and fences).</li>
 *   <li>{@link WritableLedger#append(byte[])} on a fenced ledger surfaces BookKeeper's
 *       {@code LedgerFencedException} as a Candybox {@link FencedException}.</li>
 * </ul>
 *
 * <p>All operations are synchronous; async batching is left to a later phase.
 */
public final class BookKeeperLedgerStore implements LedgerStore {

    private static final Logger LOG = LoggerFactory.getLogger(BookKeeperLedgerStore.class);
    private static final BookKeeper.DigestType DIGEST = BookKeeper.DigestType.CRC32C;

    private final BookKeeper bookKeeper;
    private final ClientConfiguration clientConfiguration;
    private final byte[] password;
    private final boolean ownsClient;

    /**
     * Builds a store, creating and owning a BookKeeper client from {@code clientConfiguration}.
     *
     * @param clientConfiguration BK client config (ZooKeeper connect string, etc.)
     * @param password            ledger password (may be empty)
     */
    /**
     * Builds a store from a BookKeeper {@code metadataServiceUri} (e.g. {@code zk://host:2181/ledgers}),
     * constructing and owning the BookKeeper client. This keeps the raw BookKeeper
     * {@link ClientConfiguration} entirely inside this module — callers (the server's composition root)
     * need only a connect-string URI, preserving the "only this module touches the raw BK client"
     * invariant.
     */
    public static BookKeeperLedgerStore create(String metadataServiceUri, byte[] password) {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(metadataServiceUri);
        return new BookKeeperLedgerStore(conf, password);
    }

    public BookKeeperLedgerStore(ClientConfiguration clientConfiguration, byte[] password) {
        this.clientConfiguration = clientConfiguration;
        this.password = password.clone();
        try {
            this.bookKeeper = new BookKeeper(clientConfiguration);
        } catch (IOException | InterruptedException | BKException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new StorageException("Failed to start BookKeeper client", e);
        }
        this.ownsClient = true;
    }

    /** Builds a store over an externally managed BookKeeper client (the caller closes it). */
    public BookKeeperLedgerStore(BookKeeper bookKeeper, ClientConfiguration clientConfiguration,
                                 byte[] password) {
        this.bookKeeper = bookKeeper;
        this.clientConfiguration = clientConfiguration;
        this.password = password.clone();
        this.ownsClient = false;
    }

    @Override
    public WritableLedger createLedger(LedgerConfig config) {
        try {
            LedgerHandle lh = bookKeeper.createLedger(
                    config.quorum().ensembleSize(),
                    config.quorum().writeQuorum(),
                    config.quorum().ackQuorum(),
                    DIGEST,
                    password,
                    config.customMetadata());
            LOG.debug("Created ledger {} with quorum {}", lh.getId(), config.quorum());
            return new BkWritableHandle(lh);
        } catch (BKException | InterruptedException e) {
            throw mapException("create ledger", -1, e);
        }
    }

    @Override
    public ReadableLedger openLedger(long ledgerId) {
        try {
            LedgerHandle lh = bookKeeper.openLedgerNoRecovery(ledgerId, DIGEST, password);
            return new BkReadableHandle(lh);
        } catch (BKException | InterruptedException e) {
            throw mapException("open ledger", ledgerId, e);
        }
    }

    @Override
    public ReadableLedger recoverOpen(long ledgerId) {
        try {
            // openLedger performs recovery: it fences the ledger and seals it at the recovered tail.
            LedgerHandle lh = bookKeeper.openLedger(ledgerId, DIGEST, password);
            return new BkReadableHandle(lh);
        } catch (BKException | InterruptedException e) {
            throw mapException("recover-open ledger", ledgerId, e);
        }
    }

    @Override
    public void deleteLedger(long ledgerId) {
        try {
            bookKeeper.deleteLedger(ledgerId);
        } catch (BKException | InterruptedException e) {
            throw mapException("delete ledger", ledgerId, e);
        }
    }

    @Override
    public List<Long> listLedgers() {
        List<Long> result = new ArrayList<>();
        try (BookKeeperAdmin admin = new BookKeeperAdmin(clientConfiguration)) {
            for (Long id : admin.listLedgers()) {
                result.add(id);
            }
        } catch (IOException | InterruptedException | BKException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new StorageException("Failed to list ledgers", e);
        }
        result.sort(Long::compareTo);
        return result;
    }

    @Override
    public void close() {
        if (ownsClient) {
            try {
                bookKeeper.close();
            } catch (BKException | InterruptedException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new StorageException("Failed to close BookKeeper client", e);
            }
        }
    }

    private static CandyboxException mapException(String op, long ledgerId, Exception e) {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return new StorageException("Interrupted during " + op, e);
        }
        if (e instanceof BKException bke) {
            int code = bke.getCode();
            if (code == BKException.Code.LedgerFencedException
                    || code == BKException.Code.LedgerClosedException) {
                // Both mean the ledger is sealed and will not accept appends — surface as fencing.
                return new FencedException("Ledger " + ledgerId + " is sealed/fenced; " + op
                        + " rejected");
            }
            if (code == BKException.Code.NoSuchLedgerExistsException
                    || code == BKException.Code.NoSuchLedgerExistsOnMetadataServerException) {
                return new LedgerNotFoundException(ledgerId);
            }
        }
        return new StorageException("BookKeeper " + op + " failed for ledger " + ledgerId, e);
    }

    // -------------------------------------------------------------------------------------------

    private abstract static class BkBaseHandle implements Ledger {
        final LedgerHandle lh;

        BkBaseHandle(LedgerHandle lh) {
            this.lh = lh;
        }

        @Override
        public long ledgerId() {
            return lh.getId();
        }

        @Override
        public long lastAddConfirmed() {
            return lh.getLastAddConfirmed();
        }

        @Override
        public boolean isSealed() {
            return lh.isClosed();
        }

        @Override
        public Map<String, byte[]> customMetadata() {
            return lh.getLedgerMetadata().getCustomMetadata();
        }

        @Override
        public void close() {
            try {
                lh.close();
            } catch (BKException | InterruptedException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new StorageException("Failed to close ledger " + lh.getId(), e);
            }
        }
    }

    private static class BkReadableHandle extends BkBaseHandle implements ReadableLedger {
        BkReadableHandle(LedgerHandle lh) {
            super(lh);
        }

        @Override
        public LedgerEntry read(long entryId) {
            List<LedgerEntry> entries = readRange(entryId, entryId);
            return entries.get(0);
        }

        @Override
        public List<LedgerEntry> readRange(long firstEntryId, long lastEntryId) {
            if (firstEntryId < 0 || lastEntryId < firstEntryId) {
                throw new StorageException("Invalid read range [" + firstEntryId + ", " + lastEntryId
                        + "]");
            }
            try {
                List<LedgerEntry> out = new ArrayList<>();
                // readUnconfirmedEntries fetches entries by id directly from bookies, which lets us
                // read durably-written entries from a ledger that is still open (e.g. an active Syrup)
                // whose read handle's LAC has not yet caught up. Callers always pass known-valid ids.
                Enumeration<org.apache.bookkeeper.client.LedgerEntry> e =
                        lh.readUnconfirmedEntries(firstEntryId, lastEntryId);
                while (e.hasMoreElements()) {
                    org.apache.bookkeeper.client.LedgerEntry be = e.nextElement();
                    out.add(new LedgerEntry(be.getEntryId(), be.getEntry()));
                }
                return out;
            } catch (BKException | InterruptedException ex) {
                throw mapException("read ledger", lh.getId(), ex);
            }
        }
    }

    private static final class BkWritableHandle extends BkReadableHandle implements WritableLedger {
        BkWritableHandle(LedgerHandle lh) {
            super(lh);
        }

        @Override
        public long append(byte[] data) {
            try {
                return lh.addEntry(data);
            } catch (BKException | InterruptedException e) {
                throw mapException("append to ledger", lh.getId(), e);
            }
        }
    }
}
