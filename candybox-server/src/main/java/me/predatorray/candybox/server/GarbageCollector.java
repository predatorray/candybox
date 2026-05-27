package me.predatorray.candybox.server;

import java.util.List;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.exception.StorageException;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reference-counted garbage collection of obsoleted ledgers.
 *
 * <p>WS2 reclaims <b>SSTable</b> ledgers that a committed compaction removed from the manifest: the
 * engine records each removed ledger (with the time it left the committed manifest), and after a grace
 * period (Pulsar-style — leaving a margin for in-flight readers / continuation tokens) the
 * {@link GarbageCollector} physically deletes it via {@link LedgerStore#deleteLedger(long)}.
 *
 * <p>Safety: this runs only for a Box this node still owns (the caller checks ownership), so the
 * physical delete is effectively gated on the owner's fencing token. The removed ledgers are already
 * out of the committed manifest (the compaction commit passed the manifest fence), so no live reader
 * resolves them. Deletes are idempotent: a missing ledger is treated as already gone.
 *
 * <p>TODO(phase-3 WS3/WS4): orphaned-Syrup reclamation (per-SSTable referenced-Syrup tracking) and WAL
 * GC. v1 reclaims a Syrup only once every segment in it is dead (whole-ledger delete; no defrag).
 */
public final class GarbageCollector {

    private static final Logger LOG = LoggerFactory.getLogger(GarbageCollector.class);

    private final LedgerStore ledgerStore;
    private final long graceMillis;
    private final Clock clock;

    public GarbageCollector(LedgerStore ledgerStore, long graceMillis, Clock clock) {
        this.ledgerStore = ledgerStore;
        this.graceMillis = graceMillis;
        this.clock = clock;
    }

    /**
     * Deletes obsolete SSTable ledgers for {@code engine} that have been out of the committed manifest
     * for at least the grace period.
     *
     * @return the number of ledgers deleted
     */
    public int collect(BoxEngine engine) {
        long cutoff = clock.currentTimeMillis() - graceMillis;
        int deleted = collectSSTables(engine, cutoff);
        deleted += collectSyrups(engine, cutoff);
        return deleted;
    }

    private int collectSSTables(BoxEngine engine, long cutoff) {
        int deleted = 0;
        for (long ledgerId : engine.reclaimableSSTables(cutoff)) {
            if (deleteLedger(ledgerId)) {
                deleted++;
            }
            engine.forgetObsoleteSSTable(ledgerId);
        }
        if (deleted > 0) {
            LOG.debug("GC deleted {} obsolete SSTable ledger(s)", deleted);
        }
        return deleted;
    }

    private int collectSyrups(BoxEngine engine, long cutoff) {
        List<Long> orphans = engine.reclaimableSyrups(cutoff);
        if (orphans.isEmpty()) {
            return 0;
        }
        // Drop them from the live set first via a fencing-gated manifest edit; then delete the ledgers
        // (a Syrup is removed whole only once every segment in it is dead — v1 has no defragmentation).
        engine.dropSyrups(orphans);
        int deleted = 0;
        for (long syrupId : orphans) {
            if (deleteLedger(syrupId)) {
                deleted++;
            }
        }
        LOG.debug("GC deleted {} orphaned Syrup ledger(s)", deleted);
        return deleted;
    }

    private boolean deleteLedger(long ledgerId) {
        try {
            ledgerStore.deleteLedger(ledgerId);
            return true;
        } catch (StorageException e) {
            // Already gone (or transient): a real failure is retried on a later pass.
            LOG.debug("GC delete of ledger {} did not succeed cleanly: {}", ledgerId, e.getMessage());
            return false;
        }
    }
}
