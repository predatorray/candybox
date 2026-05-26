package me.predatorray.candybox.server;

import me.predatorray.candybox.lsm.engine.BoxEngine;

/**
 * Reference-counted garbage collection of obsoleted ledgers. Scaffolded in this run; implemented in
 * Phase 3.
 *
 * <p>Design (documented now so the call sites exist):
 * <ul>
 *   <li>Runs <b>only by the Box's manifest owner against a committed manifest snapshot</b> (never a
 *       stale tail), with every deletion edit and physical ledger delete gated on the owner's fencing
 *       token.</li>
 *   <li>An SSTable/Syrup is deleted only after a grace period (Pulsar-style), once no committed
 *       manifest references it.</li>
 *   <li>Orphaned-Syrup reclamation is driven by a <b>pending-orphan list</b> recorded at
 *       supersede/dedupe time (a retried or conflicting put knows its losing segments immediately); a
 *       full live-locator scan is only a backstop.</li>
 *   <li>v1 reclaims a Syrup only once <b>every</b> segment in it is dead (whole-ledger delete);
 *       Syrup defragmentation (copying survivors into a fresh Syrup) is deferred.</li>
 * </ul>
 *
 * @see CompactionService
 */
public final class GarbageCollector {

    /**
     * Performs one GC pass for the Box owned via {@code engine}.
     *
     * @return the number of ledgers deleted
     */
    public int collectOnce(BoxEngine engine) {
        // TODO(phase-3): compute the set of ledgers no longer referenced by the committed manifest
        //   (SSTables removed by compaction, Syrups on the pending-orphan list with all segments dead),
        //   honour the grace period and the bottommost+time-bound tombstone rule, then delete each via
        //   LedgerStore.deleteLedger gated on the owner's fencing token.
        return 0;
    }
}
