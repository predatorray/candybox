/*
 * Copyright (c) 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * <p>Three reclaim sources, each after a grace period (Pulsar-style — a margin for in-flight readers /
 * continuation tokens), via {@link LedgerStore#deleteLedger(long)}:
 * <ul>
 *   <li><b>SSTables</b> removed from the manifest by a committed compaction;</li>
 *   <li><b>Syrups</b> no longer referenced by any SSTable, the memtable, or the open write Syrup
 *       (dropped from the live set first via a fencing-gated manifest edit, then whole-ledger-deleted —
 *       v1 reclaims a Syrup only once every segment in it is dead; no defragmentation);</li>
 *   <li><b>WAL</b> ledgers rotated out at flush, whose mutations are now durable in an SSTable.</li>
 * </ul>
 *
 * <p>Safety: this runs only for a Box this node still owns (the caller checks ownership), so the
 * physical delete is effectively gated on the owner's fencing token, and the reclaimed ledgers are
 * already out of the committed manifest. Deletes are idempotent: a missing ledger is treated as gone.
 *
 * <p>TODO(phase-3+): an enumeration backstop so ledgers orphaned by a prior owner that crashed before
 * GC are eventually reclaimed (the pending sets are in-memory today).
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
        deleted += collectWals(engine, cutoff);
        return deleted;
    }

    private int collectWals(BoxEngine engine, long cutoff) {
        int deleted = 0;
        for (long ledgerId : engine.reclaimableWals(cutoff)) {
            if (deleteLedger(ledgerId)) {
                deleted++;
            }
            engine.forgetObsoleteWal(ledgerId);
        }
        if (deleted > 0) {
            LOG.debug("GC deleted {} rotated WAL ledger(s)", deleted);
        }
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
