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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reference-counted garbage collection of obsoleted ledgers.
 *
 * <p>Every ledger that leaves a partition's committed state is recorded in its manifest's pending
 * deletes, so the set survives handover; GC deletes each via {@link LedgerStore#deleteLedger(long)}
 * after a grace period (Pulsar-style — a margin for in-flight readers / continuation tokens), and only
 * then drops it from the set, so a failed delete is retried on a later pass. Pending deletes are:
 * <ul>
 *   <li><b>SSTables</b> removed from the manifest by a committed compaction;</li>
 *   <li><b>Syrups</b> no longer referenced by any SSTable, the memtable, an in-flight multipart
 *       upload, or the open write Syrup (claimed from the live set via a fencing-gated manifest edit —
 *       v1 reclaims a Syrup only once every segment in it is dead; no defragmentation);</li>
 *   <li><b>WAL</b> ledgers rotated out at flush, whose mutations are now durable in an SSTable, and
 *       the prior owner's WAL once nothing replayed from it is unflushed;</li>
 *   <li>the prior owner's <b>manifest</b> ledger, superseded at handover.</li>
 * </ul>
 *
 * <p>Safety: the physical delete is not itself fenced (only the claim and forget edits are), so even a
 * deposed owner that has not noticed its lease loss may delete. That is safe because:
 * <ol>
 *   <li>the manifest advances its in-memory state only after an append is acked, and a successor's
 *       recover-open includes every acked entry, so a deposed owner's pending set is a subset of every
 *       successor's;</li>
 *   <li>an id leaves the set only after its delete succeeded (deletes are idempotent);</li>
 *   <li>a ledger enters the set only once nothing uses it, and nothing uses it again: superseded
 *       WALs and manifests and compaction inputs are never reopened, and a claimed Syrup passed the
 *       Box-global reference gate. A change that could re-reference a pending ledger (e.g. a
 *       trivial-move compaction re-adding a table id) breaks this.</li>
 * </ol>
 * So any owner deletes only what every later owner also treats as dead.
 *
 * <p>The opt-in {@link LedgerRetention} reconciler covers allocations no manifest recorded.
 */
public final class GarbageCollector {

    private static final Logger LOG = LoggerFactory.getLogger(GarbageCollector.class);

    private final LedgerStore ledgerStore;
    private final long graceMillis;
    private final Clock clock;
    private final java.util.function.BiPredicate<BoxEngine, Long> deletionGate;

    public GarbageCollector(LedgerStore ledgerStore, long graceMillis, Clock clock) {
        this(ledgerStore, graceMillis, clock, (engine, ledger) -> true);
    }

    GarbageCollector(LedgerStore ledgerStore, long graceMillis, Clock clock,
                     java.util.function.BiPredicate<BoxEngine, Long> deletionGate) {
        this.deletionGate = deletionGate;
        this.ledgerStore = ledgerStore;
        this.graceMillis = graceMillis;
        this.clock = clock;
    }

    /**
     * Deletes the ledgers of {@code engine} that have been out of the committed manifest for at least
     * the grace period, claiming orphaned Syrups first.
     *
     * @return the number of ledgers deleted
     */
    public int collect(BoxEngine engine) {
        return collect(engine, Set.of());
    }

    /**
     * As {@link #collect(BoxEngine)}, but a Syrup is only physically reclaimed when it is referenced
     * by <em>no</em> partition of the Box: {@code foreignReferencedSyrups} is the union of every
     * sibling partition's published held-Syrup set, so a Syrup shared cross-partition by a
     * zero-copy copy/rename is never deleted out from under the partition that points at it. This is
     * the Box-global garbage collection of DESIGN §9. A held set also includes the sibling's orphans
     * younger than the grace period, which is what gives in-flight readers their margin after the
     * last foreign reference; raw reference sets give none.
     */
    public int collect(BoxEngine engine, Set<Long> foreignReferencedSyrups) {
        return collect(engine, foreignReferencedSyrups, Long.MAX_VALUE);
    }

    /**
     * As {@link #collect(BoxEngine, Set)}, with {@code foreignReferencedSyrups} read as of
     * {@code referencesAsOfMillis}: only Syrups orphaned at least the grace period before then are
     * claimed, so a Syrup orphaned after the references were read is never claimed against them.
     */
    public int collect(BoxEngine engine, Set<Long> foreignReferencedSyrups,
                       long referencesAsOfMillis) {
        long now = clock.currentTimeMillis();
        claimSyrups(engine, Math.min(now, referencesAsOfMillis) - graceMillis,
                foreignReferencedSyrups);
        return collectPendingDeletes(engine, now - graceMillis);
    }

    /**
     * Deletes only the already pending ledgers, for a Box whose Syrup references are unknown (no
     * Syrup is claimed; claimed ones were claimed against valid references).
     */
    public int collectWithoutSyrups(BoxEngine engine) {
        return collectPendingDeletes(engine, clock.currentTimeMillis() - graceMillis);
    }

    private void claimSyrups(BoxEngine engine, long cutoff, Set<Long> foreignReferencedSyrups) {
        // Box-global gate: a Syrup any sibling partition still references stays a pending orphan and is
        // retried on a later pass. The engine re-checks this partition's own references atomically with
        // moving the rest to the pending deletes (whole-ledger — v1 has no defragmentation).
        List<Long> claimed = engine.claimReclaimableSyrups(cutoff, foreignReferencedSyrups);
        if (!claimed.isEmpty()) {
            LOG.debug("GC claimed {} orphaned Syrup ledger(s)", claimed.size());
        }
    }

    private int collectPendingDeletes(BoxEngine engine, long cutoff) {
        List<Long> gone = new ArrayList<>();
        int deleted = 0;
        for (long ledgerId : engine.reclaimableLedgers(cutoff)) {
            try {
                if (!deletionGate.test(engine, ledgerId)) { continue; }
                ledgerStore.deleteLedger(ledgerId); // idempotent: an already-deleted ledger is gone
                deleted++;
                gone.add(ledgerId);
            } catch (RuntimeException e) {
                // Stays pending, so it is retried on a later pass.
                LOG.warn("GC delete of ledger {} failed: {}", ledgerId, e.getMessage());
            }
        }
        engine.forgetDeletedLedgers(gone);
        if (deleted > 0) {
            LOG.debug("GC deleted {} obsolete ledger(s)", deleted);
        }
        return deleted;
    }
}
