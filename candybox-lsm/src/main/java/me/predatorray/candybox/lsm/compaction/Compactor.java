package me.predatorray.candybox.lsm.compaction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.RangeTombstone;
import me.predatorray.candybox.lsm.iterator.MergingIterator;
import me.predatorray.candybox.lsm.iterator.PeekingIterator;
import me.predatorray.candybox.lsm.manifest.ManifestEdit;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;
import me.predatorray.candybox.lsm.sstable.SSTableReader;
import me.predatorray.candybox.lsm.sstable.SSTableWriter;

/**
 * Executes a {@link CompactionTask}: opens the input SSTables, merges them by LWW, applies the
 * tombstone-drop rule, writes the merged output SSTable, and returns the manifest edit to commit.
 *
 * <p>Tombstone-drop rule (LevelDB + late-write window): a DELETE is dropped only when the task is at
 * the bottommost level <em>and</em> the tombstone is older than the configured GC grace; otherwise it
 * is preserved so it keeps shadowing older PUTs in lower levels and so an in-flight late write cannot
 * resurrect the Candy. The same rule governs range tombstones: an aged range tombstone at the
 * bottommost level is dropped together with the (necessarily older) point locators it covers; younger
 * or non-bottommost range tombstones are carried forward into the output so they keep shadowing.
 *
 * <p>This is the reusable execution core; distributed scheduling (ZK task claims/leases) and the
 * fenced commit + reference-counted GC live in the server in Phase 3.
 */
public final class Compactor {

    private final LedgerStore ledgerStore;
    private final SSTableWriter writer;
    private final LedgerConfig sstableConfig;
    private final long tombstoneGcGraceMillis;
    private final Clock clock;

    public Compactor(LedgerStore ledgerStore, SSTableWriter writer, LedgerConfig sstableConfig,
                     long tombstoneGcGraceMillis, Clock clock) {
        this.ledgerStore = ledgerStore;
        this.writer = writer;
        this.sstableConfig = sstableConfig;
        this.tombstoneGcGraceMillis = tombstoneGcGraceMillis;
        this.clock = clock;
    }

    /** Runs the task and returns the output table (if any) plus the manifest edit. */
    public CompactionResult compact(CompactionTask task) {
        List<SSTableReader> readers = new ArrayList<>();
        Set<Long> removed = new LinkedHashSet<>();
        try {
            List<Iterator<Mutation>> sources = new ArrayList<>();
            List<RangeTombstone> inputRangeTombstones = new ArrayList<>();
            for (SSTableMeta input : task.inputs()) {
                SSTableReader reader = new SSTableReader(ledgerStore, input.ledgerId());
                readers.add(reader);
                sources.add(reader.scan(null));
                inputRangeTombstones.addAll(reader.rangeTombstones());
                removed.add(input.ledgerId());
            }

            long now = clock.currentTimeMillis();
            // At the bottommost level, aged range tombstones (and the points they cover) are dropped;
            // younger or non-bottommost ones are carried forward so they keep shadowing lower levels.
            List<RangeTombstone> dropping = new ArrayList<>();
            List<RangeTombstone> carriedForward = new ArrayList<>();
            for (RangeTombstone rt : inputRangeTombstones) {
                if (task.bottommost() && isAged(rt)) {
                    dropping.add(rt);
                } else {
                    carriedForward.add(rt);
                }
            }

            Iterator<Mutation> merged = new MergingIterator(sources, false);
            Iterator<Mutation> filtered = task.bottommost()
                    ? dropDeadEntries(merged, dropping) : merged;
            PeekingIterator<Mutation> peek = new PeekingIterator<>(filtered);

            if (!peek.hasNext() && carriedForward.isEmpty()) {
                ManifestEdit edit = ManifestEdit.builder().removedTableLedgerIds(removed).build();
                return new CompactionResult(Optional.empty(), edit);
            }

            SSTableMeta output = writer.write(sstableConfig, task.outputLevel(), peek, carriedForward);
            ManifestEdit edit = ManifestEdit.builder()
                    .addedTables(List.of(output))
                    .removedTableLedgerIds(removed)
                    .build();
            return new CompactionResult(Optional.of(output), edit);
        } finally {
            for (SSTableReader reader : readers) {
                reader.close();
            }
        }
    }

    /** Whether a range tombstone is older than the GC grace (uses its HLC's physical time). */
    private boolean isAged(RangeTombstone rt) {
        return clock.currentTimeMillis() - rt.hlc().physicalMillis() >= tombstoneGcGraceMillis;
    }

    /**
     * Drops, at the bottommost level: aged point tombstones, and any point locator covered by a
     * range tombstone that is being dropped this compaction (the covering tombstone has a higher HLC,
     * so the covered point is provably dead and safe to discard alongside it).
     */
    private Iterator<Mutation> dropDeadEntries(Iterator<Mutation> delegate,
                                               List<RangeTombstone> droppingRanges) {
        long now = clock.currentTimeMillis();
        return new Iterator<>() {
            private Mutation nextOut;
            private boolean computed;

            @Override
            public boolean hasNext() {
                if (!computed) {
                    nextOut = advance();
                    computed = true;
                }
                return nextOut != null;
            }

            @Override
            public Mutation next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }
                Mutation m = nextOut;
                nextOut = null;
                computed = false;
                return m;
            }

            private Mutation advance() {
                while (delegate.hasNext()) {
                    Mutation m = delegate.next();
                    if (m.isTombstone()
                            && now - m.locator().createdAtMillis() >= tombstoneGcGraceMillis) {
                        continue; // aged point tombstone at bottommost level: safe to drop
                    }
                    if (coveredByDropping(m)) {
                        continue; // covered by an aged range tombstone being dropped this round
                    }
                    return m;
                }
                return null;
            }

            private boolean coveredByDropping(Mutation m) {
                for (RangeTombstone rt : droppingRanges) {
                    if (rt.hlc().isAfter(m.hlc()) && rt.covers(m.key())) {
                        return true;
                    }
                }
                return false;
            }
        };
    }
}
