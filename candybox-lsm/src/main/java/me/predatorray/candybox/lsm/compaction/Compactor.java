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
 * resurrect the Candy.
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
            for (SSTableMeta input : task.inputs()) {
                SSTableReader reader = new SSTableReader(ledgerStore, input.ledgerId());
                readers.add(reader);
                sources.add(reader.scan(null));
                removed.add(input.ledgerId());
            }

            Iterator<Mutation> merged = new MergingIterator(sources, false);
            Iterator<Mutation> filtered = task.bottommost() ? dropAgedTombstones(merged) : merged;
            PeekingIterator<Mutation> peek = new PeekingIterator<>(filtered);

            if (!peek.hasNext()) {
                ManifestEdit edit = ManifestEdit.builder().removedTableLedgerIds(removed).build();
                return new CompactionResult(Optional.empty(), edit);
            }

            SSTableMeta output = writer.write(sstableConfig, task.outputLevel(), peek);
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

    private Iterator<Mutation> dropAgedTombstones(Iterator<Mutation> delegate) {
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
                        continue; // aged tombstone at bottommost level: safe to drop
                    }
                    return m;
                }
                return null;
            }
        };
    }
}
