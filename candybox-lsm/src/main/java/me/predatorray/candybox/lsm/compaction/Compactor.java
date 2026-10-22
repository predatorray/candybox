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
package me.predatorray.candybox.lsm.compaction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
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
 * tombstone-drop rule, writes the merged output as one or more key-disjoint SSTables of about
 * {@code targetFileSizeBytes} each, and returns the manifest edit to commit.
 *
 * <p>Tombstone-drop rule (LevelDB + late-write window): a DELETE is dropped only when the task is at
 * the bottommost level <em>and</em> the tombstone is older than the configured GC grace; otherwise it
 * is preserved so it keeps shadowing older PUTs in lower levels and so an in-flight late write cannot
 * resurrect the Candy. The same rule governs range tombstones: an aged range tombstone at the
 * bottommost level is dropped together with the (necessarily older) point locators it covers; younger
 * or non-bottommost range tombstones are carried forward into the output so they keep shadowing. An
 * aged range tombstone is also kept while a table outside the task overlaps its range, since that
 * table may hold points it still has to shadow. Carried range tombstones go into the first output.
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
    private final long targetFileSizeBytes;

    /** A compactor with no target output size; it still splits output to keep meta blocks small. */
    public Compactor(LedgerStore ledgerStore, SSTableWriter writer, LedgerConfig sstableConfig,
                     long tombstoneGcGraceMillis, Clock clock) {
        this(ledgerStore, writer, sstableConfig, tombstoneGcGraceMillis, clock, Long.MAX_VALUE);
    }

    public Compactor(LedgerStore ledgerStore, SSTableWriter writer, LedgerConfig sstableConfig,
                     long tombstoneGcGraceMillis, Clock clock, long targetFileSizeBytes) {
        if (targetFileSizeBytes <= 0) {
            throw new IllegalArgumentException("targetFileSizeBytes must be > 0");
        }
        this.ledgerStore = ledgerStore;
        this.writer = writer;
        this.sstableConfig = sstableConfig;
        this.tombstoneGcGraceMillis = tombstoneGcGraceMillis;
        this.clock = clock;
        this.targetFileSizeBytes = targetFileSizeBytes;
    }

    /** Runs the task and returns the output tables (possibly none) plus the manifest edit. */
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
                if (task.bottommost() && isAged(rt) && !overlapsOtherTable(rt, task)) {
                    dropping.add(rt);
                } else {
                    carriedForward.add(rt);
                }
            }

            Iterator<Mutation> merged = new MergingIterator(sources, false);
            Iterator<Mutation> filtered = task.bottommost()
                    ? dropDeadEntries(merged, dropping) : merged;
            PeekingIterator<Mutation> peek = new PeekingIterator<>(filtered);

            List<SSTableMeta> outputs = new ArrayList<>();
            List<RangeTombstone> pendingRanges = carriedForward;
            try {
                while (peek.hasNext() || !pendingRanges.isEmpty()) {
                    outputs.add(writer.write(sstableConfig, task.outputLevel(), peek, pendingRanges,
                            targetFileSizeBytes));
                    pendingRanges = List.of();
                }
            } catch (RuntimeException e) {
                // The edit was never returned, so nothing will reference or GC these ledgers.
                for (SSTableMeta output : outputs) {
                    deleteQuietly(output.ledgerId(), e);
                }
                throw e;
            }
            ManifestEdit edit = ManifestEdit.builder()
                    .addedTables(outputs)
                    .removedTableLedgerIds(removed)
                    .build();
            return new CompactionResult(outputs, edit);
        } finally {
            for (SSTableReader reader : readers) {
                reader.close();
            }
        }
    }

    private void deleteQuietly(long ledgerId, RuntimeException cause) {
        try {
            ledgerStore.deleteLedger(ledgerId);
        } catch (RuntimeException e) {
            cause.addSuppressed(e);
        }
    }

    private static boolean overlapsOtherTable(RangeTombstone rt, CompactionTask task) {
        for (SSTableMeta t : task.otherTables()) {
            boolean afterStart = rt.startInclusive() == null
                    || t.maxKey().compareTo(rt.startInclusive()) >= 0;
            boolean beforeEnd = rt.endExclusive() == null
                    || t.minKey().compareTo(rt.endExclusive()) < 0;
            if (afterStart && beforeEnd) {
                return true;
            }
        }
        return false;
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
