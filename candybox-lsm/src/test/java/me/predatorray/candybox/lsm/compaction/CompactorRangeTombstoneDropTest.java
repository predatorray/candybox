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

import static me.predatorray.candybox.lsm.TestData.hlc;
import static me.predatorray.candybox.lsm.TestData.put;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.RangeTombstone;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.lsm.iterator.MergingIterator;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;
import me.predatorray.candybox.lsm.sstable.SSTableReader;
import me.predatorray.candybox.lsm.sstable.SSTableWriter;
import org.junit.jupiter.api.Test;

class CompactorRangeTombstoneDropTest {

    private static final long GRACE = 100;
    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private final LedgerConfig ledgerConfig = LedgerConfig.forRole(LedgerRole.SSTABLE);
    private final SSTableWriter writer = new SSTableWriter(store, 10, 256);
    private final ManualClock clock = new ManualClock(5);
    private final Compactor compactor = new Compactor(store, writer, ledgerConfig, GRACE, clock);

    @Test
    void dropsRangeWhenOutsideTableIsEntirelyBeforeStart() {
        assertDecision(range("m", "t"), outside("a", "c"), false, "a", "c", "zz");
    }

    @Test
    void dropsRangeWhenOutsideTableStartsAtExclusiveEnd() {
        assertDecision(range("m", "t"), outside("t", "z"), false, "t", "z", "zz");
    }

    @Test
    void keepsRangeWhenOutsideTableTouchesInclusiveStart() {
        assertDecision(range("m", "t"), outside("k", "m"), true, "k", "zz");
    }

    @Test
    void keepsUnboundedStartWhenOutsideTableIsBelowEnd() {
        assertDecision(range(null, "d"), outside("a", "b"), true, "zz");
    }

    @Test
    void dropsUnboundedStartWhenOutsideTableIsBeyondEnd() {
        assertDecision(range(null, "d"), outside("x", "z"), false, "x", "z", "zz");
    }

    @Test
    void keepsUnboundedEndWhenOutsideTableIsAboveStart() {
        assertDecision(range("w", null), outside("x", "z"), true, "zz");
    }

    @Test
    void dropsUnboundedEndWhenOutsideTableIsBeforeStart() {
        assertDecision(range("w", null), outside("a", "c"), false, "a", "c", "zz");
    }

    @Test
    void keepsRangeWhenLegacyOutsideMetadataHasUnknownPointAge() {
        SSTableMeta written = outside("n", "s");
        // Pre-v5 manifests did not persist minPointHlc; use the compatibility constructor.
        SSTableMeta legacy = new SSTableMeta(written.ledgerId(), written.level(), written.minKey(),
                written.maxKey(), written.entryCount(), written.sizeBytes(), written.referencedSyrups());
        assertThat(legacy.minPointHlc()).isNull();
        assertDecision(range("m", "t"), legacy, true, "zz");
    }

    @Test
    void dropsRangeWhenOverlappingOutsidePointsAreNewer() {
        // Both outside points at HLC 6 postdate the deletion at HLC 5.
        SSTableMeta newer = write(List.of(point("n", 6), point("s", 6)), List.of());
        assertDecision(range("m", "t"), newer, false, "n", "s", "zz");
    }

    @Test
    void ignoresEmptyOutsideTableEvenWhenItsKeyRangeOverlaps() {
        SSTableMeta empty = write(List.of(), List.of(range("n", "s")));
        assertThat(empty.entryCount()).isZero();
        assertThat(empty.minKey()).isEqualTo(CandyKey.of("n"));
        assertThat(empty.maxKey()).isEqualTo(CandyKey.of("n"));
        assertDecision(range("m", "t"), empty, false, "zz");
    }

    @Test
    void removingEveryPointAndRangeCreatesNoOutputOrLedger() {
        // The deletion at HLC 5 shadows both older points at HLC 1.
        SSTableMeta input = write(List.of(point("b", 1), point("c", 1)),
                List.of(range("a", "z")));
        List<Long> ledgersBefore = store.listLedgers();
        clock.advance(GRACE + 1);

        CompactionResult result = compactor.compact(
                new CompactionTask(List.of(input), 2, true, List.of()));

        assertThat(result.outputs()).isEmpty();
        assertThat(result.edit().removedTableLedgerIds()).containsExactly(input.ledgerId());
        assertThat(result.edit().addedTables()).isEmpty();
        assertThat(store.listLedgers()).containsExactlyInAnyOrderElementsOf(ledgersBefore);
    }

    private void assertDecision(RangeTombstone tombstone, SSTableMeta outside,
                                boolean kept, String... visibleKeys) {
        // HLC 10 survives the range at HLC 5; the outside points at HLC 1 must stay shadowed.
        SSTableMeta input = write(List.of(point("zz", 10)), List.of(tombstone));
        clock.advance(GRACE + 1);

        CompactionResult result = compactor.compact(
                new CompactionTask(List.of(input), 2, true, List.of(outside)));

        assertThat(result.outputs()).hasSize(1);
        for (SSTableMeta output : result.outputs()) {
            try (SSTableReader reader = new SSTableReader(store, output.ledgerId())) {
                if (kept) {
                    assertThat(reader.rangeTombstones()).containsExactly(tombstone);
                } else {
                    assertThat(reader.rangeTombstones()).isEmpty();
                }
            }
        }
        List<SSTableMeta> tables = new ArrayList<>(result.outputs());
        tables.add(outside);
        assertThat(mergedVisibleKeys(tables)).containsExactly(visibleKeys);
    }

    /** Merge all point sources, then apply all ranges, as the engine's read path does. */
    private List<String> mergedVisibleKeys(List<SSTableMeta> tables) {
        List<SSTableReader> readers = new ArrayList<>();
        try {
            List<Iterator<Mutation>> sources = new ArrayList<>();
            List<RangeTombstone> ranges = new ArrayList<>();
            for (SSTableMeta table : tables) {
                SSTableReader reader = new SSTableReader(store, table.ledgerId());
                readers.add(reader);
                sources.add(reader.scan(null));
                ranges.addAll(reader.rangeTombstones());
            }
            MergingIterator merged = new MergingIterator(sources, true);
            List<String> visible = new ArrayList<>();
            while (merged.hasNext()) {
                Mutation point = merged.next();
                boolean shadowed = ranges.stream().anyMatch(rt -> rt.covers(point.key())
                        && rt.hlc().isAfter(point.hlc()));
                if (!shadowed) {
                    visible.add(point.key().value());
                }
            }
            return visible;
        } finally {
            readers.forEach(SSTableReader::close);
        }
    }

    private SSTableMeta outside(String first, String last) {
        // Outside points predate the deletion at HLC 5.
        return write(List.of(point(first, 1), point(last, 1)), List.of());
    }

    private SSTableMeta write(List<Mutation> points, List<RangeTombstone> ranges) {
        return writer.write(ledgerConfig, 1, points.iterator(), ranges);
    }

    private static Mutation point(String key, long physicalMillis) {
        return new Mutation(CandyKey.of(key), put(hlc(physicalMillis, 0, 1), 1, 5));
    }

    private static RangeTombstone range(String start, String end) {
        return new RangeTombstone(start == null ? null : CandyKey.of(start),
                end == null ? null : CandyKey.of(end), hlc(5, 0, 1));
    }
}
