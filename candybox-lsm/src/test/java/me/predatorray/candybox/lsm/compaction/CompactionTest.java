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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerEntry;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.RangeTombstone;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.lsm.manifest.ManifestEdit;
import me.predatorray.candybox.lsm.manifest.ManifestState;
import me.predatorray.candybox.lsm.sstable.KeySpan;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;
import me.predatorray.candybox.lsm.sstable.SSTableReader;
import me.predatorray.candybox.lsm.sstable.SSTableWriter;
import org.junit.jupiter.api.Test;

class CompactionTest {

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private final LedgerConfig cfg = LedgerConfig.forRole(LedgerRole.SSTABLE);
    private final SSTableWriter writer = new SSTableWriter(store, 10, 256);

    private SSTableMeta write(int level, List<Mutation> sorted) {
        return writer.write(cfg, level, sorted.iterator());
    }

    private SSTableMeta write(int level, List<Mutation> sorted, List<RangeTombstone> rangeTombstones) {
        return writer.write(cfg, level, sorted.iterator(), rangeTombstones);
    }

    private List<String> keysOf(SSTableMeta meta) {
        List<String> keys = new ArrayList<>();
        try (SSTableReader r = new SSTableReader(store, meta.ledgerId())) {
            var it = r.scan(null);
            while (it.hasNext()) {
                keys.add(it.next().key().value());
            }
        }
        return keys;
    }

    private List<RangeTombstone> rangeTombstonesOf(SSTableMeta meta) {
        try (SSTableReader r = new SSTableReader(store, meta.ledgerId())) {
            return r.rangeTombstones();
        }
    }

    @Test
    void leveledStrategyTriggersOnL0Count() {
        LeveledCompactionStrategy strategy = new LeveledCompactionStrategy(4);

        ManifestState below = ManifestState.empty().apply(ManifestEdit.builder()
                .addedTables(List.of(meta(1, 0), meta(2, 0), meta(3, 0))).build());
        assertThat(strategy.pickCompaction(below)).isEmpty();

        ManifestState atTrigger = below.apply(ManifestEdit.builder()
                .addedTables(List.of(meta(4, 0))).build());
        Optional<CompactionTask> task = strategy.pickCompaction(atTrigger);
        assertThat(task).isPresent();
        assertThat(task.get().outputLevel()).isEqualTo(1);
        assertThat(task.get().bottommost()).isTrue();
        assertThat(task.get().inputs()).hasSize(4);
    }

    @Test
    void leveledStrategyTriggersOnLevelByteBudget() {
        // L0 trigger high so only the byte budget matters; L1 budget = 1000 bytes.
        LeveledCompactionStrategy strategy = new LeveledCompactionStrategy(100, 1000, 10);

        ManifestState underBudget = ManifestState.empty().apply(ManifestEdit.builder()
                .addedTables(List.of(meta(1, 1, 400), meta(2, 1, 400))).build());
        assertThat(strategy.pickCompaction(underBudget)).isEmpty();

        ManifestState overBudget = ManifestState.empty().apply(ManifestEdit.builder()
                .addedTables(List.of(meta(1, 1, 800), meta(2, 1, 800))).build());
        Optional<CompactionTask> task = strategy.pickCompaction(overBudget);
        assertThat(task).isPresent();
        assertThat(task.get().outputLevel()).isEqualTo(2);
    }

    @Test
    void bottommostCompactionDropsAgedTombstoneButKeepsYoungOne() {
        SSTableMeta a = write(0, List.of(new Mutation(CandyKey.of("a"), put(hlc(1, 0, 1), 1, 5)),
                new Mutation(CandyKey.of("k"), put(hlc(1, 0, 1), 1, 5))));
        SSTableMeta b = write(0, List.of(
                new Mutation(CandyKey.of("k"), CandyLocator.tombstone(hlc(5, 0, 1), 1000L)),
                new Mutation(CandyKey.of("z"), put(hlc(1, 0, 1), 1, 5))));
        CompactionTask task = new CompactionTask(List.of(a, b), 1, true, List.of());

        // now=2000, grace=100 => tombstone (created 1000) is aged out and dropped.
        Compactor aged = new Compactor(store, writer, cfg, 100, new ManualClock(2000));
        SSTableMeta out = aged.compact(task).outputs().get(0);
        assertThat(keysOf(out)).containsExactly("a", "z");

        // now=1050, grace=100 => tombstone too young, must be preserved.
        SSTableMeta a2 = write(0, List.of(new Mutation(CandyKey.of("a"), put(hlc(1, 0, 1), 1, 5)),
                new Mutation(CandyKey.of("k"), put(hlc(1, 0, 1), 1, 5))));
        SSTableMeta b2 = write(0, List.of(
                new Mutation(CandyKey.of("k"), CandyLocator.tombstone(hlc(5, 0, 1), 1000L)),
                new Mutation(CandyKey.of("z"), put(hlc(1, 0, 1), 1, 5))));
        Compactor young = new Compactor(store, writer, cfg, 100, new ManualClock(1050));
        SSTableMeta out2 = young.compact(new CompactionTask(List.of(a2, b2), 1, true, List.of()))
                .outputs().get(0);
        assertThat(keysOf(out2)).containsExactly("a", "k", "z");
    }

    @Test
    void nonBottommostCompactionAlwaysKeepsTombstone() {
        SSTableMeta a = write(0, List.of(new Mutation(CandyKey.of("k"), put(hlc(1, 0, 1), 1, 5))));
        SSTableMeta b = write(0, List.of(
                new Mutation(CandyKey.of("k"), CandyLocator.tombstone(hlc(5, 0, 1), 0L))));
        // bottommost=false even though tombstone is ancient.
        Compactor c = new Compactor(store, writer, cfg, 100, new ManualClock(1_000_000));
        SSTableMeta out = c.compact(new CompactionTask(List.of(a, b), 1, false, List.of())).outputs().get(0);
        assertThat(keysOf(out)).containsExactly("k");
    }

    @Test
    void nonBottommostCompactionCarriesRangeTombstoneAndDropsCoveredPoint() {
        SSTableMeta a = write(0, List.of(new Mutation(CandyKey.of("c"), put(hlc(1, 0, 1), 1, 5))));
        SSTableMeta b = write(0, List.of(),
                List.of(new RangeTombstone(CandyKey.of("b"), CandyKey.of("d"), hlc(5, 0, 1))));

        // Ancient range tombstone, but non-bottommost: nothing may be dropped.
        Compactor c = new Compactor(store, writer, cfg, 100, new ManualClock(1_000_000));
        SSTableMeta out = c.compact(new CompactionTask(List.of(a, b), 1, false, List.of())).outputs().get(0);

        assertThat(keysOf(out)).isEmpty();                     // the carried tombstone still shadows "c"
        assertThat(rangeTombstonesOf(out)).hasSize(1);         // range tombstone carried forward
    }

    @Test
    void bottommostCompactionDropsAgedRangeTombstoneAndItsCoveredPoints() {
        SSTableMeta a = write(0, List.of(new Mutation(CandyKey.of("c"), put(hlc(1, 0, 1), 1, 5)),
                new Mutation(CandyKey.of("z"), put(hlc(1, 0, 1), 1, 5))));
        // Range tombstone HLC physical=1000; now=1_000_000, grace=100 => aged.
        SSTableMeta b = write(0, List.of(),
                List.of(new RangeTombstone(CandyKey.of("b"), CandyKey.of("d"), hlc(1000, 0, 1))));

        Compactor aged = new Compactor(store, writer, cfg, 100, new ManualClock(1_000_000));
        SSTableMeta out = aged.compact(new CompactionTask(List.of(a, b), 1, true, List.of())).outputs().get(0);

        assertThat(keysOf(out)).containsExactly("z");          // "c" covered+aged => dropped
        assertThat(rangeTombstonesOf(out)).isEmpty();          // aged range tombstone dropped
    }

    @Test
    void bottommostCompactionKeepsYoungRangeTombstoneButDropsCoveredPoint() {
        SSTableMeta a = write(0, List.of(new Mutation(CandyKey.of("c"), put(hlc(1, 0, 1), 1, 5))));
        // Range tombstone HLC physical=1000; now=1050, grace=100 => too young to drop.
        SSTableMeta b = write(0, List.of(),
                List.of(new RangeTombstone(CandyKey.of("b"), CandyKey.of("d"), hlc(1000, 0, 1))));

        Compactor young = new Compactor(store, writer, cfg, 100, new ManualClock(1050));
        SSTableMeta out = young.compact(new CompactionTask(List.of(a, b), 1, true, List.of())).outputs().get(0);

        assertThat(keysOf(out)).isEmpty();                     // shadowed by the carried tombstone
        assertThat(rangeTombstonesOf(out)).hasSize(1);         // carried forward
    }

    @Test
    void compactionSplitsOutputIntoKeyDisjointTablesNearTheTargetSize() {
        List<Mutation> even = new ArrayList<>();
        List<Mutation> odd = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            Mutation m = new Mutation(CandyKey.of(String.format("k%04d", i)), put(hlc(1, 0, 1), 1, 5));
            (i % 2 == 0 ? even : odd).add(m);
        }
        SSTableMeta a = write(0, even);
        SSTableMeta b = write(0, odd,
                List.of(new RangeTombstone(CandyKey.of("x"), CandyKey.of("y"), hlc(5, 0, 1))));

        Compactor c = new Compactor(store, writer, cfg, 100, new ManualClock(0), 2048);
        CompactionResult result = c.compact(new CompactionTask(List.of(a, b), 1, false, List.of()));

        List<SSTableMeta> outputs = result.outputs();
        assertThat(outputs).hasSizeGreaterThan(2);
        assertThat(result.edit().addedTables()).isEqualTo(outputs);
        List<String> merged = new ArrayList<>();
        for (int i = 0; i < outputs.size(); i++) {
            SSTableMeta out = outputs.get(i);
            assertThat(out.level()).isEqualTo(1);
            assertThat(out.sizeBytes()).isLessThan(2048 + 256);
            if (i > 0) {
                assertThat(outputs.get(i - 1).maxKey()).isLessThan(out.minKey());
            }
            merged.addAll(keysOf(out));
            // [x, y) lies past every point key, so only the last table's slot holds it.
            assertThat(rangeTombstonesOf(out)).hasSize(i == outputs.size() - 1 ? 1 : 0);
        }
        assertThat(merged).hasSize(400).isSorted();
    }

    @Test
    void carriedRangeTombstoneIsSplitAcrossTheOutputsItSpans() {
        List<Mutation> points = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            points.add(new Mutation(CandyKey.of(String.format("k%04d", i)), put(hlc(9, 0, 1), 1, 5)));
        }
        SSTableMeta a = write(0, points);
        SSTableMeta b = write(0, List.of(),
                List.of(new RangeTombstone(null, CandyKey.of("k0150"), hlc(5, 0, 1))));

        Compactor c = new Compactor(store, writer, cfg, 100, new ManualClock(0), 1024);
        List<SSTableMeta> outputs = c.compact(new CompactionTask(List.of(a, b), 1, false, List.of()))
                .outputs();

        assertThat(outputs).hasSizeGreaterThan(2);
        for (int i = 0; i < outputs.size(); i++) {
            SSTableMeta out = outputs.get(i);
            List<RangeTombstone> fragments = rangeTombstonesOf(out);
            if (out.minKey().compareTo(CandyKey.of("k0150")) >= 0) {
                assertThat(fragments).isEmpty();
                continue;
            }
            assertThat(fragments).hasSize(1);
            RangeTombstone f = fragments.get(0);
            assertThat(f.startInclusive()).isEqualTo(i == 0 ? null : out.minKey());
            CandyKey slotEnd = outputs.get(i + 1).minKey();
            assertThat(f.endExclusive()).isEqualTo(
                    slotEnd.compareTo(CandyKey.of("k0150")) < 0 ? slotEnd : CandyKey.of("k0150"));
            assertThat(out.rangeTombstoneSpan()).isEqualTo(KeySpan.of(f));
        }
        for (int i = 1; i < outputs.size(); i++) {
            assertThat(outputs.get(i - 1).span().overlaps(outputs.get(i).span())).isFalse();
        }
    }

    @Test
    void leveledStrategyPullsInTablesCoveredByARangeTombstone() {
        LeveledCompactionStrategy strategy = new LeveledCompactionStrategy(100, 1000, 10);
        SSTableMeta withRange = new SSTableMeta(1, 1, CandyKey.of("a"), CandyKey.of("b"), 1, 2000,
                java.util.Set.of(), new KeySpan(CandyKey.of("a"), CandyKey.of("m"), false));
        SSTableMeta covered = meta(2, 2, "f", "g", 100);
        SSTableMeta atEnd = meta(3, 2, "m", "z", 100);
        ManifestState state = ManifestState.empty().apply(ManifestEdit.builder()
                .addedTables(List.of(withRange, covered, atEnd)).build());

        CompactionTask task = strategy.pickCompaction(state).orElseThrow();

        assertThat(task.inputs()).containsExactly(withRange, covered);
        assertThat(task.otherTables()).containsExactly(atEnd);
    }

    @Test
    void compactionAlsoSplitsWhenTheBloomFilterWouldOutgrowOneEntry() {
        SSTableWriter wideBloom = new SSTableWriter(store, 1000); // 125 B of bloom per key
        List<Mutation> sorted = new ArrayList<>();
        for (int i = 0; i < 20_000; i++) {
            sorted.add(new Mutation(CandyKey.of(String.format("k%06d", i)), put(hlc(1, 0, 1), 1, 5)));
        }
        SSTableMeta in = wideBloom.write(cfg, 0, sorted.iterator());

        Compactor c = new Compactor(store, wideBloom, cfg, 100, new ManualClock(0));
        List<SSTableMeta> outputs = c.compact(new CompactionTask(List.of(in), 1, false, List.of())).outputs();

        assertThat(outputs).hasSizeGreaterThan(1);
        assertThat(outputs).allSatisfy(out -> assertThat(out.entryCount()).isLessThan(9000));
        assertThat(outputs.stream().mapToLong(SSTableMeta::entryCount).sum()).isEqualTo(20_000);
    }

    @Test
    void compactionDeletesItsPartialOutputsWhenAWriteFails() {
        SSTableMeta in = write(0, List.of(new Mutation(CandyKey.of("a"), put(hlc(1, 0, 1), 1, 5)),
                new Mutation(CandyKey.of("b"), put(hlc(1, 0, 1), 1, 5))));
        List<Long> before = store.listLedgers();
        FailingStore failing = new FailingStore(store, 2, -1); // one key per output; the second fails
        Compactor c = new Compactor(failing, new SSTableWriter(failing, 10, 256), cfg, 100,
                new ManualClock(0), 1);
        assertThatThrownBy(() -> c.compact(new CompactionTask(List.of(in), 1, false, List.of())))
                .isInstanceOf(IllegalStateException.class);
        assertThat(store.listLedgers()).containsExactlyInAnyOrderElementsOf(before);
    }

    @Test
    void compactionDeletesTheTableItFailsPartwayThrough() {
        SSTableMeta in = write(0, List.of(new Mutation(CandyKey.of("a"), put(hlc(1, 0, 1), 1, 5)),
                new Mutation(CandyKey.of("b"), put(hlc(1, 0, 1), 1, 5))));
        List<Long> before = store.listLedgers();
        FailingStore failing = new FailingStore(store, -1, 2); // the bloom append of the only output
        Compactor c = new Compactor(failing, new SSTableWriter(failing, 10, 1 << 16), cfg, 100,
                new ManualClock(0));
        assertThatThrownBy(() -> c.compact(new CompactionTask(List.of(in), 1, false, List.of())))
                .isInstanceOf(IllegalStateException.class);
        assertThat(store.listLedgers()).containsExactlyInAnyOrderElementsOf(before);
    }

    @Test
    void leveledStrategyPicksOneTableAndOnlyTheOverlappingNextLevelTables() {
        LeveledCompactionStrategy strategy = new LeveledCompactionStrategy(100, 1000, 10);
        SSTableMeta l1a = meta(1, 1, "a", "c", 600);
        SSTableMeta l1b = meta(2, 1, "d", "f", 600);
        SSTableMeta l1c = meta(3, 1, "g", "i", 600);
        SSTableMeta l2a = meta(4, 2, "a", "b", 5000);
        SSTableMeta l2b = meta(5, 2, "e", "e", 100);
        SSTableMeta l2c = meta(6, 2, "h", "z", 5000);
        ManifestState state = ManifestState.empty().apply(ManifestEdit.builder()
                .addedTables(List.of(l1a, l1b, l1c, l2a, l2b, l2c)).build());

        CompactionTask task = strategy.pickCompaction(state).orElseThrow();

        // l1b has the least next-level overlap per byte of its own.
        assertThat(task.inputs()).containsExactly(l1b, l2b);
        assertThat(task.outputLevel()).isEqualTo(2);
        assertThat(task.bottommost()).isTrue();
        assertThat(task.otherTables()).containsExactlyInAnyOrder(l1a, l1c, l2a, l2c);
    }

    @Test
    void agedRangeTombstoneIsKeptWhileATableOutsideTheTaskOverlapsIt() {
        // Two key-disjoint L1 tables. The range tombstone in the first also covers an older point in
        // the second; dropping it while compacting only the first would resurrect "m".
        SSTableMeta withRange = write(1, List.of(new Mutation(CandyKey.of("b"), put(hlc(10, 0, 1), 1, 5))),
                List.of(new RangeTombstone(CandyKey.of("a"), CandyKey.of("z"), hlc(5, 0, 1))));
        SSTableMeta older = write(1, List.of(new Mutation(CandyKey.of("m"), put(hlc(1, 0, 1), 1, 5))));
        ManifestState state = ManifestState.empty().apply(ManifestEdit.builder()
                .addedTables(List.of(withRange, older)).build());

        CompactionTask task = new LeveledCompactionStrategy(100, 1, 10).pickCompaction(state)
                .orElseThrow();
        assertThat(task.inputs()).containsExactly(withRange);
        assertThat(task.bottommost()).isTrue();

        Compactor aged = new Compactor(store, writer, cfg, 100, new ManualClock(1_000_000));
        SSTableMeta out = aged.compact(task).outputs().get(0);
        assertThat(keysOf(out)).containsExactly("b");
        assertThat(rangeTombstonesOf(out)).hasSize(1);

        // Once nothing outside the task overlaps it, the aged range tombstone is dropped.
        SSTableMeta out2 = aged.compact(new CompactionTask(List.of(out, older), 2, true, List.of())).outputs().get(0);
        assertThat(keysOf(out2)).containsExactly("b");
        assertThat(rangeTombstonesOf(out2)).isEmpty();
    }

    /** Delegates to a store but fails its {@code createAt}-th ledger creation or {@code appendAt}-th append. */
    private static final class FailingStore implements LedgerStore {
        private final LedgerStore delegate;
        private final int createAt;
        private final int appendAt;
        private int created;
        private int appended;

        FailingStore(LedgerStore delegate, int createAt, int appendAt) {
            this.delegate = delegate;
            this.createAt = createAt;
            this.appendAt = appendAt;
        }

        @Override
        public WritableLedger createLedger(LedgerConfig config) {
            if (++created == createAt) {
                throw new IllegalStateException("injected createLedger failure");
            }
            WritableLedger ledger = delegate.createLedger(config);
            return new WritableLedger() {
                @Override
                public long append(byte[] data) {
                    if (++appended == appendAt) {
                        throw new IllegalStateException("injected append failure");
                    }
                    return ledger.append(data);
                }

                @Override
                public LedgerEntry read(long entryId) {
                    return ledger.read(entryId);
                }

                @Override
                public List<LedgerEntry> readRange(long firstEntryId, long lastEntryId) {
                    return ledger.readRange(firstEntryId, lastEntryId);
                }

                @Override
                public long ledgerId() {
                    return ledger.ledgerId();
                }

                @Override
                public long lastAddConfirmed() {
                    return ledger.lastAddConfirmed();
                }

                @Override
                public boolean isSealed() {
                    return ledger.isSealed();
                }

                @Override
                public java.util.Map<String, byte[]> customMetadata() {
                    return ledger.customMetadata();
                }

                @Override
                public void close() {
                    ledger.close();
                }
            };
        }

        @Override
        public ReadableLedger openLedger(long ledgerId) {
            return delegate.openLedger(ledgerId);
        }

        @Override
        public ReadableLedger recoverOpen(long ledgerId) {
            return delegate.recoverOpen(ledgerId);
        }

        @Override
        public void deleteLedger(long ledgerId) {
            delegate.deleteLedger(ledgerId);
        }

        @Override
        public List<Long> listLedgers() {
            return delegate.listLedgers();
        }

        @Override
        public void close() {
        }
    }

    private static SSTableMeta meta(long id, int level, String min, String max, long sizeBytes) {
        return new SSTableMeta(id, level, CandyKey.of(min), CandyKey.of(max), 1, sizeBytes,
                java.util.Set.of());
    }

    private static SSTableMeta meta(long id, int level) {
        return meta(id, level, 1024);
    }

    private static SSTableMeta meta(long id, int level, long sizeBytes) {
        return new SSTableMeta(id, level, CandyKey.of("a"), CandyKey.of("z"), 1, sizeBytes,
                java.util.Set.of());
    }
}
