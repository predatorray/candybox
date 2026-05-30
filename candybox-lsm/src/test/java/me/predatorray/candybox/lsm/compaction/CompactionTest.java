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
import java.util.List;
import java.util.Optional;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.RangeTombstone;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.lsm.manifest.ManifestEdit;
import me.predatorray.candybox.lsm.manifest.ManifestState;
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
        CompactionTask task = new CompactionTask(List.of(a, b), 1, true);

        // now=2000, grace=100 => tombstone (created 1000) is aged out and dropped.
        Compactor aged = new Compactor(store, writer, cfg, 100, new ManualClock(2000));
        SSTableMeta out = aged.compact(task).output().orElseThrow();
        assertThat(keysOf(out)).containsExactly("a", "z");

        // now=1050, grace=100 => tombstone too young, must be preserved.
        SSTableMeta a2 = write(0, List.of(new Mutation(CandyKey.of("a"), put(hlc(1, 0, 1), 1, 5)),
                new Mutation(CandyKey.of("k"), put(hlc(1, 0, 1), 1, 5))));
        SSTableMeta b2 = write(0, List.of(
                new Mutation(CandyKey.of("k"), CandyLocator.tombstone(hlc(5, 0, 1), 1000L)),
                new Mutation(CandyKey.of("z"), put(hlc(1, 0, 1), 1, 5))));
        Compactor young = new Compactor(store, writer, cfg, 100, new ManualClock(1050));
        SSTableMeta out2 = young.compact(new CompactionTask(List.of(a2, b2), 1, true))
                .output().orElseThrow();
        assertThat(keysOf(out2)).containsExactly("a", "k", "z");
    }

    @Test
    void nonBottommostCompactionAlwaysKeepsTombstone() {
        SSTableMeta a = write(0, List.of(new Mutation(CandyKey.of("k"), put(hlc(1, 0, 1), 1, 5))));
        SSTableMeta b = write(0, List.of(
                new Mutation(CandyKey.of("k"), CandyLocator.tombstone(hlc(5, 0, 1), 0L))));
        // bottommost=false even though tombstone is ancient.
        Compactor c = new Compactor(store, writer, cfg, 100, new ManualClock(1_000_000));
        SSTableMeta out = c.compact(new CompactionTask(List.of(a, b), 1, false)).output().orElseThrow();
        assertThat(keysOf(out)).containsExactly("k");
    }

    @Test
    void nonBottommostCompactionCarriesRangeTombstoneAndKeepsCoveredPoint() {
        SSTableMeta a = write(0, List.of(new Mutation(CandyKey.of("c"), put(hlc(1, 0, 1), 1, 5))));
        SSTableMeta b = write(0, List.of(),
                List.of(new RangeTombstone(CandyKey.of("b"), CandyKey.of("d"), hlc(5, 0, 1))));

        // Ancient range tombstone, but non-bottommost: nothing may be dropped.
        Compactor c = new Compactor(store, writer, cfg, 100, new ManualClock(1_000_000));
        SSTableMeta out = c.compact(new CompactionTask(List.of(a, b), 1, false)).output().orElseThrow();

        assertThat(keysOf(out)).containsExactly("c");          // covered point preserved
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
        SSTableMeta out = aged.compact(new CompactionTask(List.of(a, b), 1, true)).output().orElseThrow();

        assertThat(keysOf(out)).containsExactly("z");          // "c" covered+aged => dropped
        assertThat(rangeTombstonesOf(out)).isEmpty();          // aged range tombstone dropped
    }

    @Test
    void bottommostCompactionKeepsYoungRangeTombstoneAndCoveredPoint() {
        SSTableMeta a = write(0, List.of(new Mutation(CandyKey.of("c"), put(hlc(1, 0, 1), 1, 5))));
        // Range tombstone HLC physical=1000; now=1050, grace=100 => too young to drop.
        SSTableMeta b = write(0, List.of(),
                List.of(new RangeTombstone(CandyKey.of("b"), CandyKey.of("d"), hlc(1000, 0, 1))));

        Compactor young = new Compactor(store, writer, cfg, 100, new ManualClock(1050));
        SSTableMeta out = young.compact(new CompactionTask(List.of(a, b), 1, true)).output().orElseThrow();

        assertThat(keysOf(out)).containsExactly("c");          // covered point preserved (young)
        assertThat(rangeTombstonesOf(out)).hasSize(1);         // carried forward
    }

    private static SSTableMeta meta(long id, int level) {
        return meta(id, level, 1024);
    }

    private static SSTableMeta meta(long id, int level, long sizeBytes) {
        return new SSTableMeta(id, level, CandyKey.of("a"), CandyKey.of("z"), 1, sizeBytes,
                java.util.Set.of());
    }
}
