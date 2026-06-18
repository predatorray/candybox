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
package me.predatorray.candybox.lsm.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.lsm.compaction.Compactor;
import me.predatorray.candybox.lsm.compaction.CompactionResult;
import me.predatorray.candybox.lsm.compaction.CompactionTask;
import me.predatorray.candybox.lsm.manifest.ManifestState;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;
import me.predatorray.candybox.lsm.sstable.SSTableReader;
import me.predatorray.candybox.lsm.sstable.SSTableWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Multi-level compaction coverage for {@link BoxEngine}: drives the engine's
 * {@link BoxEngine#applyCompaction} commit path with a {@link Compactor} so an entry travels
 * memtable → L0 → L1 → L2 and stays readable, last-write-wins is honoured when versions land on
 * different (deep) levels, and a DELETE issued against a key whose value lives on a non-memtable
 * level both shadows it through the merged read path and is reconciled away by a bottommost
 * compaction.
 *
 * <p>{@link BoxEngineTest} only exercises the memtable → L0 (flush) hop; the leveled {@code Compactor}
 * is otherwise tested in isolation at the SSTable level. These tests close the loop end-to-end through
 * the engine's read path across several levels.
 */
class BoxEngineCompactionTest {

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private final BoxName box = BoxName.of("compaction-box");
    private final LedgerConfig sstableConfig = LedgerConfig.forRole(LedgerRole.SSTABLE);
    private BoxEngine engine;

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.close();
        }
        store.close();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private BoxEngine newEngine() {
        // High flush/stall thresholds so the test, not the engine, decides when to flush/compact.
        CandyboxConfig cfg = CandyboxConfig.builder()
                .memtableFlushThresholdBytes(1L << 30)
                .l0StallThreshold(1000)
                .build();
        return BoxEngine.createNew(box, cfg, store, 1, new ManualClock(1_000), 1L);
    }

    /**
     * Merges the given input tables into {@code outputLevel} and commits the edit on the engine,
     * exactly as {@code CompactionService} would, with a compactor clock of {@code nowMillis} and the
     * given tombstone GC grace. Mirrors the leveled strategy's input selection by pulling the seed
     * level plus any overlapping output-level tables.
     */
    private void compact(int fromLevel, int outputLevel, boolean bottommost, long graceMillis,
                         long nowMillis) {
        ManifestState state = engine.manifestState();
        List<SSTableMeta> seed = state.level(fromLevel);
        assertThat(seed).as("level %d must have tables to compact", fromLevel).isNotEmpty();
        CandyKey min = null;
        CandyKey max = null;
        for (SSTableMeta t : seed) {
            if (min == null || t.minKey().compareTo(min) < 0) {
                min = t.minKey();
            }
            if (max == null || t.maxKey().compareTo(max) > 0) {
                max = t.maxKey();
            }
        }
        List<SSTableMeta> inputs = new ArrayList<>(seed);
        for (SSTableMeta t : state.level(outputLevel)) {
            if (t.overlaps(min, max)) {
                inputs.add(t);
            }
        }
        Clock compactorClock = new ManualClock(nowMillis);
        Compactor compactor = new Compactor(store, new SSTableWriter(store, 10, 256), sstableConfig,
                graceMillis, compactorClock);
        CompactionResult result = compactor.compact(new CompactionTask(inputs, outputLevel, bottommost));
        engine.applyCompaction(result.edit());
    }

    /** Keys contained in a specific level's tables (tombstones included), via direct SSTable scans. */
    private List<String> keysAtLevel(int level) {
        List<String> keys = new ArrayList<>();
        for (SSTableMeta meta : engine.manifestState().level(level)) {
            try (SSTableReader r = new SSTableReader(store, meta.ledgerId())) {
                var it = r.scan(null);
                while (it.hasNext()) {
                    Mutation m = it.next();
                    keys.add(m.key().value() + (m.isTombstone() ? "(del)" : ""));
                }
            }
        }
        return keys;
    }

    @Test
    void entryTravelsFromMemtableToL0ToL1ToL2AndStaysReadable() {
        engine = newEngine();
        engine.putCandy(CandyKey.of("k"), bytes("v"), "text/plain", Map.of("m", "1"), null);

        // Memtable read: no SSTables yet.
        assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v"));
        assertThat(engine.manifestState().tables()).isEmpty();

        // memtable -> L0.
        engine.flush();
        assertThat(engine.manifestState().level0()).hasSize(1);
        assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v"));

        // L0 -> L1.
        compact(0, 1, false, 100, 2_000);
        assertThat(engine.manifestState().level0()).isEmpty();
        assertThat(engine.manifestState().level(1)).hasSize(1);
        assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v"));
        assertThat(engine.headCandy(CandyKey.of("k")).userMetadata()).containsEntry("m", "1");
        assertThat(engine.listCandies(null, null, 100).entries())
                .extracting(e -> e.key().value()).containsExactly("k");

        // L1 -> L2 (bottommost, but a live PUT so it is preserved).
        compact(1, 2, true, 100, 3_000);
        assertThat(engine.manifestState().level(1)).isEmpty();
        assertThat(engine.manifestState().level(2)).hasSize(1);
        assertThat(engine.manifestState().maxLevel()).isEqualTo(2);

        // Still served from the deep level by get / head / list / range.
        assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v"));
        assertThat(engine.headCandy(CandyKey.of("k")).contentLength()).isEqualTo(1);
        assertThat(engine.listCandies(null, null, 100).entries())
                .extracting(e -> e.key().value()).containsExactly("k");
        java.io.ByteArrayOutputStream slice = new java.io.ByteArrayOutputStream();
        engine.getCandyRange(CandyKey.of("k"), 0, 0, slice);
        assertThat(slice.toByteArray()).isEqualTo(bytes("v"));
    }

    @Test
    void lastWriteWinsWhenAnOlderVersionSitsOnADeepLevel() {
        engine = newEngine();
        engine.putCandy(CandyKey.of("k"), bytes("v1"), null, Map.of(), null);
        engine.flush();
        compact(0, 1, false, 100, 2_000);
        compact(1, 2, true, 100, 3_000); // v1 now lives at L2

        // A newer version in the memtable must out-rank the deep old copy.
        engine.putCandy(CandyKey.of("k"), bytes("v2"), null, Map.of(), null);
        assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v2"));

        // ... and after that newer version is flushed to L0 while the old copy is still at L2.
        engine.flush();
        assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v2"));

        // Reconcile L0(v2) with L2(v1): push L0 down to L1, then merge L1 into L2.
        compact(0, 1, false, 100, 4_000);
        compact(1, 2, true, 100, 5_000);
        assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v2"));
        // Exactly one surviving version of the key at the bottom level.
        assertThat(keysAtLevel(2)).containsExactly("k");
    }

    @Test
    void deleteOfEntryLivingOnADeepLevelShadowsAcrossLevelsAndIsReconciledAway() {
        engine = newEngine();
        engine.putCandy(CandyKey.of("doomed"), bytes("v"), null, Map.of(), null);
        engine.putCandy(CandyKey.of("keep"), bytes("v"), null, Map.of(), null);
        engine.flush();
        compact(0, 1, false, 100, 2_000); // both keys now at L1 (a non-memtable level)
        assertThat(engine.manifestState().level(1)).hasSize(1);

        // Delete a key whose only copy lives on L1 — the tombstone starts in the memtable.
        engine.deleteCandy(CandyKey.of("doomed"));
        assertThatThrownBy(() -> engine.getCandy(CandyKey.of("doomed")))
                .isInstanceOf(CandyNotFoundException.class);
        assertThat(engine.getCandy(CandyKey.of("keep"))).isEqualTo(bytes("v"));

        // Flush the tombstone: now BOTH the entry (L1) and its tombstone (L0) live on SSTables.
        engine.flush();
        assertThat(engine.manifestState().level0()).hasSize(1);
        assertThatThrownBy(() -> engine.getCandy(CandyKey.of("doomed")))
                .isInstanceOf(CandyNotFoundException.class);
        assertThat(engine.listCandies(null, null, 100).entries())
                .extracting(e -> e.key().value()).containsExactly("keep");

        // Bottommost compaction with the tombstone aged out: the entry AND its tombstone vanish.
        // Merge L0(tombstone) with the overlapping L1(entry) into L1, aged (now - createdAt >= grace).
        compact(0, 1, true, 100, 1_000_000);
        assertThatThrownBy(() -> engine.getCandy(CandyKey.of("doomed")))
                .isInstanceOf(CandyNotFoundException.class);
        assertThat(engine.getCandy(CandyKey.of("keep"))).isEqualTo(bytes("v"));
        // Physically gone from the merged output: neither the value nor a lingering tombstone remains.
        assertThat(keysAtLevel(1)).containsExactly("keep");
    }

    @Test
    void deleteOfEntryOnADeepLevelKeepsAYoungTombstoneThroughBottommostCompaction() {
        engine = newEngine();
        engine.putCandy(CandyKey.of("doomed"), bytes("v"), null, Map.of(), null);
        engine.putCandy(CandyKey.of("keep"), bytes("v"), null, Map.of(), null);
        engine.flush();
        compact(0, 1, false, 100, 2_000); // entries at L1

        engine.deleteCandy(CandyKey.of("doomed")); // tombstone created at engine clock = 1000
        engine.flush();                            // tombstone -> L0

        // Bottommost compaction, but the tombstone is younger than the grace window (now=1050,
        // created=1000, grace=100): it must be carried forward so it keeps shadowing the entry.
        compact(0, 1, true, 100, 1_050);
        assertThatThrownBy(() -> engine.getCandy(CandyKey.of("doomed")))
                .isInstanceOf(CandyNotFoundException.class);
        assertThat(engine.getCandy(CandyKey.of("keep"))).isEqualTo(bytes("v"));
        // The tombstone survives in the merged output (the value was collapsed into it).
        assertThat(keysAtLevel(1)).containsExactlyInAnyOrder("keep", "doomed(del)");
    }
}
