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
package me.predatorray.candybox.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.lsm.compaction.Compactor;
import me.predatorray.candybox.lsm.compaction.CompactionTask;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.lsm.manifest.ManifestState;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;
import me.predatorray.candybox.lsm.sstable.SSTableWriter;
import me.predatorray.candybox.server.CandyboxNode;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Exercises the full LSM level hierarchy on real backends (embedded BookKeeper + in-process
 * ZooKeeper): an entry is pushed memtable → L0 → L1 → L2 by successive compactions and must stay
 * readable from each level, and a key whose value lives on a deep (non-memtable) level is deleted —
 * the tombstone must shadow it across the merged read path and a bottommost compaction must reconcile
 * both the value and the tombstone away. GC then reclaims the obsoleted ledgers while the surviving
 * data stays intact.
 *
 * <p>The node's {@code CompactionService} only ever scores L0 by count and the higher levels against a
 * 10&nbsp;MiB budget, so a tiny dataset never naturally cascades past L1. To drive the deep-level
 * behaviour deterministically this test runs the same {@link Compactor} the node uses but with
 * explicit {@link CompactionTask}s, committing each through the live owner's
 * {@link BoxEngine#applyCompaction} (fencing-gated) exactly as the service would.
 */
class MultiLevelCompactionIT {

    private static EmbeddedBookKeeper bookKeeper;
    private static TestingServer zookeeper;

    @BeforeAll
    static void start() throws Exception {
        bookKeeper = new EmbeddedBookKeeper(3);
        zookeeper = new TestingServer(true);
    }

    @AfterAll
    static void stop() throws Exception {
        if (zookeeper != null) {
            zookeeper.close();
        }
        if (bookKeeper != null) {
            bookKeeper.close();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void entryCascadesToDeepLevelsThenDeleteOnADeepLevelIsReconciled() {
        CandyboxConfig config = CandyboxConfig.builder()
                .memtableFlushThresholdBytes(1) // every write flushes => its own L0 SSTable + WAL rotation
                .syrupRolloverBytes(1)          // every Candy in its own Syrup
                .l0CompactionTrigger(2)
                .l0StallThreshold(1000)
                .ledgerGcGraceMillis(0)         // reclaim obsolete ledgers immediately
                .tombstoneGcGraceMillis(0)      // a flushed tombstone is droppable at the bottommost level
                .leaseRenewIntervalMillis(0)
                .compactionIntervalMillis(0)    // drive compaction/GC manually
                .build();
        BoxName box = BoxName.of("levels-box");

        BookKeeperLedgerStore store = new BookKeeperLedgerStore(bookKeeper.clientConfiguration(),
                bytes("candybox"));
        ZooKeeperCoordinationService coordination =
                new ZooKeeperCoordinationService(zookeeper.getConnectString(), SystemClock.INSTANCE);
        CandyboxNode node = new CandyboxNode(1, config, store, coordination, SystemClock.INSTANCE);

        // The compactor the node would use; we feed it explicit tasks to force deep-level transitions.
        SSTableWriter writer = new SSTableWriter(store, config.bloomBitsPerKey());
        LedgerConfig sstableConfig = LedgerConfig.forRole(LedgerRole.SSTABLE);
        Compactor compactor = new Compactor(store, writer, sstableConfig,
                config.tombstoneGcGraceMillis(), SystemClock.INSTANCE);
        try {
            node.createBox(box, 1);
            BoxEngine engine = node.enginePartition(box, 0);

            for (int i = 0; i < 4; i++) {
                engine.putCandy(CandyKey.of("k" + i), bytes("v" + i), null, Map.of(), null);
            }
            assertThat(engine.manifestState().level0()).hasSize(4); // four L0 tables, one per put

            // memtable -> L0 (done) -> L1 -> L2: the data ends up two levels below L0.
            compactAllOfLevel(engine, compactor, 0, 1);
            assertThat(engine.manifestState().level(1)).isNotEmpty();
            assertReadable(engine, 4);

            compactAllOfLevel(engine, compactor, 1, 2);
            assertThat(engine.manifestState().maxLevel()).isEqualTo(2);
            assertThat(engine.manifestState().level0()).isEmpty();
            assertThat(engine.manifestState().level(1)).isEmpty();
            // All four keys are now served from the deep L2 level.
            assertReadable(engine, 4);

            // ---- delete a key whose only copy lives on the deep level ----
            engine.deleteCandy(CandyKey.of("k1"));
            assertThatThrownBy(() -> engine.getCandy(CandyKey.of("k1")))
                    .isInstanceOf(CandyNotFoundException.class);
            engine.flush(); // the tombstone is now an L0 SSTable; the value is still down at L2
            assertThat(engine.manifestState().level0()).hasSize(1);
            assertThatThrownBy(() -> engine.getCandy(CandyKey.of("k1")))
                    .isInstanceOf(CandyNotFoundException.class); // L0 tombstone shadows the L2 value

            // Bottommost compaction merging the L0 tombstone with the L2 value: with the tombstone
            // aged past the grace, both the value and the tombstone are reconciled away.
            ManifestState state = engine.manifestState();
            List<SSTableMeta> inputs = new ArrayList<>(state.level0());
            inputs.addAll(state.level(2));
            engine.applyCompaction(compactor.compact(new CompactionTask(inputs, 2, true)).edit());

            assertThatThrownBy(() -> engine.getCandy(CandyKey.of("k1")))
                    .isInstanceOf(CandyNotFoundException.class);
            assertThat(engine.listCandies(null, null, 100).entries())
                    .extracting(e -> e.key().value()).containsExactly("k0", "k2", "k3");

            // GC physically reclaims the obsoleted SSTables / WALs / orphaned Syrups.
            int deleted = node.collectGarbageOnce();
            assertThat(deleted).isGreaterThan(0);

            // Surviving data is intact and still readable after compaction + GC.
            assertThat(engine.getCandy(CandyKey.of("k0"))).isEqualTo(bytes("v0"));
            assertThat(engine.getCandy(CandyKey.of("k2"))).isEqualTo(bytes("v2"));
            assertThat(engine.getCandy(CandyKey.of("k3"))).isEqualTo(bytes("v3"));
            assertThatThrownBy(() -> engine.getCandy(CandyKey.of("k1")))
                    .isInstanceOf(CandyNotFoundException.class);
        } finally {
            node.close();
            coordination.close();
            store.close();
        }
    }

    /** Merges every table at {@code fromLevel} (plus overlapping output-level tables) into the next level. */
    private static void compactAllOfLevel(BoxEngine engine, Compactor compactor, int fromLevel,
                                          int outputLevel) {
        ManifestState state = engine.manifestState();
        List<SSTableMeta> seed = state.level(fromLevel);
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
        boolean bottommost = state.tables().stream().noneMatch(t -> t.level() > outputLevel);
        engine.applyCompaction(compactor.compact(new CompactionTask(inputs, outputLevel, bottommost)).edit());
    }

    private static void assertReadable(BoxEngine engine, int count) {
        for (int i = 0; i < count; i++) {
            assertThat(engine.getCandy(CandyKey.of("k" + i))).isEqualTo(bytes("v" + i));
        }
    }
}
