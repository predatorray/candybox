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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.Partitioning;
import me.predatorray.candybox.common.auth.ObjectAcl;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.lsm.manifest.RenameIntent;
import org.junit.jupiter.api.Test;

/**
 * Server-level coverage of the cross-partition zero-copy machinery on a single node owning several
 * partitions of a Box: the Box-global garbage collector keeps a Syrup shared across partitions alive,
 * and the rename-intent maintenance sweep finalizes (via the rendezvous marker) or abandons (past the
 * window) the source-side delete.
 */
class CrossPartitionGcAndRenameTest {

    private static final BoxName BOX = BoxName.of("xp-box");
    private static final int PARTITIONS = 4;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String keyIn(int partition, String tag) {
        for (int i = 0; i < 10_000; i++) {
            String candidate = tag + "-" + i;
            if (Partitioning.partitionOf(candidate, PARTITIONS) == partition) {
                return candidate;
            }
        }
        throw new AssertionError("no key for partition " + partition);
    }

    private static CandyboxConfig aggressiveGcConfig() {
        return CandyboxConfig.builder()
                .memtableFlushThresholdBytes(1)   // every write flushes => its own L0 table + WAL
                .syrupRolloverBytes(1)             // every Candy in its own Syrup ledger
                .l0CompactionTrigger(2)
                .ledgerGcGraceMillis(0)            // reclaim immediately
                .leaseRenewIntervalMillis(0)
                .compactionIntervalMillis(0)       // manual control
                .build();
    }

    @Test
    void boxGlobalGcKeepsASyrupSharedAcrossPartitionsAlive() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        try (CandyboxNode node = new CandyboxNode(1, aggressiveGcConfig(), store,
                new InMemoryCoordinationService(), new ManualClock(1000))) {
            node.createBox(BOX, PARTITIONS);
            String src = keyIn(0, "src");
            String dst = keyIn(1, "dst");
            BoxEngine p0 = node.enginePartition(BOX, 0);
            BoxEngine p1 = node.enginePartition(BOX, 1);

            p0.putCandy(CandyKey.of(src), bytes("payload"), "text/plain", Map.of(), null);
            p0.flush();
            CandyLocator locator = p0.resolveLocator(CandyKey.of(src));
            long sharedSyrup = locator.parts().get(0).segments().get(0).syrupId();

            // Partition 1 zero-copy-references partition 0's Syrup (a cross-partition copy).
            p1.zeroCopyPut(CandyKey.of(dst), locator.parts(), "text/plain", Map.of(), 0L,
                    ObjectAcl.NONE, null);
            assertThat(p1.referencedSyrups()).contains(sharedSyrup);

            // Delete the source and compact partition 0 so its Syrup becomes a local orphan.
            node.collectGarbageOnce(); // publishes every partition's refs (incl. p1 -> sharedSyrup)
            p0.deleteCandy(CandyKey.of(src));
            p0.flush();
            for (int i = 0; i < 5; i++) {
                node.compactOwnedBoxesOnce();
            }

            // Box-global GC must NOT reclaim the shared Syrup — partition 1 still points at it.
            node.collectGarbageOnce();
            assertThat(p1.getCandy(CandyKey.of(dst))).isEqualTo(bytes("payload"));
        }
        store.close();
    }

    @Test
    void renameIntentIsFinalizedWhenTheRendezvousMarkerIsPresent() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        ManualClock clock = new ManualClock(1000);
        try (CandyboxNode node = new CandyboxNode(1, CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0).build(), store,
                new InMemoryCoordinationService(clock), clock)) {
            node.createBox(BOX, PARTITIONS);
            String src = keyIn(0, "src");
            String dst = keyIn(1, "dst");
            BoxEngine p0 = node.enginePartition(BOX, 0);
            BoxEngine p1 = node.enginePartition(BOX, 1);
            String token = "rename-token-1";

            // Simulate a cross-partition rename that crashed after the destination put (W2: both live).
            p0.putCandy(CandyKey.of(src), bytes("payload"), null, Map.of(), null);
            CandyLocator locator = p0.resolveLocator(CandyKey.of(src));
            p0.recordRenameIntent(new RenameIntent(token, src, locator.hlc(), dst, 1,
                    node.currentTimeMillis()));
            p1.zeroCopyPut(CandyKey.of(dst), locator.parts(), null, Map.of(), 0L, ObjectAcl.NONE,
                    token);
            node.writeRenameMarker(BOX.value(), token, src, 0, locator.hlc(), dst);

            // Both keys are live before finalization.
            assertThat(p0.getCandy(CandyKey.of(src))).isEqualTo(bytes("payload"));
            assertThat(p1.getCandy(CandyKey.of(dst))).isEqualTo(bytes("payload"));

            int finalized = node.finalizeRenameIntentsOnce();

            assertThat(finalized).isEqualTo(1);
            assertThatThrownBy(() -> p0.getCandy(CandyKey.of(src)))
                    .isInstanceOf(CandyNotFoundException.class);     // source tombstoned
            assertThat(p1.getCandy(CandyKey.of(dst))).isEqualTo(bytes("payload")); // destination kept
            assertThat(p0.listRenameIntents()).isEmpty();             // intent cleared
            assertThat(node.renameMarkerPresent(BOX.value(), token)).isFalse(); // marker deleted
        }
        store.close();
    }

    @Test
    void renameIntentIsAbandonedWhenNoMarkerAppearsWithinTheWindow() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        ManualClock clock = new ManualClock(1_000_000);
        try (CandyboxNode node = new CandyboxNode(1, CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0).renameIntentAbandonMillis(60_000).build(), store,
                new InMemoryCoordinationService(clock), clock)) {
            node.createBox(BOX, PARTITIONS);
            String src = keyIn(0, "src");
            BoxEngine p0 = node.enginePartition(BOX, 0);
            String token = "stuck-token";

            p0.putCandy(CandyKey.of(src), bytes("payload"), null, Map.of(), null);
            CandyLocator locator = p0.resolveLocator(CandyKey.of(src));
            // An intent recorded well outside the abandon window, with no destination marker ever set.
            p0.recordRenameIntent(new RenameIntent(token, src, locator.hlc(), "dst", 1,
                    node.currentTimeMillis() - 120_000));

            int finalized = node.finalizeRenameIntentsOnce();

            assertThat(finalized).isEqualTo(0);                       // nothing was completed
            assertThat(p0.listRenameIntents()).isEmpty();             // the stuck intent was dropped
            assertThat(p0.getCandy(CandyKey.of(src))).isEqualTo(bytes("payload")); // source stays live
        }
        store.close();
    }
}
