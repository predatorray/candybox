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
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Box-global Syrup GC when a partition re-references a Syrup it had already marked as a pending orphan
 * (a cross-partition zero-copy rename/copy back into it): the Syrup must survive GC (issue #46).
 */
class BoxGlobalGcTest {

    private static final BoxName BOX = BoxName.of("gc-box");
    private static final int PARTITIONS = 2;
    private static final long GRACE = 60_000;

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private final ManualClock clock = new ManualClock(1_000_000);
    private CandyboxNode node;
    private BoxEngine p0;
    private BoxEngine p1;

    @BeforeEach
    void setUp() {
        CandyboxConfig config = CandyboxConfig.builder()
                .syrupRolloverBytes(1)             // every Candy in its own (sealed) Syrup
                .l0CompactionTrigger(2)
                .ledgerGcGraceMillis(GRACE)
                .ownershipLeaseTtlMillis(3_600_000)
                .leaseRenewIntervalMillis(0)
                .compactionIntervalMillis(0)
                .build();
        node = new CandyboxNode(1, config, store, new InMemoryCoordinationService(clock), clock);
        node.createBox(BOX, PARTITIONS);
        p0 = node.enginePartition(BOX, 0);
        p1 = node.enginePartition(BOX, 1);
    }

    @AfterEach
    void tearDown() {
        node.close();
        store.close();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static CandyKey keyIn(int partition, String tag) {
        for (int i = 0; ; i++) {
            if (Partitioning.partitionOf(tag + i, PARTITIONS) == partition) {
                return CandyKey.of(tag + i);
            }
        }
    }

    /** The client-driven cross-partition rename: zero-copy put at {@code dst}, then LWW delete of src. */
    private static void rename(BoxEngine from, CandyKey src, BoxEngine to, CandyKey dst) {
        CandyLocator locator = from.resolveLocator(src);
        to.zeroCopyPut(dst, locator.parts(), null, Map.of(), 0L, ObjectAcl.NONE, null);
        assertThat(from.deleteCandyConditional(src, locator.hlc())).isTrue();
    }

    private static long syrupOf(BoxEngine engine, CandyKey key) {
        return engine.resolveLocator(key).parts().get(0).segments().get(0).syrupId();
    }

    /** Puts {@code a} in p0 (sealed in its own Syrup), moves it to p1 and orphans the Syrup in p0. */
    private long orphanInP0(CandyKey a, CandyKey b) {
        p0.putCandy(a, bytes("precious"), null, Map.of(), null);
        p0.putCandy(keyIn(0, "other"), bytes("x"), null, Map.of(), null); // rolls a's Syrup over
        long syrup = syrupOf(p0, a);
        p0.flush();
        rename(p0, a, p1, b);
        p0.flush();
        node.compactOwnedBoxesOnce();
        assertThat(p0.referencedSyrups()).doesNotContain(syrup);
        return syrup;
    }

    @Test
    void renameBackIntoThePartitionThatOrphanedTheSyrupKeepsItAlive() {
        CandyKey a = keyIn(0, "a");
        CandyKey b = keyIn(1, "b");
        orphanInP0(a, b);

        rename(p1, b, p0, a); // p0 references the orphaned Syrup again
        p1.flush();
        node.compactOwnedBoxesOnce();

        clock.advance(GRACE + 1);
        node.collectGarbageOnce();
        assertThat(p0.getCandy(a)).isEqualTo(bytes("precious"));
    }

    @Test
    void zeroCopyCopyBackIntoThePartitionThatOrphanedTheSyrupKeepsItAlive() {
        CandyKey a = keyIn(0, "a");
        CandyKey b = keyIn(1, "b");
        CandyKey c = keyIn(0, "c");
        orphanInP0(a, b);

        CandyLocator locator = p1.resolveLocator(b);
        p0.zeroCopyPut(c, locator.parts(), null, Map.of(), 0L, ObjectAcl.NONE, null);
        p0.flush();
        p1.deleteCandy(b);
        p1.flush();
        node.compactOwnedBoxesOnce();

        clock.advance(GRACE + 1);
        node.collectGarbageOnce();
        assertThat(p0.getCandy(c)).isEqualTo(bytes("precious"));
    }

    @Test
    void aSyrupOrphanedAgainAfterBeingReReferencedRestartsItsGracePeriod() {
        CandyKey a = keyIn(0, "a");
        CandyKey b = keyIn(1, "b");
        long syrup = orphanInP0(a, b);

        clock.advance(GRACE / 2);
        rename(p1, b, p0, a);
        p0.flush();
        p1.flush();
        node.compactOwnedBoxesOnce();

        clock.advance(GRACE / 2);
        p0.deleteCandy(a); // orphaned again, half a grace period after it was first orphaned
        p0.flush();
        node.compactOwnedBoxesOnce();
        assertThat(p0.referencedSyrups()).doesNotContain(syrup);

        clock.advance(GRACE / 2 + 1); // past the first orphaning's grace, not the second's
        node.collectGarbageOnce();
        assertThat(store.listLedgers()).contains(syrup);

        clock.advance(GRACE / 2);
        node.collectGarbageOnce();
        assertThat(store.listLedgers()).doesNotContain(syrup);
    }
}
