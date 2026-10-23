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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.StorageException;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** GC reclaims every ledger that leaves a partition's committed state, across failures and handover. */
class LedgerReclamationTest {

    private static final BoxName BOX = BoxName.of("reclaim-box");
    private static final long GRACE = 1_000;

    private final FlakyLedgerStore store = new FlakyLedgerStore(new InMemoryLedgerStore());
    private final ManualClock clock = new ManualClock(1_000);
    private final InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
    private final CandyboxConfig config = CandyboxConfig.builder()
            .syrupRolloverBytes(1)              // every Candy (or part) in its own Syrup
            .l0CompactionTrigger(2)
            .ledgerGcGraceMillis(GRACE)
            .ownershipLeaseTtlMillis(10_000)
            .leaseRenewIntervalMillis(0)
            .compactionIntervalMillis(0)
            .balancerIntervalMillis(0)
            .build();
    private final CandyboxNode nodeA = new CandyboxNode(1, config, store, coordination, clock);
    private final CandyboxNode nodeB = new CandyboxNode(2, config, store, coordination, clock);

    @AfterEach
    void tearDown() {
        nodeA.close();
        nodeB.close();
        store.close();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private boolean exists(long ledgerId) {
        return store.listLedgers().contains(ledgerId);
    }

    @Test
    void failedDeletesStayPendingAndAreRetriedOnTheNextPass() {
        nodeA.createBox(BOX, 1);
        BoxEngine engine = nodeA.enginePartition(BOX, 0);
        String uploadId = engine.createMultipartUpload(CandyKey.of("mpu"), null, Map.of());
        engine.uploadPart(uploadId, 1, bytes("part"));
        long partSyrup = engine.multipartUpload(uploadId).parts().get(1).segments().get(0).syrupId();
        engine.putCandy(CandyKey.of("k"), bytes("v"), null, Map.of(), null); // seals the part's Syrup
        engine.abortMultipartUpload(uploadId);
        long rotatedWal = engine.manifestState().walLedgerId();
        engine.flush();
        clock.advance(GRACE + 1);

        store.failDeletes = true;
        assertThat(nodeA.collectGarbageOnce()).isZero();
        assertThat(exists(rotatedWal)).isTrue();
        assertThat(exists(partSyrup)).isTrue();
        assertThat(engine.manifestState().pendingDeletes()).containsKeys(rotatedWal, partSyrup);
        assertThat(engine.manifestState().liveSyrups()).doesNotContain(partSyrup);

        store.failDeletes = false;
        assertThat(nodeA.collectGarbageOnce()).isEqualTo(2);
        assertThat(exists(rotatedWal)).isFalse();
        assertThat(exists(partSyrup)).isFalse();
        assertThat(engine.manifestState().pendingDeletes()).isEmpty();
        assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v"));
    }

    @Test
    void handoverReclaimsThePriorManifestAndThePriorWalOnceItsReplayIsFlushed() {
        nodeA.createBox(BOX, 1);
        BoxEngine ownerA = nodeA.enginePartition(BOX, 0);
        ownerA.putCandy(CandyKey.of("k"), bytes("v1"), null, Map.of(), null); // only in A's WAL
        long priorManifest = ownerA.manifestLedgerId();
        long priorWal = ownerA.manifestState().walLedgerId();

        clock.advance(11_000); // A's lease lapses
        nodeB.openBox(BOX);
        BoxEngine ownerB = nodeB.enginePartition(BOX, 0);

        clock.advance(GRACE + 1);
        nodeB.collectGarbageOnce();
        assertThat(exists(priorManifest)).isFalse();
        assertThat(exists(priorWal)).isTrue(); // still the only durable copy of "k"

        ownerB.flush();
        clock.advance(GRACE + 1);
        nodeB.collectGarbageOnce();
        assertThat(exists(priorWal)).isFalse();
        assertThat(ownerB.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v1"));
    }

    @Test
    void plannedHandoverReclaimsThePriorWalAndInheritsPendingDeletes() {
        nodeA.createBox(BOX, 1);
        BoxEngine ownerA = nodeA.enginePartition(BOX, 0);
        ownerA.putCandy(CandyKey.of("a"), bytes("1"), null, Map.of(), null);
        ownerA.flush();
        ownerA.putCandy(CandyKey.of("b"), bytes("2"), null, Map.of(), null);
        ownerA.flush();
        Set<Long> inputs = ownerA.manifestState().tables().stream().map(SSTableMeta::ledgerId)
                .collect(Collectors.toSet());
        assertThat(nodeA.compactOwnedBoxesOnce()).isPositive();
        long priorManifest = ownerA.manifestLedgerId();
        long priorWal = ownerA.manifestState().walLedgerId();

        // A hands over before its grace period elapses, so only B can still reclaim A's obsolete ledgers.
        nodeA.releasePartitionForHandover(BOX, 0);
        nodeB.openPartition(BOX, 0);
        BoxEngine ownerB = nodeB.enginePartition(BOX, 0);
        assertThat(ownerB.manifestState().pendingDeletes()).containsKeys(inputs.toArray(Long[]::new));

        clock.advance(GRACE + 1);
        nodeB.collectGarbageOnce();
        assertThat(inputs).noneMatch(this::exists);
        assertThat(exists(priorManifest)).isFalse();
        assertThat(exists(priorWal)).isFalse();
        assertThat(ownerB.getCandy(CandyKey.of("a"))).isEqualTo(bytes("1"));
        assertThat(ownerB.getCandy(CandyKey.of("b"))).isEqualTo(bytes("2"));
    }

    @Test
    void anAbortedUploadsPartOnlySyrupIsReclaimed() {
        nodeA.createBox(BOX, 1);
        BoxEngine engine = nodeA.enginePartition(BOX, 0);
        String uploadId = engine.createMultipartUpload(CandyKey.of("mpu"), null, Map.of());
        engine.uploadPart(uploadId, 1, bytes("part"));
        List<SegmentRef> segments = engine.multipartUpload(uploadId).parts().get(1).segments();
        long partSyrup = segments.get(0).syrupId();
        assertThat(engine.manifestState().liveSyrups()).contains(partSyrup);

        engine.putCandy(CandyKey.of("k"), bytes("v"), null, Map.of(), null); // seals the part's Syrup
        clock.advance(GRACE + 1);
        nodeA.collectGarbageOnce();
        assertThat(exists(partSyrup)).isTrue(); // still referenced by the in-flight upload

        engine.abortMultipartUpload(uploadId);
        clock.advance(GRACE + 1);
        nodeA.collectGarbageOnce();
        assertThat(exists(partSyrup)).isFalse();
    }

    /** Delegates to a real store, optionally failing every delete. */
    private static final class FlakyLedgerStore implements LedgerStore {
        private final LedgerStore delegate;
        volatile boolean failDeletes;

        FlakyLedgerStore(LedgerStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public WritableLedger createLedger(LedgerConfig config) {
            return delegate.createLedger(config);
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
            if (failDeletes) {
                throw new StorageException("injected delete failure for ledger " + ledgerId);
            }
            delegate.deleteLedger(ledgerId);
        }

        @Override
        public List<Long> listLedgers() {
            return delegate.listLedgers();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
