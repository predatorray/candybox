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

import java.io.IOException;
import java.io.InputStream;
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
import me.predatorray.candybox.common.exception.FencedException;
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
        engine.putCandy(CandyKey.of("a"), bytes("1"), null, Map.of(), null); // seals the part's Syrup
        engine.abortMultipartUpload(uploadId);
        long firstWal = engine.manifestState().walLedgerId();
        engine.flush();
        engine.putCandy(CandyKey.of("b"), bytes("2"), null, Map.of(), null);
        long secondWal = engine.manifestState().walLedgerId();
        engine.flush();
        Set<Long> compacted = engine.manifestState().tables().stream().map(SSTableMeta::ledgerId)
                .collect(Collectors.toSet());
        assertThat(nodeA.compactOwnedBoxesOnce()).isPositive();
        clock.advance(GRACE + 1);

        store.failDeletes = true;
        assertThat(nodeA.collectGarbageOnce()).isZero();
        Set<Long> pending = Set.copyOf(engine.manifestState().pendingDeletes().keySet());
        Set<Long> expected = new java.util.HashSet<>(compacted);
        expected.addAll(List.of(partSyrup, firstWal, secondWal));
        assertThat(pending).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(pending).allMatch(this::exists);
        assertThat(engine.manifestState().liveSyrups()).doesNotContain(partSyrup);

        store.failDeletes = false;
        assertThat(nodeA.collectGarbageOnce()).isEqualTo(pending.size());
        assertThat(pending).noneMatch(this::exists);
        assertThat(engine.manifestState().pendingDeletes()).isEmpty();
        assertThat(engine.getCandy(CandyKey.of("a"))).isEqualTo(bytes("1"));
        assertThat(engine.getCandy(CandyKey.of("b"))).isEqualTo(bytes("2"));
    }

    @Test
    void anAlreadyDeletedPendingLedgerIsDroppedFromTheSet() {
        nodeA.createBox(BOX, 1);
        BoxEngine engine = nodeA.enginePartition(BOX, 0);
        engine.putCandy(CandyKey.of("k"), bytes("v"), null, Map.of(), null);
        long rotatedWal = engine.manifestState().walLedgerId();
        engine.flush();
        store.deleteLedger(rotatedWal); // e.g. deleted by a pass whose forget edit then failed
        clock.advance(GRACE + 1);

        nodeA.collectGarbageOnce();
        assertThat(engine.manifestState().pendingDeletes()).doesNotContainKey(rotatedWal);
    }

    @Test
    void aSyrupSupersededBeforeTheFirstFlushIsReclaimed() {
        nodeA.createBox(BOX, 1);
        BoxEngine engine = nodeA.enginePartition(BOX, 0);
        engine.putCandy(CandyKey.of("k"), bytes("v1"), null, Map.of(), null);
        long v1Syrup = engine.resolveLocator(CandyKey.of("k")).parts().get(0).segments().get(0)
                .syrupId();
        engine.putCandy(CandyKey.of("k"), bytes("v2"), null, Map.of(), null);
        engine.putCandy(CandyKey.of("x"), bytes("v3"), null, Map.of(), null);
        engine.flush();
        clock.advance(GRACE + 1);

        nodeA.collectGarbageOnce();
        assertThat(exists(v1Syrup)).isFalse();
        assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v2"));
        assertThat(engine.getCandy(CandyKey.of("x"))).isEqualTo(bytes("v3"));
    }

    @Test
    void handoverFlushesTheReplayedWalAndReclaimsIt() {
        nodeA.createBox(BOX, 1);
        BoxEngine ownerA = nodeA.enginePartition(BOX, 0);
        ownerA.putCandy(CandyKey.of("k"), bytes("v1"), null, Map.of(), null); // only in A's WAL
        long priorManifest = ownerA.manifestLedgerId();
        long priorWal = ownerA.manifestState().walLedgerId();

        clock.advance(11_000); // A's lease lapses
        nodeB.openBox(BOX);
        BoxEngine ownerB = nodeB.enginePartition(BOX, 0);

        clock.advance(GRACE + 1);
        assertThat(nodeA.collectGarbageOnce()).isZero(); // its lease gate skips the partition
        assertThat(exists(priorManifest)).isTrue();
        nodeB.collectGarbageOnce();
        assertThat(exists(priorManifest)).isFalse();
        assertThat(exists(priorWal)).isFalse(); // its replay is durable in B's L0 table
        assertThat(ownerB.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v1"));
    }

    @Test
    void aHandoverWhoseRecoveryFlushFailsKeepsThePriorWal() {
        nodeA.createBox(BOX, 1);
        BoxEngine ownerA = nodeA.enginePartition(BOX, 0);
        ownerA.putCandy(CandyKey.of("k"), bytes("v1"), null, Map.of(), null);
        long priorManifest = ownerA.manifestLedgerId();
        long priorWal = ownerA.manifestState().walLedgerId();

        clock.advance(11_000);
        store.failCreatesOfRole = "SSTABLE";
        assertThatThrownBy(() -> nodeB.openBox(BOX)).isInstanceOf(StorageException.class);
        assertThat(ManifestPointer.decode(coordination.get(PartitionOwnership.manifestKey(BOX, 0))
                .orElseThrow().value()).ledgerId()).isEqualTo(priorManifest);
        assertThat(exists(priorManifest)).isTrue();
        assertThat(exists(priorWal)).isTrue();

        nodeB.openBox(BOX);
        BoxEngine ownerB = nodeB.enginePartition(BOX, 0);
        clock.advance(GRACE + 1);
        nodeB.collectGarbageOnce();
        assertThat(exists(priorWal)).isFalse();
        assertThat(ownerB.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v1"));
    }

    @Test
    void aFailedFlushKeepsItsWalUntilARetrySucceeds() {
        nodeA.createBox(BOX, 1);
        BoxEngine engine = nodeA.enginePartition(BOX, 0);
        engine.putCandy(CandyKey.of("k"), bytes("v1"), null, Map.of(), null);
        long wal = engine.manifestState().walLedgerId();

        store.failCreatesOfRole = "SSTABLE";
        assertThatThrownBy(engine::flush).isInstanceOf(StorageException.class);
        clock.advance(GRACE + 1);
        nodeA.collectGarbageOnce();
        assertThat(exists(wal)).isTrue();
        assertThat(engine.manifestState().walLedgerId()).isEqualTo(wal);
        assertThat(engine.manifestState().pendingDeletes()).doesNotContainKey(wal);

        engine.flush();
        clock.advance(GRACE + 1);
        nodeA.collectGarbageOnce();
        assertThat(exists(wal)).isFalse();
        assertThat(engine.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v1"));
    }

    @Test
    void aSyrupSupersededInAReplayedWalIsReclaimedAfterHandover() {
        nodeA.createBox(BOX, 1);
        BoxEngine ownerA = nodeA.enginePartition(BOX, 0);
        ownerA.putCandy(CandyKey.of("k"), bytes("v1"), null, Map.of(), null);
        long v1Syrup = ownerA.resolveLocator(CandyKey.of("k")).parts().get(0).segments().get(0)
                .syrupId();
        ownerA.putCandy(CandyKey.of("k"), bytes("v2"), null, Map.of(), null); // A crashes unflushed

        clock.advance(11_000);
        nodeB.openBox(BOX);
        BoxEngine ownerB = nodeB.enginePartition(BOX, 0);
        clock.advance(GRACE + 1);
        nodeB.collectGarbageOnce();
        assertThat(exists(v1Syrup)).isFalse();
        assertThat(ownerB.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v2"));
    }

    @Test
    void aSyrupCreatedByAWriteThatFailedMidStreamIsReclaimed() {
        nodeA.createBox(BOX, 1);
        BoxEngine engine = nodeA.enginePartition(BOX, 0);
        engine.putCandy(CandyKey.of("a"), bytes("1"), null, Map.of(), null);
        Set<Long> before = Set.copyOf(store.listLedgers());
        int chunk = config.sizeLimits().chunkSizeBytes();
        InputStream disconnecting = new InputStream() {
            private long remaining = 2L * chunk;

            @Override
            public int read() throws IOException {
                if (remaining-- <= 0) {
                    throw new IOException("client disconnected");
                }
                return 'x';
            }
        };
        assertThatThrownBy(() -> engine.putCandy(CandyKey.of("big"), disconnecting, null, Map.of(),
                null)).isInstanceOf(StorageException.class);
        Set<Long> orphaned = store.listLedgers().stream().filter(id -> !before.contains(id))
                .collect(Collectors.toSet());
        assertThat(orphaned).isNotEmpty();

        engine.putCandy(CandyKey.of("b"), bytes("2"), null, Map.of(), null); // rolls off the last one
        engine.flush();
        clock.advance(GRACE + 1);
        nodeA.collectGarbageOnce();
        Set<Long> leftover = new java.util.HashSet<>(orphaned);
        leftover.remove(engine.resolveLocator(CandyKey.of("b")).parts().get(0).segments().get(0)
                .syrupId());
        leftover.retainAll(store.listLedgers());
        assertThat(leftover).isEmpty();
        assertThat(engine.getCandy(CandyKey.of("a"))).isEqualTo(bytes("1"));
        assertThat(engine.getCandy(CandyKey.of("b"))).isEqualTo(bytes("2"));
    }

    @Test
    void aDeposedOwnerThatMissedItsLeaseLossOnlyDeletesWhatTheNewOwnerAlsoTreatsAsPending() {
        nodeA.createBox(BOX, 1);
        BoxEngine ownerA = nodeA.enginePartition(BOX, 0);
        ownerA.putCandy(CandyKey.of("a"), bytes("1"), null, Map.of(), null);
        long rotatedWal = ownerA.manifestState().walLedgerId();
        ownerA.flush();
        ownerA.putCandy(CandyKey.of("b"), bytes("2"), null, Map.of(), null); // only in A's WAL

        clock.advance(11_000); // A's lease lapses
        nodeB.openBox(BOX);
        BoxEngine ownerB = nodeB.enginePartition(BOX, 0);
        assertThat(ownerB.manifestState().pendingDeletes()).containsKey(rotatedWal);
        clock.advance(GRACE + 1);

        // A stale A (e.g. resuming from a pause) bypasses the lease gate: its forget edit is fenced.
        GarbageCollector staleGc = new GarbageCollector(store, GRACE, clock);
        assertThatThrownBy(() -> staleGc.collectWithoutSyrups(ownerA))
                .isInstanceOf(FencedException.class);
        assertThat(exists(rotatedWal)).isFalse();
        assertThat(ownerB.getCandy(CandyKey.of("a"))).isEqualTo(bytes("1"));
        assertThat(ownerB.getCandy(CandyKey.of("b"))).isEqualTo(bytes("2"));

        nodeB.collectGarbageOnce();
        assertThat(ownerB.manifestState().pendingDeletes()).doesNotContainKey(rotatedWal);
    }

    @Test
    void aPendingLedgerThatIsStillInUseIsNeverDeleted() {
        nodeA.createBox(BOX, 1);
        BoxEngine engine = nodeA.enginePartition(BOX, 0);
        engine.putCandy(CandyKey.of("k"), bytes("v"), null, Map.of(), null);
        engine.flush();
        SSTableMeta table = engine.manifestState().tables().get(0);
        // A (hypothetical) trivial-move compaction: the table is both removed and re-added.
        engine.applyCompaction(me.predatorray.candybox.lsm.manifest.ManifestEdit.builder()
                .removedTableLedgerIds(Set.of(table.ledgerId())).addedTables(List.of(table)).build());
        assertThat(engine.manifestState().pendingDeletes()).containsKey(table.ledgerId());
        clock.advance(GRACE + 1);

        nodeA.collectGarbageOnce();
        assertThat(exists(table.ledgerId())).isTrue();
        assertThat(engine.manifestState().pendingDeletes()).containsKey(table.ledgerId());
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

    /** Delegates to a real store, optionally failing every delete or the next few creates. */
    private static final class FlakyLedgerStore implements LedgerStore {
        private final LedgerStore delegate;
        volatile boolean failDeletes;
        volatile String failCreatesOfRole; // the next create of this role fails

        FlakyLedgerStore(LedgerStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public WritableLedger createLedger(LedgerConfig config) {
            byte[] role = config.customMetadata().get("candybox-role");
            if (role != null && new String(role, StandardCharsets.UTF_8).equals(failCreatesOfRole)) {
                failCreatesOfRole = null;
                throw new StorageException("injected create failure");
            }
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
