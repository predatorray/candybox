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

import static org.assertj.core.api.Assertions.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import me.predatorray.candybox.bookkeeper.*;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.*;
import me.predatorray.candybox.common.config.*;
import me.predatorray.candybox.common.exception.FencedException;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import org.junit.jupiter.api.Test;

class LedgerRetentionTest {
    private final ManualClock clock = new ManualClock(1_000);
    private final InMemoryLedgerStore store = new InMemoryLedgerStore(clock);
    private final InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
    private final LedgerRetention retention = new LedgerRetention(store, coordination, clock);

    LedgerRetentionTest() {
        coordination.create(me.predatorray.candybox.coordination.CandyboxKeys.boxMetaKey("box"),
                new me.predatorray.candybox.coordination.BoxDescriptor(2, "generation").encode());
    }

    private WritableLedger syrup(LedgerStore scope) {
        return scope.createLedger(new LedgerConfig(new QuorumConfig(1, 1, 1), Map.of(
                "candybox-box", "box".getBytes(StandardCharsets.UTF_8),
                "candybox-role", "SYRUP".getBytes(StandardCharsets.UTF_8))));
    }

    @Test void oldAllocationCannotBeClaimedWhileItsWriterCanPublish() {
        var scope = retention.allocate("box", "generation", 0);
        var ledger = syrup(scope);
        clock.advance(100_000);
        assertThat(retention.reconcile(100, 100, false).deleted()).isZero();
        assertThat(ledger.append(new byte[]{1})).isZero();
    }

    @Test void retiredUnreferencedAllocationIsReclaimedButInheritedRootIsKept() {
        var old = retention.allocate("box", "generation", 0);
        var orphan = syrup(old);
        var live = syrup(old);
        orphan.close(); live.close();
        var successor = retention.allocate("box", "generation", 0);
        successor.inheritAndRetire(Set.of(live.ledgerId()));
        clock.advance(1_000);
        assertThat(retention.reconcile(100, 100, false).deleted()).isEqualTo(1);
        assertThat(store.listLedgers()).containsExactly(live.ledgerId());
    }

    @Test void youngAndBackupPinnedAllocationsAreRetainedAndReleasedPinsAreReclaimed() {
        var old = retention.allocate("box", "generation", 0);
        var ledger = syrup(old);
        ledger.close();
        retention.pinBackup("backup", Set.of(ledger.ledgerId()));
        retention.allocate("box", "generation", 0).inheritAndRetire(Set.of());
        assertThat(retention.reconcile(100, 100, false).deleted()).isZero();
        clock.advance(1_000);
        assertThat(retention.reconcile(100, 100, false).deleted()).isZero();
        retention.releaseBackup("backup", Set.of(ledger.ledgerId()));
        assertThat(retention.reconcile(100, 100, false).deleted()).isEqualTo(1);
        assertThatThrownBy(() -> retention.pinBackup("late", Set.of(ledger.ledgerId())))
                .isInstanceOf(FencedException.class);
    }

    @Test void delayedTransferIsPinnedUntilDestinationEpochIsFenced() {
        var source = retention.allocate("box", "generation", 0);
        var ledger = syrup(source);
        ledger.close();
        var destination = retention.allocate("box", "generation", 1);
        destination.retainLedgers(Set.of(ledger.ledgerId()));
        retention.allocate("box", "generation", 0).inheritAndRetire(Set.of());
        clock.advance(1_000);
        assertThat(retention.reconcile(100, 100, false).deleted()).isZero();
        retention.allocate("box", "generation", 1).inheritAndRetire(Set.of());
        assertThat(retention.reconcile(100, 100, false).deleted()).isEqualTo(1);
        assertThatThrownBy(() -> destination.retainLedgers(Set.of(ledger.ledgerId())))
                .isInstanceOf(FencedException.class);
    }

    @Test void recreatedBoxReclaimsOnlyTheEarlierIncarnation() {
        String metaKey = me.predatorray.candybox.coordination.CandyboxKeys.boxMetaKey("box");
        coordination.compareAndSet(metaKey,
                new me.predatorray.candybox.coordination.BoxDescriptor(1, "old").encode(), 0);
        var old = retention.allocate("box", "old", 0);
        var ledger = syrup(old);
        ledger.close();
        coordination.compareAndSet(metaKey,
                new me.predatorray.candybox.coordination.BoxDescriptor(1, "new").encode(), 1);
        var fresh = retention.allocate("box", "new", 0);
        var current = syrup(fresh); current.close();
        fresh.inheritAndRetire(Set.of());
        clock.advance(1_000);
        assertThat(retention.reconcile(100, 100, false).deleted()).isEqualTo(1);
        assertThat(store.listLedgers()).containsExactly(current.ledgerId());
    }

    @Test void boundedEnumerationMakesProgressPastHeldLedgers() {
        var active = retention.allocate("box", "generation", 1);
        syrup(active).close();
        var old = retention.allocate("box", "generation", 0);
        var orphan = syrup(old); orphan.close();
        retention.allocate("box", "generation", 0).inheritAndRetire(Set.of());
        clock.advance(1_000);
        assertThat(retention.reconcile(100, 1, false).deleted()).isZero();
        assertThat(retention.reconcile(100, 1, false).deleted()).isEqualTo(1);
        assertThat(store.listLedgers()).doesNotContain(orphan.ledgerId());
    }
    @Test void ownerRotationReclaimsFailedOutputAndPreservesCommittedCandy() {
        CandyboxConfig config = CandyboxConfig.builder().ledgerEnumerationGraceMillis(100)
                .leaseRenewIntervalMillis(0).compactionIntervalMillis(0).balancerIntervalMillis(0)
                .ownershipLeaseTtlMillis(100_000).build();
        coordination.delete(me.predatorray.candybox.coordination.CandyboxKeys.boxMetaKey("box"), 0);
        try (CandyboxNode node = new CandyboxNode(1, config, store, coordination, clock)) {
            BoxName box = BoxName.of("box");
            node.createBox(box, 1);
            var engine = node.enginePartition(box, 0);
            engine.putCandy(CandyKey.of("key"), new byte[]{42}, null, Map.of(), null);
            var orphan = store.createLedger(engine.sstableLedgerConfig());
            orphan.close();
            clock.advance(1_000);
            node.rotateAllocationEpochs();
            node.reconcileAllocations();
            assertThat(store.listLedgers()).doesNotContain(orphan.ledgerId());
            assertThat(node.enginePartition(box, 0).getCandy(CandyKey.of("key"))).containsExactly((byte) 42);
            assertThatThrownBy(() -> engine.putCandy(CandyKey.of("late"), new byte[]{1}, null, Map.of(), null))
                    .isInstanceOf(FencedException.class);
        }
    }

    @Test void appliedButFailedPointerCasIsRecoveredWithoutDeletingItsOutputs() {
        CandyboxConfig config = CandyboxConfig.builder().ledgerEnumerationGraceMillis(100)
                .leaseRenewIntervalMillis(0).compactionIntervalMillis(0).balancerIntervalMillis(0)
                .ownershipLeaseTtlMillis(100_000).build();
        BoxName box = BoxName.of("box");
        coordination.delete(me.predatorray.candybox.coordination.CandyboxKeys.boxMetaKey("box"), 0);
        try (CandyboxNode node = new CandyboxNode(1, config, store, coordination, clock)) {
            node.createBox(box, 1);
            node.enginePartition(box, 0).putCandy(CandyKey.of("key"), new byte[]{7}, null, Map.of(), null);
            node.releaseBox(box);
            var armed = new java.util.concurrent.atomic.AtomicBoolean(true);
            var ambiguous = (me.predatorray.candybox.coordination.CoordinationService)
                    java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{me.predatorray.candybox.coordination.CoordinationService.class},
                    (proxy, method, args) -> {
                        try {
                            Object result = method.invoke(coordination, args);
                            if (method.getName().equals("compareAndSet")
                                    && ((String) args[0]).contains("/incarnations/")
                                    && armed.getAndSet(false)) {
                                throw new me.predatorray.candybox.coordination.CoordinationException("lost response");
                            }
                            return result;
                        } catch (java.lang.reflect.InvocationTargetException failure) {
                            throw failure.getCause();
                        }
                    });
            assertThatThrownBy(() -> PartitionOwnership.recover(box, 0, config, store, ambiguous, 2, clock))
                    .isInstanceOf(me.predatorray.candybox.coordination.CoordinationException.class);
            clock.advance(1_000);
            retention.reconcile(100, 1024, false);
            node.openBox(box);
            assertThat(node.enginePartition(box, 0).getCandy(CandyKey.of("key"))).containsExactly((byte) 7);
        }
    }

    @Test void reportOnlyExplainsCandidateWithoutClaimingOrDeletingIt() {
        var scope = retention.allocate("box", "generation", 0);
        var ledger = syrup(scope); ledger.close();
        retention.allocate("box", "generation", 0).inheritAndRetire(Set.of());
        clock.advance(1_000);
        assertThat(retention.inspect(ledger.ledgerId(), 100).reason()).contains("eligible");
        assertThat(retention.reconcile(100, 100, true).deleted()).isZero();
        retention.pinBackup("after-report", Set.of(ledger.ledgerId()));
        assertThat(retention.inspect(ledger.ledgerId(), 100).knownRoots()).contains("backup:after-report");
        assertThat(retention.reconcile(100, 100, false).deleted()).isZero();
    }

    @Test void missingEpochAndUnknownRoleFailClosed() {
        var scope = retention.allocate("box", "generation", 0);
        var ledger = syrup(scope); ledger.close();
        var key = "retention/epochs/" + new String(scope.allocationMetadata()
                .get(LedgerRetention.EPOCH_TAG), StandardCharsets.UTF_8);
        coordination.delete(key, coordination.get(key).orElseThrow().version());
        clock.advance(1_000);
        assertThat(retention.reconcile(100, 100, false).errors()).isEqualTo(1);
        assertThat(store.listLedgers()).contains(ledger.ledgerId());
        var unknown = store.createLedger(new LedgerConfig(new QuorumConfig(1, 1, 1), Map.of(
                "candybox-role", "FUTURE".getBytes(StandardCharsets.UTF_8))));
        unknown.close();
        assertThat(retention.inspect(unknown.ledgerId(), 100).reason()).contains("legacy");
        assertThat(retention.reconcile(100, 100, false).deleted()).isZero();
    }

    @Test void deletedEpochAdmissionClosesBeforeRegisteredWritersAreFenced() {
        var scope = retention.allocate("box", "generation", 0);
        var writer = scope.createLedger(new LedgerConfig(new QuorumConfig(1, 1, 1), Map.of(
                "candybox-box", "box".getBytes(StandardCharsets.UTF_8),
                "candybox-role", "WAL".getBytes(StandardCharsets.UTF_8))));
        writer.append(new byte[]{1});
        String metaKey = me.predatorray.candybox.coordination.CandyboxKeys.boxMetaKey("box");
        coordination.delete(metaKey, 0);
        clock.advance(1_000);
        assertThat(retention.reconcile(100, 100, false).deleted()).isEqualTo(1);
        assertThatThrownBy(() -> writer.append(new byte[]{2})).isInstanceOf(FencedException.class);
        assertThatThrownBy(() -> syrup(scope)).isInstanceOf(FencedException.class);
    }

    @Test void deleteFailureSurvivesCollectorRestartAndIsRetried() {
        var scope = retention.allocate("box", "generation", 0);
        var ledger = syrup(scope); ledger.close();
        retention.allocate("box", "generation", 0).inheritAndRetire(Set.of());
        clock.advance(1_000);
        var failing = new AdversarialStore(store);
        failing.failDeletes = true;
        assertThat(new LedgerRetention(failing, coordination, clock).reconcile(100, 100, false).errors())
                .isEqualTo(1);
        assertThat(store.listLedgers()).contains(ledger.ledgerId());
        assertThat(retention.reconcile(100, 100, false).deleted()).isEqualTo(1);
    }

    @Test void consumerPausedBeforeWalAppendKeepsBytesUntilItsEpochIsFenced() throws Exception {
        var failing = new AdversarialStore(store);
        var registry = new LedgerRetention(failing, coordination, clock);
        var sourceScope = registry.allocate("box", "generation", 0);
        var destinationScope = registry.allocate("box", "generation", 1);
        CandyboxConfig config = CandyboxConfig.builder().build();
        var source = me.predatorray.candybox.lsm.engine.BoxEngine.createNew(
                BoxName.of("box"), config, sourceScope, 1, clock, 1);
        var destination = me.predatorray.candybox.lsm.engine.BoxEngine.createNew(
                BoxName.of("box"), config, destinationScope, 2, clock, 1);
        source.putCandy(CandyKey.of("key"), new byte[]{42}, null, Map.of(), null);
        var locator = source.resolveLocator(CandyKey.of("key"));
        long syrup = locator.segments().get(0).syrupId();
        var entered = new java.util.concurrent.CountDownLatch(1);
        var resume = new java.util.concurrent.CountDownLatch(1);
        failing.beforeWalAppend = () -> {
            entered.countDown();
            try {
                if (!resume.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
                    throw new AssertionError("WAL append was never resumed");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
        };
        var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            var consumer = executor.submit(() -> destination.zeroCopyPut(CandyKey.of("copy"),
                    locator.parts(), locator.contentType(), locator.userMetadata(), locator.createdAtMillis(),
                    locator.acl(), null));
            assertThat(entered.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            source.close();
            registry.allocate("box", "generation", 0).inheritAndRetire(Set.of());
            clock.advance(100_000);
            registry.reconcile(100, 100, false);
            assertThat(store.listLedgers()).contains(syrup);
            resume.countDown();
            consumer.get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(destination.getCandy(CandyKey.of("copy"))).containsExactly((byte) 42);
        } finally {
            resume.countDown();
            executor.shutdownNow();
            source.close();
            destination.close();
        }
    }

    private static final class AdversarialStore implements LedgerStore {
        private final LedgerStore delegate;
        volatile boolean failDeletes;
        volatile Runnable beforeWalAppend;
        AdversarialStore(LedgerStore delegate) { this.delegate = delegate; }
        @Override public WritableLedger createLedger(LedgerConfig config) {
            WritableLedger writer = delegate.createLedger(config);
            byte[] role = config.customMetadata().get("candybox-role");
            if (role == null || !new String(role, StandardCharsets.UTF_8).equals("WAL")) { return writer; }
            return new WritableLedger() {
                @Override public long append(byte[] bytes) {
                    Runnable action = beforeWalAppend;
                    if (action != null) { action.run(); }
                    return writer.append(bytes);
                }
                @Override public long ledgerId() { return writer.ledgerId(); }
                @Override public long lastAddConfirmed() { return writer.lastAddConfirmed(); }
                @Override public boolean isSealed() { return writer.isSealed(); }
                @Override public Map<String, byte[]> customMetadata() { return writer.customMetadata(); }
                @Override public LedgerEntry read(long entry) { return writer.read(entry); }
                @Override public java.util.List<LedgerEntry> readRange(long first, long last) {
                    return writer.readRange(first, last);
                }
                @Override public void close() { writer.close(); }
            };
        }
        @Override public ReadableLedger openLedger(long id) { return delegate.openLedger(id); }
        @Override public ReadableLedger recoverOpen(long id) { return delegate.recoverOpen(id); }
        @Override public LedgerMetadata readMetadata(long id) { return delegate.readMetadata(id); }
        @Override public java.util.List<Long> listLedgers() { return delegate.listLedgers(); }
        @Override public void deleteLedger(long id) {
            if (failDeletes) { throw new me.predatorray.candybox.common.exception.StorageException("delete failed"); }
            delegate.deleteLedger(id);
        }
        @Override public void close() { delegate.close(); }
    }

    @Test void staleOwnerCannotRetireNewerOrSameTokenEpoch() {
        var newer = retention.allocate("box", "generation", 0, 3);
        var ledger = syrup(newer); ledger.close();
        var stale = retention.allocate("box", "generation", 0, 2);
        stale.inheritAndRetire(Set.of());
        var sameToken = retention.allocate("box", "generation", 0, 3);
        sameToken.inheritAndRetire(Set.of());
        clock.advance(1_000);
        retention.reconcile(100, 100, false);
        assertThat(store.listLedgers()).contains(ledger.ledgerId());
        retention.allocate("box", "generation", 0, 4).inheritAndRetire(Set.of());
        assertThat(retention.reconcile(100, 100, false).deleted()).isEqualTo(1);
    }

    @Test void claimedPendingDeleteDoesNotPreventRecoveryWhetherDeleteSucceededOrFailed() {
        for (boolean physicallyDeleted : new boolean[]{false, true}) {
            var prior = retention.allocate("box", "generation", 0);
            var ledger = syrup(prior); ledger.close();
            assertThat(retention.claim(ledger.ledgerId())).isTrue();
            if (physicallyDeleted) { store.deleteLedger(ledger.ledgerId()); }
            var successor = retention.allocate("box", "generation", 0);
            successor.inheritAndRetire(Set.of(ledger.ledgerId()), Set.of(ledger.ledgerId()));
            clock.advance(1_000);
            retention.reconcile(100, 100, false);
            assertThat(store.listLedgers()).doesNotContain(ledger.ledgerId());
        }
    }

    @Test void disablingEnumerationContinuesFromLatestIncarnationPointer() {
        String metaKey = me.predatorray.candybox.coordination.CandyboxKeys.boxMetaKey("box");
        coordination.delete(metaKey, 0);
        CandyboxConfig enabled = CandyboxConfig.builder().ledgerEnumerationGraceMillis(100)
                .leaseRenewIntervalMillis(0).compactionIntervalMillis(0).balancerIntervalMillis(0).build();
        CandyboxConfig disabled = CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0).compactionIntervalMillis(0).balancerIntervalMillis(0).build();
        BoxName box = BoxName.of("box");
        try (var first = new CandyboxNode(1, enabled, store, coordination, clock)) {
            first.createBox(box, 1);
            first.enginePartition(box, 0).putCandy(CandyKey.of("key"), new byte[]{9}, null, Map.of(), null);
            first.releaseBox(box);
            try (var second = new CandyboxNode(2, disabled, store, coordination, clock)) {
                second.openBox(box);
                assertThat(second.enginePartition(box, 0).getCandy(CandyKey.of("key"))).containsExactly((byte) 9);
            }
        }
    }

    @Test void ownerHandoverRetriesClaimedDeletesAndToleratesCrashBeforeForget() {
        CandyboxConfig config = CandyboxConfig.builder().ledgerEnumerationGraceMillis(100)
                .ledgerGcGraceMillis(0).memtableFlushThresholdBytes(1)
                .leaseRenewIntervalMillis(0).compactionIntervalMillis(0).balancerIntervalMillis(0).build();
        for (boolean physicallyDeleted : new boolean[]{false, true}) {
            BoxName box = BoxName.of(physicallyDeleted ? "deleted-pending" : "failed-pending");
            try (var first = new CandyboxNode(1, config, store, coordination, clock);
                 var second = new CandyboxNode(2, config, store, coordination, clock)) {
                first.createBox(box, 1);
                var engine = first.enginePartition(box, 0);
                long oldWal = engine.manifestState().walLedgerId();
                engine.putCandy(CandyKey.of("key"), new byte[]{11}, null, Map.of(), null);
                assertThat(engine.manifestState().pendingDeletes()).containsKey(oldWal);
                assertThat(retention.deletePending(engine, oldWal)).isTrue();
                if (physicallyDeleted) { store.deleteLedger(oldWal); }
                first.releaseBox(box);
                second.openBox(box);
                second.collectGarbageOnce();
                assertThat(store.listLedgers()).doesNotContain(oldWal);
                assertThat(second.enginePartition(box, 0).getCandy(CandyKey.of("key")))
                        .containsExactly((byte) 11);
            }
        }
    }

    @Test void migratedBoxCanBeDeletedAndRecreatedAfterDisablingEnumeration() {
        coordination.delete(me.predatorray.candybox.coordination.CandyboxKeys.boxMetaKey("box"), 0);
        CandyboxConfig enabled = CandyboxConfig.builder().ledgerEnumerationGraceMillis(100)
                .leaseRenewIntervalMillis(0).compactionIntervalMillis(0).balancerIntervalMillis(0).build();
        CandyboxConfig disabled = CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0).compactionIntervalMillis(0).balancerIntervalMillis(0).build();
        BoxName box = BoxName.of("box");
        try (var initial = new CandyboxNode(1, disabled, store, coordination, clock)) {
            initial.createBox(box, 1);
            initial.enginePartition(box, 0).putCandy(CandyKey.of("old"), new byte[]{3}, null, Map.of(), null);
        }
        try (var migrated = new CandyboxNode(2, enabled, store, coordination, clock)) {
            migrated.openBox(box);
            migrated.deleteBox(box, true);
        }
        try (var recreated = new CandyboxNode(3, disabled, store, coordination, clock)) {
            recreated.createBox(box, 1);
            recreated.enginePartition(box, 0).putCandy(CandyKey.of("new"), new byte[]{4}, null, Map.of(), null);
            assertThatThrownBy(recreated::retention).isInstanceOf(IllegalStateException.class);
        }
        try (var recovered = new CandyboxNode(4, disabled, store, coordination, clock)) {
            recovered.openBox(box);
            assertThat(recovered.enginePartition(box, 0).getCandy(CandyKey.of("new"))).containsExactly((byte) 4);
            assertThatThrownBy(() -> recovered.enginePartition(box, 0).getCandy(CandyKey.of("old")))
                    .isInstanceOf(me.predatorray.candybox.common.exception.CandyNotFoundException.class);
        }
    }

    @Test void emptyManifestContainerDoesNotBlockNewIncarnation() {
        coordination.delete(me.predatorray.candybox.coordination.CandyboxKeys.boxMetaKey("box"), 0);
        coordination.create(me.predatorray.candybox.coordination.CandyboxKeys.manifestKey("box", 0), new byte[0]);
        CandyboxConfig disabled = CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0).compactionIntervalMillis(0).balancerIntervalMillis(0).build();
        try (var node = new CandyboxNode(1, disabled, store, coordination, clock)) {
            node.createBox(BoxName.of("box"), 1);
        }
    }

    @Test void sealedWriterRegistrationsDoNotGrowAcrossWalRotation() {
        var scope = retention.allocate("box", "generation", 0);
        var config = new LedgerConfig(new QuorumConfig(1, 1, 1), Map.of(
                "candybox-box", "box".getBytes(StandardCharsets.UTF_8),
                "candybox-role", "WAL".getBytes(StandardCharsets.UTF_8)));
        for (int i = 0; i < 2_000; i++) {
            try (var writer = scope.createLedger(config)) { writer.append(new byte[]{1}); }
        }
        String epochId = new String(scope.allocationMetadata().get(LedgerRetention.EPOCH_TAG), StandardCharsets.UTF_8);
        byte[] encoded = coordination.get("retention/epochs/" + epochId).orElseThrow().value();
        assertThat(LedgerRetention.Epoch.decode(encoded).writers()).isEmpty();
        assertThat(encoded.length).isLessThan(512);
    }

}
