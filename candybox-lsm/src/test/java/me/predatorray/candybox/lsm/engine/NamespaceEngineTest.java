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
import static me.predatorray.candybox.common.namespace.NamespaceOperation.*;
import java.util.*;
import java.util.concurrent.*;
import me.predatorray.candybox.bookkeeper.*;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.*;
import me.predatorray.candybox.common.config.*;
import me.predatorray.candybox.common.exception.*;
import me.predatorray.candybox.common.namespace.*;
import org.junit.jupiter.api.Test;

class NamespaceEngineTest {
    private static final CandyboxConfig CONFIG = CandyboxConfig.defaults();
    private static final BoxName BOX = BoxName.of("namespaces");
    private static BoxEngine.NamespaceResult call(BoxEngine e, NamespaceOperation op,
            String key, byte[] data, long version) {
        return e.namespace(op, "address", key, data, 100, version, 2, null, 10000);
    }
    private static void create(BoxEngine e) { assertThat(call(e, CREATE, null, null, 0).outcome()).isEqualTo("OK"); }

    @Test void renewalInvalidatesStaleClaimAndPreservesOtherKeys() {
        try (var store = new InMemoryLedgerStore();
             var e = BoxEngine.createNew(BOX, CONFIG, store, 1, new ManualClock(1000), 1)) {
            create(e);
            assertThat(call(e, PUT, "a", new byte[]{1}, 0).state().version()).isEqualTo(2);
            assertThat(call(e, PUT, "b", new byte[]{2}, 0).state().version()).isEqualTo(3);
            assertThat(call(e, CLAIM_EXPIRY, null, null, 2).outcome()).isEqualTo("STALE");
            assertThat(call(e, GET, "a", null, 0).data()).containsExactly((byte)1);
            assertThat(call(e, HEAD, null, null, 0).state().version()).isEqualTo(3);
            assertThat(call(e, DELETE, "b", null, 0).state().version()).isEqualTo(3);
            assertThat(call(e, LIST, null, null, 0).keys()).containsExactly("a");
        }
    }

    @Test void deadlineEqualityReadRetiresAndNeverResurrectsAfterRecoveryAndCleanup() {
        var clock = new ManualClock(1000);
        try (var store = new InMemoryLedgerStore();
             var e = BoxEngine.createNew(BOX, CONFIG, store, 1, clock, 1)) {
            create(e); call(e, PUT, "", new byte[]{42}, 0);
            clock.setTime(1099);
            assertThat(call(e, GET, "", null, 0).outcome()).isEqualTo("OK");
            clock.setTime(1100);
            assertThat(call(e, GET, "", null, 0).outcome()).isEqualTo("EXPIRED");
            clock.setTime(1000);
            assertThat(call(e, PUT, "late", new byte[]{3}, 0).outcome()).isEqualTo("EXPIRED");
            assertThat(call(e, CREATE, null, null, 0).outcome()).isEqualTo("EXISTS");
            assertThat(call(e, CLEANUP, null, null, 0).outcome()).isEqualTo("OK");
            e.flush();
            try (var recovered = BoxEngine.recover(BOX, CONFIG, store, 2, clock, e.manifestLedgerId(), 2)) {
                assertThat(call(recovered, HEAD, null, null, 0).outcome()).isEqualTo("EXPIRED");
                assertThat(call(recovered, CREATE, null, null, 0).outcome()).isEqualTo("EXISTS");
                assertThat(recovered.listCandies("i/", null, 128).entries()).isEmpty();
                assertThat(recovered.listCandies("l/", null, 128).entries()).hasSize(1);
            }
        }
    }

    @Test void rejectedOversizeAndUnavailablePayloadDoNotRenew() {
        var limits = new SizeLimits(1024, 100, 8192, 262144, 2);
        var config = CandyboxConfig.builder().sizeLimits(limits).build();
        try (var store = new InMemoryLedgerStore();
             var e = BoxEngine.createNew(BOX, config, store, 1, new ManualClock(1000), 1)) {
            create(e);
            assertThatThrownBy(() -> call(e, PUT, "a", new byte[3], 0)).isInstanceOf(LimitExceededException.class);
            assertThatThrownBy(() -> call(e, PUT, "a".repeat(100), new byte[1], 0)).isInstanceOf(LimitExceededException.class);
            store.setAvailableBookies(1);
            assertThatThrownBy(() -> call(e, PUT, "a", new byte[1], 0)).isInstanceOf(StorageException.class);
            store.setAvailableBookies(Integer.MAX_VALUE);
            assertThat(call(e, HEAD, null, null, 0).state().version()).isEqualTo(1);
        }
    }

    @Test void unicodeReservedAndEmptyItemsHaveDisjointKeysAndBoundedCleanup() {
        var clock = new ManualClock(1000);
        try (var store = new InMemoryLedgerStore();
             var e = BoxEngine.createNew(BOX, CONFIG, store, 1, clock, 1)) {
            create(e);
            for (String key : List.of("", "../l/address", "你好/🍬", "x")) { call(e, PUT, key, new byte[]{1}, 0); }
            assertThat(call(e, CLEAR, null, null, 0).cursor()).isNotNull();
            assertThat(e.listCandies(NamespaceKeys.prefix("address"), null, 128).entries()).hasSize(2);
            clock.setTime(1200);
            assertThat(call(e, CLAIM_EXPIRY, null, null, 5).outcome()).isEqualTo("RETIRED");
            call(e, CLEANUP, null, null, 0); call(e, CLEANUP, null, null, 0);
            assertThat(e.listCandies("i/", null, 128).entries()).isEmpty();
            assertThat(call(e, CREATE, null, null, 0).outcome()).isEqualTo("EXISTS");
        }
    }

    @Test void unknownAckAndCrashRecoverAllOrNoneAndPoisonOldView() {
        for (boolean durable : List.of(false, true)) {
            for (boolean crash : List.of(false, true)) {
                try (var store = new FaultStore();
                     var e = BoxEngine.createNew(BOX, CONFIG, store, 1, new ManualClock(1000), 1)) {
                    create(e);
                    store.targetWal = e.manifestState().walLedgerId();
                    store.durable = durable; store.crash = crash; store.failNext = true;
                    assertThatThrownBy(() -> call(e, PUT, "a", new byte[]{42}, 0))
                            .isInstanceOf(crash ? SimulatedCrash.class : StorageException.class);
                    assertThatThrownBy(() -> call(e, HEAD, null, null, 0)).isInstanceOf(IllegalStateException.class);
                    try (var recovered = BoxEngine.recover(BOX, CONFIG, store, 2, new ManualClock(1000), e.manifestLedgerId(), 2)) {
                        assertThat(call(recovered, HEAD, null, null, 0).state().version()).isEqualTo(durable ? 2 : 1);
                        assertThat(call(recovered, GET, "a", null, 0).outcome()).isEqualTo(durable ? "OK" : "ITEM_MISSING");
                    }
                }
            }
        }
    }

    @Test void pausedPutPublishesPairBeforeWaitingClaimOrReader() throws Exception {
        var clock = new ManualClock(1000);
        try (var store = new FaultStore();
             var e = BoxEngine.createNew(BOX, CONFIG, store, 1, clock, 1)) {
            create(e);
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            store.targetWal = e.manifestState().walLedgerId();
            store.pause = () -> { entered.countDown(); await(release); };
            ExecutorService pool = Executors.newFixedThreadPool(3);
            try {
                var put = pool.submit(() -> call(e, PUT, "a", new byte[]{42}, 0));
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                var claim = pool.submit(() -> call(e, CLAIM_EXPIRY, null, null, 1));
                var read = pool.submit(() -> call(e, GET, "a", null, 0));
                assertThat(read.isDone()).isFalse();
                release.countDown();
                assertThat(put.get(5, TimeUnit.SECONDS).state().version()).isEqualTo(2);
                assertThat(claim.get(5, TimeUnit.SECONDS).outcome()).isEqualTo("STALE");
                var result = read.get(5, TimeUnit.SECONDS);
                assertThat(result.state().version()).isEqualTo(2);
                assertThat(result.data()).containsExactly((byte)42);
            } finally { release.countDown(); pool.shutdownNow(); }
        }
    }

    @Test void newOwnerFencesDelayedOldPutAndRetirementAndClockSkewFailsClosed() {
        var clock = new ManualClock(1000);
        try (var store = new InMemoryLedgerStore();
             var e = BoxEngine.createNew(BOX, CONFIG, store, 1, clock, 1)) {
            create(e);
            try (var recovered = BoxEngine.recover(BOX, CONFIG, store, 2, clock, e.manifestLedgerId(), 2)) {
                assertThatThrownBy(() -> call(e, PUT, "a", new byte[]{1}, 0)).isInstanceOf(FencedException.class);
                call(recovered, PUT, "a", new byte[]{42}, 0);
                assertThat(call(recovered, GET, "a", null, 0).data()).containsExactly((byte)42);
            }
        }
        try (var store = new InMemoryLedgerStore();
             var e = BoxEngine.createNew(BOX, CONFIG, store, 1, new ManualClock(1000000), 1)) {
            create(e);
            assertThatThrownBy(() -> BoxEngine.recover(BOX, CONFIG, store, 2,
                    new ManualClock(0), e.manifestLedgerId(), 2)).isInstanceOf(IllegalStateException.class);
        }
    }
    @Test void retiredIdentitySurvivesCompactionAndTinyLiveItemPinsMixedSyrup() {
        var clock = new ManualClock(1000);
        var config = CandyboxConfig.builder().l0CompactionTrigger(2).build();
        try (var store = new InMemoryLedgerStore();
             var e = BoxEngine.createNew(BOX, config, store, 1, clock, 1)) {
            create(e);
            call(e, PUT, "dead", new byte[32768], 0);
            e.namespace(CREATE, "long-lived", null, null, 365L * 24 * 60 * 60 * 1000, 0, 2, null, 10000);
            e.namespace(PUT, "long-lived", "tiny", new byte[]{1}, 0, 0, 2, null, 10000);
            long shared = e.resolveLocator(CandyKey.of(NamespaceKeys.item("address", "dead"))).segments().get(0).syrupId();
            assertThat(e.resolveLocator(CandyKey.of(NamespaceKeys.item("long-lived", "tiny"))).segments().get(0).syrupId()).isEqualTo(shared);
            e.flush();
            clock.setTime(1100);
            call(e, CLAIM_EXPIRY, null, null, 2); call(e, CLEANUP, null, null, 0); e.flush();
            clock.advance(config.tombstoneGcGraceMillis() + 1);
            var compactor = new me.predatorray.candybox.lsm.compaction.Compactor(store,
                    new me.predatorray.candybox.lsm.sstable.SSTableWriter(store, config.bloomBitsPerKey()),
                    LedgerConfig.forRole(LedgerRole.SSTABLE), config.tombstoneGcGraceMillis(), clock);
            var task = new me.predatorray.candybox.lsm.compaction.LeveledCompactionStrategy(2)
                    .pickCompaction(e.manifestState()).orElseThrow();
            e.applyCompaction(compactor.compact(task).edit());
            assertThat(call(e, CREATE, null, null, 0).outcome()).isEqualTo("EXISTS");
            assertThat(e.claimReclaimableSyrups(Long.MAX_VALUE, Set.of())).doesNotContain(shared);
            assertThat(e.getCandy(CandyKey.of(NamespaceKeys.item("long-lived", "tiny")))).containsExactly((byte)1);
            // Logical deletion of 32768 bytes leaves >=32777 payload+CRC bytes pinned by 1 live byte.
            assertThat(e.referencedSyrups()).contains(shared);
        }
    }

    @Test void differentKeyWritersAndClearPagesNeverRenewOnDelete() throws Exception {
        var clock = new ManualClock(1000);
        try (var store = new InMemoryLedgerStore();
             var e = BoxEngine.createNew(BOX, CONFIG, store, 1, clock, 1)) {
            create(e);
            var pool = Executors.newFixedThreadPool(4);
            try {
                List<Future<BoxEngine.NamespaceResult>> writes = new ArrayList<>();
                for (int i = 0; i < 20; i++) {
                    String key = Integer.toString(i);
                    writes.add(pool.submit(() -> call(e, PUT, key, new byte[]{1}, 0)));
                }
                for (var f : writes) { assertThat(f.get(5, TimeUnit.SECONDS).outcome()).isEqualTo("OK"); }
                assertThat(call(e, HEAD, null, null, 0).state().version()).isEqualTo(21);
                var cleared = pool.submit(() -> call(e, CLEAR, null, null, 0));
                var written = pool.submit(() -> call(e, PUT, "z", new byte[]{42}, 0));
                cleared.get(5, TimeUnit.SECONDS); written.get(5, TimeUnit.SECONDS);
                assertThat(call(e, HEAD, null, null, 0).state().version()).isEqualTo(22);
                assertThat(call(e, GET, "z", null, 0).data()).containsExactly((byte)42);
            } finally { pool.shutdownNow(); }
        }
    }

    @Test void backwardTimeUsesDurableHighWaterAndForwardJumpRetiresPermanently() {
        var clock = new ManualClock(1000);
        try (var store = new InMemoryLedgerStore();
             var e = BoxEngine.createNew(BOX, CONFIG, store, 1, clock, 1)) {
            create(e); clock.setTime(900);
            assertThat(call(e, PUT, "a", new byte[]{1}, 0).state().expiresAtMillis()).isEqualTo(1100);
            clock.setTime(1000000);
            assertThat(call(e, LIST, null, null, 0).outcome()).isEqualTo("EXPIRED");
            clock.setTime(1000);
            assertThat(call(e, PUT, "a", new byte[]{2}, 0).outcome()).isEqualTo("EXPIRED");
        }
    }

    @Test void putPausedInPayloadWriteCrossingDeadlineIsRejectedBeforePublishing() {
        var clock = new ManualClock(1000);
        try (var store = new FaultStore();
             var e = BoxEngine.createNew(BOX, CONFIG, store, 1, clock, 1)) {
            create(e); store.targetWal = e.manifestState().walLedgerId();
            store.pauseSyrup = true; store.pause = () -> clock.setTime(1100);
            assertThat(call(e, PUT, "late", new byte[]{42}, 0).outcome()).isEqualTo("EXPIRED");
            assertThat(call(e, HEAD, null, null, 0).state().version()).isEqualTo(1);
            assertThat(e.listCandies("i/", null, 128).entries()).isEmpty();
            assertThat(call(e, CLAIM_EXPIRY, null, null, 1).outcome()).isEqualTo("RETIRED");
            assertThat(call(e, PUT, "late", new byte[]{42}, 0).outcome()).isEqualTo("EXPIRED");
        }
    }

    @Test void ambiguousCreationAndRetirementRetainIdentityAfterRecovery() {
        for (var operation : List.of(CREATE, CLAIM_EXPIRY)) {
            var clock = new ManualClock(1000);
            try (var store = new FaultStore();
                 var e = BoxEngine.createNew(BOX, CONFIG, store, 1, clock, 1)) {
                if (operation == CLAIM_EXPIRY) { create(e); clock.setTime(1100); }
                store.targetWal = e.manifestState().walLedgerId();
                store.durable = true; store.failNext = true;
                assertThatThrownBy(() -> call(e, operation, null, null, 1)).isInstanceOf(StorageException.class);
                try (var recovered = BoxEngine.recover(BOX, CONFIG, store, 2, clock, e.manifestLedgerId(), 2)) {
                    assertThat(call(recovered, CREATE, null, null, 0).outcome()).isEqualTo("EXISTS");
                    assertThat(call(recovered, HEAD, null, null, 0).outcome())
                            .isEqualTo(operation == CLAIM_EXPIRY ? "EXPIRED" : "OK");
                }
            }
        }
    }

    private static void await(CountDownLatch latch) {
        try { if (!latch.await(5, TimeUnit.SECONDS)) { throw new AssertionError("latch timeout"); } }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
    }
    private static final class SimulatedCrash extends Error { }

    /** Fails the next append, optionally after the delegate has made that entry durable. */
    private static final class FaultStore implements LedgerStore {
        private final InMemoryLedgerStore delegate = new InMemoryLedgerStore();
        private boolean failNext;
        private boolean durable;
        private boolean crash;
        private long targetWal = -1;
        private Runnable pause;
        private boolean pauseSyrup;

        @Override
        public WritableLedger createLedger(LedgerConfig config) {
            WritableLedger ledger = delegate.createLedger(config);
            return new WritableLedger() {
                @Override
                public long append(byte[] data) {
                    if (pause != null && (pauseSyrup ? ledger.ledgerId() != targetWal : ledger.ledgerId() == targetWal)) {
                        Runnable action = pause; pause = null; action.run();
                    }
                    if (failNext && ledger.ledgerId() == targetWal) {
                        failNext = false;
                        if (durable) {
                            ledger.append(data);
                        }
                        if (crash) {
                            throw new SimulatedCrash();
                        }
                        throw new StorageException("injected lost append acknowledgement");
                    }
                    return ledger.append(data);
                }
                @Override public long ledgerId() { return ledger.ledgerId(); }
                @Override public long lastAddConfirmed() { return ledger.lastAddConfirmed(); }
                @Override public boolean isSealed() { return ledger.isSealed(); }
                @Override public Map<String, byte[]> customMetadata() { return ledger.customMetadata(); }
                @Override public LedgerEntry read(long id) { return ledger.read(id); }
                @Override public List<LedgerEntry> readRange(long first, long last) {
                    return ledger.readRange(first, last);
                }
                @Override public void close() { ledger.close(); }
            };
        }
        @Override public ReadableLedger openLedger(long id) { return delegate.openLedger(id); }
        @Override public ReadableLedger recoverOpen(long id) { return delegate.recoverOpen(id); }
        @Override public void deleteLedger(long id) { delegate.deleteLedger(id); }
        @Override public List<Long> listLedgers() { return delegate.listLedgers(); }
        @Override public void close() { delegate.close(); }
    }
}
