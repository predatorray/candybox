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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerEntry;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.common.exception.StorageException;
import java.util.Random;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.config.SizeLimits;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.lsm.engine.ListResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Phase 1 end-to-end engine tests against real BookKeeper ledgers: put/get/delete/list, large-object
 * chunking across Syrups, and recovery of un-flushed writes from the WAL.
 */
class Phase1EngineIT {

    private static EmbeddedBookKeeper bookKeeper;
    private BookKeeperLedgerStore store;

    @BeforeAll
    static void startCluster() {
        bookKeeper = new EmbeddedBookKeeper(3);
    }

    @AfterAll
    static void stopCluster() {
        if (bookKeeper != null) {
            bookKeeper.close();
        }
    }

    @BeforeEach
    void openStore() {
        store = new BookKeeperLedgerStore(bookKeeper.clientConfiguration(),
                "candybox".getBytes(StandardCharsets.UTF_8));
    }

    @AfterEach
    void closeStore() {
        store.close();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void retainedRenameReceiptsStayCheckpointableAndDrainOnRealLedgers() {
        BoxName box = BoxName.of("receipt-budget");
        var config = CandyboxConfig.defaults();
        try (BoxEngine owner = BoxEngine.createNew(box, config, store, 1, SystemClock.INSTANCE, 1)) {
            CandyKey src = CandyKey.of("src");
            owner.putCandy(src, bytes("A"), null, Map.of(), null);
            var small = owner.resolveLocator(src);
            var large = new me.predatorray.candybox.common.CandyLocator(small.hlc(), small.type(),
                    small.contentType(), small.userMetadata(), small.createdAtMillis(),
                    java.util.Collections.nCopies(10_000, small.parts().get(0)), small.acl());
            int accepted = 0;
            for (; accepted < 64; accepted++) {
                try {
                    owner.zeroCopyRename(CandyKey.of("dst-" + accepted), large, "token-" + accepted,
                            "epoch", "relay-" + accepted, 0, "src");
                } catch (me.predatorray.candybox.common.exception.BusyException full) { break; }
            }
            assertThat(accepted).isBetween(1, 63);
            owner.flush();
            try (BoxEngine recovered = BoxEngine.recover(box, config, store, 2,
                    SystemClock.INSTANCE, owner.manifestLedgerId(), 2)) {
                assertThat(recovered.listRenameReceipts()).hasSize(accepted);
                for (var receipt : recovered.listRenameReceipts()) { recovered.clearRenameReceipt(receipt.token()); }
                recovered.zeroCopyRename(CandyKey.of("after"), large, "after", "epoch", "relay", 0, "src");
                recovered.flush();
                assertThat(recovered.getCandy(CandyKey.of("after"))).hasSize(10_000);
            }
        }
    }

    @Test
    void putGetDeleteListOnRealLedgers() {
        BoxName box = BoxName.of("it-box");
        try (BoxEngine engine = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1,
                SystemClock.INSTANCE, 1L)) {
            engine.putCandy(CandyKey.of("a/1"), bytes("one"), "text/plain", Map.of("k", "v"), null);
            engine.putCandy(CandyKey.of("a/2"), bytes("two"), null, Map.of(), null);
            engine.putCandy(CandyKey.of("b/1"), bytes("three"), null, Map.of(), null);
            engine.flush(); // push to a real L0 SSTable ledger

            assertThat(engine.getCandy(CandyKey.of("a/1"))).isEqualTo(bytes("one"));
            assertThat(engine.headCandy(CandyKey.of("a/1")).userMetadata()).containsEntry("k", "v");

            ListResult listing = engine.listCandies("a/", null, 10);
            assertThat(listing.entries()).extracting(e -> e.key().value()).containsExactly("a/1", "a/2");

            engine.deleteCandy(CandyKey.of("a/1"));
            assertThatThrownBy(() -> engine.getCandy(CandyKey.of("a/1")))
                    .isInstanceOf(CandyNotFoundException.class);
            assertThat(engine.getCandy(CandyKey.of("a/2"))).isEqualTo(bytes("two"));
        }
    }

    @Test
    void largeObjectIsChunkedAcrossSyrupsAndReassembled() {
        SizeLimits limits = new SizeLimits(64 * 1024, SizeLimits.DEFAULT_MAX_KEY_BYTES,
                SizeLimits.DEFAULT_MAX_METADATA_BYTES, SizeLimits.DEFAULT_MAX_LOCATOR_BYTES, 0);
        CandyboxConfig config = CandyboxConfig.builder()
                .sizeLimits(limits)
                .syrupRolloverBytes(256 * 1024) // force several Syrups for a 1 MiB object
                .build();

        byte[] big = new byte[1024 * 1024];
        new Random(7).nextBytes(big);

        BoxName box = BoxName.of("big-box");
        try (BoxEngine engine = BoxEngine.createNew(box, config, store, 1, SystemClock.INSTANCE, 1L)) {
            engine.putCandy(CandyKey.of("large"), big, "application/octet-stream", Map.of(), null);
            assertThat(engine.getCandy(CandyKey.of("large"))).isEqualTo(big);

            engine.flush();
            assertThat(engine.getCandy(CandyKey.of("large"))).isEqualTo(big);
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void renameWithLostWalAcknowledgementRecoversAtomicallyOnRealLedgers(boolean crossPartition) {
        AtomicBoolean loseNextAck = new AtomicBoolean();
        // The decorator loses the response after the real BookKeeper quorum acknowledged the entry.
        LedgerStore faultStore = new LedgerStore() {
            @Override
            public WritableLedger createLedger(LedgerConfig config) {
                WritableLedger ledger = store.createLedger(config);
                return new WritableLedger() {
                    @Override public long append(byte[] data) {
                        long id = ledger.append(data);
                        if (loseNextAck.getAndSet(false)) {
                            throw new StorageException("injected lost acknowledgement");
                        }
                        return id;
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
            @Override public ReadableLedger openLedger(long id) { return store.openLedger(id); }
            @Override public ReadableLedger recoverOpen(long id) { return store.recoverOpen(id); }
            @Override public void deleteLedger(long id) { store.deleteLedger(id); }
            @Override public List<Long> listLedgers() { return store.listLedgers(); }
            @Override public void close() { }
        };
        BoxName box = BoxName.of("rename-crash-box");
        CandyKey src = CandyKey.of("src");
        CandyKey dst = CandyKey.of("dst");
        CandyboxConfig config = CandyboxConfig.defaults();
        try (BoxEngine owner = BoxEngine.createNew(box, config, faultStore, 1,
                SystemClock.INSTANCE, 1)) {
            owner.putCandy(src, bytes("A"), null, Map.of(), null);
            owner.putCandy(dst, bytes("B"), null, Map.of(), null);
            var source = owner.resolveLocator(src);
            loseNextAck.set(true);
            assertThatThrownBy(() -> {
                if (crossPartition) {
                    owner.zeroCopyRename(dst, source, "transfer", "incarnation", "relay", 0, src.value());
                } else {
                    owner.renameCandy(src, dst, null);
                }
            }).isInstanceOf(StorageException.class);
            assertThat(owner.getCandy(src)).isEqualTo(bytes("A"));
            assertThat(owner.getCandy(dst)).isEqualTo(bytes("B"));
            // Recover-open fences the still-open original handles; no clean shutdown/flush first.
            try (BoxEngine recovered = BoxEngine.recover(box, config, store, 2,
                    SystemClock.INSTANCE, owner.manifestLedgerId(), 2)) {
                if (crossPartition) {
                    assertThat(recovered.getCandy(src)).isEqualTo(bytes("A"));
                    assertThat(recovered.listRenameReceipts()).hasSize(1);
                    recovered.flush();
                    try (BoxEngine next = BoxEngine.recover(box, config, store, 3,
                            SystemClock.INSTANCE, recovered.manifestLedgerId(), 3)) {
                        assertThat(next.listRenameReceipts()).hasSize(1);
                        assertThat(next.getCandy(dst)).isEqualTo(bytes("A"));
                        next.clearRenameReceipt("transfer");
                        try (BoxEngine cleaned = BoxEngine.recover(box, config, store, 4,
                                SystemClock.INSTANCE, next.manifestLedgerId(), 4)) {
                            assertThat(cleaned.listRenameReceipts()).isEmpty();
                            assertThat(cleaned.getCandy(dst)).isEqualTo(bytes("A"));
                        }
                    }
                    return;
                }
                assertThatThrownBy(() -> recovered.getCandy(src)).isInstanceOf(CandyNotFoundException.class);
                assertThat(recovered.getCandy(dst)).isEqualTo(bytes("A"));
                recovered.putCandy(src, bytes("new"), null, Map.of(), null);
                assertThat(recovered.getCandy(src)).isEqualTo(bytes("new"));
            }
        }
    }

    @Test
    void unflushedWritesAreRecoveredFromTheWal() {
        BoxName box = BoxName.of("recover-box");
        long manifestLedgerId;
        BoxEngine ownerA = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1,
                SystemClock.INSTANCE, 1L);
        ownerA.putCandy(CandyKey.of("k1"), bytes("v1"), null, Map.of(), null);
        ownerA.putCandy(CandyKey.of("k2"), bytes("v2"), null, Map.of(), null);
        // Deliberately do NOT flush or close: simulate a crash with data only in the WAL.
        manifestLedgerId = ownerA.manifestLedgerId();

        try (BoxEngine ownerB = BoxEngine.recover(box, CandyboxConfig.defaults(), store, 2,
                SystemClock.INSTANCE, manifestLedgerId, 2L)) {
            assertThat(ownerB.getCandy(CandyKey.of("k1"))).isEqualTo(bytes("v1"));
            assertThat(ownerB.getCandy(CandyKey.of("k2"))).isEqualTo(bytes("v2"));
        }
    }

    @Test
    void unflushedWritesSurviveTwoHandoversWithoutAFlush() {
        BoxName box = BoxName.of("double-recover-box");
        BoxEngine ownerA = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1,
                SystemClock.INSTANCE, 1L);
        ownerA.putCandy(CandyKey.of("k"), bytes("v1"), null, Map.of(), null);

        // B takes over and "crashes" without writing or flushing.
        BoxEngine ownerB = BoxEngine.recover(box, CandyboxConfig.defaults(), store, 2,
                SystemClock.INSTANCE, ownerA.manifestLedgerId(), 2L);
        try (BoxEngine ownerC = BoxEngine.recover(box, CandyboxConfig.defaults(), store, 3,
                SystemClock.INSTANCE, ownerB.manifestLedgerId(), 3L)) {
            assertThat(ownerC.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v1"));
        }
    }

    @Test
    void flushedWritesBoundTheHlcOfABehindRecoveringOwner() {
        BoxName box = BoxName.of("flushed-hlc-box");
        BoxEngine ownerA = BoxEngine.createNew(box, CandyboxConfig.defaults(), store, 1,
                SystemClock.INSTANCE, 1L);
        ownerA.putCandy(CandyKey.of("k"), bytes("v1"), null, Map.of(), null);
        ownerA.flush(); // leaves an empty WAL: only the manifest high-water mark bounds B's clock
        long manifestLedgerId = ownerA.manifestLedgerId();

        ManualClock behind = new ManualClock(SystemClock.INSTANCE.currentTimeMillis() - 2_000);
        try (BoxEngine ownerB = BoxEngine.recover(box, CandyboxConfig.defaults(), store, 2, behind,
                manifestLedgerId, 2L)) {
            ownerB.putCandy(CandyKey.of("k"), bytes("v2"), null, Map.of(), null);
            assertThat(ownerB.getCandy(CandyKey.of("k"))).isEqualTo(bytes("v2"));
        }
    }
}
