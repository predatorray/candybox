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

import java.util.List;
import java.util.Set;
import me.predatorray.candybox.common.auth.ObjectAcl;
import java.util.Map;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerEntry;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.config.SizeLimits;
import me.predatorray.candybox.common.exception.LimitExceededException;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.common.exception.StorageException;
import org.junit.jupiter.api.Test;

class BoxEngineRenameRecoveryTest {
    private static final BoxName BOX = BoxName.of("rename-recovery");
    private static final CandyKey SRC = CandyKey.of("src");
    private static final CandyKey DST = CandyKey.of("dst");
    private static final CandyboxConfig CONFIG = CandyboxConfig.defaults();

    @Test
    void failureBeforeDurabilityLeavesOriginalState() {
        checkRecovery(false, false);
    }

    @Test
    void lostAcknowledgementRecoversWholeRename() {
        checkRecovery(true, false);
    }

    @Test
    void oversizedBatchIsRejectedBeforeAnyAppend() {
        SizeLimits defaults = SizeLimits.defaults();
        CandyboxConfig config = CandyboxConfig.builder().sizeLimits(new SizeLimits(
                defaults.chunkSizeBytes(), 2 << 20, defaults.maxUserMetadataBytes(),
                defaults.maxLocatorBytes(), 0)).build();
        try (FaultStore store = new FaultStore();
             BoxEngine owner = BoxEngine.createNew(BOX, config, store, 1, new ManualClock(1000), 1)) {
            owner.putCandy(SRC, new byte[]{1}, null, Map.of(), null);
            long walId = owner.manifestState().walLedgerId();
            long tail = store.openLedger(walId).lastAddConfirmed();
            CandyKey huge = CandyKey.of("x".repeat(1 << 20));
            assertThatThrownBy(() -> owner.renameCandy(SRC, huge, null))
                    .isInstanceOf(LimitExceededException.class);
            assertThat(store.openLedger(walId).lastAddConfirmed()).isEqualTo(tail);
            assertThat(owner.getCandy(SRC)).containsExactly((byte) 1);
            assertThatThrownBy(() -> owner.getCandy(huge)).isInstanceOf(CandyNotFoundException.class);
        }
    }

    @Test
    void recoveryPreservesMultipartAclRangeDeletesAndGcReferences() {
        CandyboxConfig config = CandyboxConfig.builder().multipartMinPartBytes(1).build();
        ObjectAcl acl = new ObjectAcl("owner", List.of());
        try (FaultStore store = new FaultStore();
             BoxEngine owner = BoxEngine.createNew(BOX, config, store, 1, new ManualClock(1000), 1)) {
            String upload = owner.createMultipartUpload(SRC, "text/plain", Map.of("k", "v"));
            BoxEngine.PartUploadResult first = owner.uploadPart(upload, 1, new byte[]{1});
            BoxEngine.PartUploadResult second = owner.uploadPart(upload, 2, new byte[]{2});
            owner.completeMultipartUpload(upload, List.of(new BoxEngine.PartCompletion(1, first.crc32c()),
                    new BoxEngine.PartCompletion(2, second.crc32c())), null, acl);
            var source = owner.resolveLocator(SRC);
            owner.deleteRange(DST, CandyKey.of("dsu"));
            long before = owner.manifestState().walLedgerId();
            long tail = store.openLedger(before).lastAddConfirmed();
            owner.renameCandy(SRC, DST, null);
            assertThat(store.openLedger(before).lastAddConfirmed()).isEqualTo(tail + 1);
            try (BoxEngine recovered = BoxEngine.recover(BOX, config, store, 2,
                    new ManualClock(1000), owner.manifestLedgerId(), 2)) {
                assertThat(recovered.getCandy(DST)).containsExactly((byte) 1, (byte) 2);
                assertThat(recovered.resolveLocator(DST).parts()).isEqualTo(source.parts());
                assertThat(recovered.getCandyAcl(DST)).isEqualTo(acl);
                assertThatThrownBy(() -> recovered.getCandy(SRC)).isInstanceOf(CandyNotFoundException.class);
                var syrupIds = source.segments().stream().map(seg -> seg.syrupId()).toList();
                assertThat(recovered.heldSyrups(Long.MAX_VALUE)).containsAll(syrupIds);
                assertThat(recovered.claimReclaimableSyrups(Long.MAX_VALUE, Set.of()))
                        .doesNotContainAnyElementsOf(syrupIds);
                // A new source write must outrank the later timestamp of the batch's DELETE.
                recovered.putCandy(SRC, new byte[]{3}, null, Map.of(), null);
                assertThat(recovered.getCandy(SRC)).containsExactly((byte) 3);
                recovered.flush();
                assertThat(recovered.getCandy(DST)).containsExactly((byte) 1, (byte) 2);
            }
        }
    }

    @Test
    void crashAfterDurabilityBeforeMemtableApplyRecoversWholeRename() {
        checkRecovery(true, true);
    }

    private void checkRecovery(boolean durable, boolean crash) {
        try (FaultStore store = new FaultStore();
             BoxEngine owner = BoxEngine.createNew(BOX, CONFIG, store, 1, new ManualClock(1000), 1)) {
            owner.putCandy(SRC, new byte[]{1}, null, Map.of(), null);
            owner.putCandy(DST, new byte[]{2}, null, Map.of(), null);
            store.durable = durable;
            store.crash = crash;
            store.failNext = true;
            assertThatThrownBy(() -> owner.renameCandy(SRC, DST, null))
                    .isInstanceOf(crash ? SimulatedCrash.class : StorageException.class);
            // Neither mutation was applied to the old owner's memtable.
            assertThat(owner.getCandy(SRC)).containsExactly((byte) 1);
            assertThat(owner.getCandy(DST)).containsExactly((byte) 2);
            try (BoxEngine recovered = BoxEngine.recover(BOX, CONFIG, store, 2,
                    new ManualClock(1000), owner.manifestLedgerId(), 2)) {
                if (durable) {
                    assertThatThrownBy(() -> recovered.getCandy(SRC))
                            .isInstanceOf(CandyNotFoundException.class);
                    assertThat(recovered.getCandy(DST)).containsExactly((byte) 1);
                } else {
                    assertThat(recovered.getCandy(SRC)).containsExactly((byte) 1);
                    assertThat(recovered.getCandy(DST)).containsExactly((byte) 2);
                }
            }
        }
    }

    private static final class SimulatedCrash extends Error { }

    /** Fails the next append, optionally after the delegate has made that entry durable. */
    private static final class FaultStore implements LedgerStore {
        private final InMemoryLedgerStore delegate = new InMemoryLedgerStore();
        private boolean failNext;
        private boolean durable;
        private boolean crash;

        @Override
        public WritableLedger createLedger(LedgerConfig config) {
            WritableLedger ledger = delegate.createLedger(config);
            return new WritableLedger() {
                @Override
                public long append(byte[] data) {
                    if (failNext) {
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
