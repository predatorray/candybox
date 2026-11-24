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

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerMetadata;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.bookkeeper.bk.BookKeeperLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.Partitioning;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.zk.ZooKeeperCoordinationService;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.server.CandyboxNode;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

/** Durable relay claim across a paused consumer and expiry, using real BookKeeper and ZooKeeper. */
class LocatorRelayTransferIT {

    @Test
    void pausedConsumerRemainsProtectedPastExpiryOnRealBackends() throws Exception {
        ManualClock clock = new ManualClock(1_000_000);
        CandyboxConfig config = CandyboxConfig.builder().memtableFlushThresholdBytes(1)
                .syrupRolloverBytes(1).l0CompactionTrigger(2).ledgerGcGraceMillis(100)
                .renameIntentAbandonMillis(1_000).ownershipLeaseTtlMillis(3_600_000)
                .leaseRenewIntervalMillis(0).build();
        BoxName box = BoxName.of("transfer-box");
        String src = keyIn(0);
        String dst = keyIn(1);
        var executor = Executors.newSingleThreadExecutor();
        CountDownLatch beforeWal = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        try (EmbeddedBookKeeper bookKeeper = new EmbeddedBookKeeper(3);
             TestingServer zk = new TestingServer(true);
             BookKeeperLedgerStore store = new BookKeeperLedgerStore(
                     bookKeeper.clientConfiguration(), bytes("candybox"));
             ZooKeeperCoordinationService coordination = new ZooKeeperCoordinationService(
                     zk.getConnectString(), clock);
             CandyboxNode source = new CandyboxNode(1, config, store, coordination, clock);
             CandyboxNode destination = new CandyboxNode(2, config,
                     new PausedImportStore(store, beforeWal, resume), coordination, clock)) {
            source.createBox(box, 2);
            source.releasePartition(box, 1);
            destination.openPartition(box, 1);
            var engine = source.enginePartition(box, 0);
            engine.putCandy(CandyKey.of(src), bytes("precious"), "text/plain", Map.of(), null);
            engine.flush();
            long syrup = engine.resolveLocator(CandyKey.of(src)).segments().get(0).syrupId();
            Message.CandyLocatorResponse relay = (Message.CandyLocatorResponse) send(source,
                    new Message.GetCandyLocatorRequest(box.value(), src));
            var put = executor.submit(() -> send(destination, new Message.ZeroCopyPutRequest(
                    box.value(), dst, 0, relay.relayToken(), null, List.of(), null)));
            assertThat(beforeWal.await(30, TimeUnit.SECONDS)).isTrue();
            try {
                engine.deleteCandy(CandyKey.of(src));
                engine.flush();
                for (int i = 0; i < 5; i++) { source.compactOwnedBoxesOnce(); }
                clock.advance(2_000);
                source.sweepStaleLocatorRelaysOnce();
                source.collectGarbageOnce();
                assertThat(store.listLedgers()).contains(syrup);
                assertThat(coordination.get(CandyboxKeys.locatorRelayKey(
                        box.value(), 0, relay.relayToken()))).isPresent();
            } finally {
                resume.countDown();
            }
            assertThat(put.get(30, TimeUnit.SECONDS)).isInstanceOf(Message.HeadCandyResponse.class);
            assertThat(destination.enginePartition(box, 1).getCandy(CandyKey.of(dst)))
                    .isEqualTo(bytes("precious"));
            assertThat(coordination.get(CandyboxKeys.locatorRelayKey(
                    box.value(), 0, relay.relayToken()))).isEmpty();
        } finally {
            resume.countDown();
            executor.shutdownNow();
        }
    }

    private static String keyIn(int partition) {
        for (int i = 0; ; i++) {
            String key = "key-" + i;
            if (Partitioning.partitionOf(key, 2) == partition) { return key; }
        }
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }

    private static Message send(CandyboxNode node, Message request) {
        MessageCodec codec = new MessageCodec();
        return codec.decode(node.requestHandler().handle(codec.encode(request)));
    }

    private record PausedImportStore(LedgerStore delegate, CountDownLatch beforeWal,
                                     CountDownLatch resume) implements LedgerStore {
        @Override public void retainLedgers(Collection<Long> ids) {
            beforeWal.countDown();
            try {
                if (!resume.await(30, TimeUnit.SECONDS)) { throw new AssertionError("Transfer was not resumed"); }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(interrupted);
            }
            delegate.retainLedgers(ids);
        }
        @Override public WritableLedger createLedger(LedgerConfig config) { return delegate.createLedger(config); }
        @Override public ReadableLedger openLedger(long id) { return delegate.openLedger(id); }
        @Override public ReadableLedger recoverOpen(long id) { return delegate.recoverOpen(id); }
        @Override public void deleteLedger(long id) { delegate.deleteLedger(id); }
        @Override public List<Long> listLedgers() { return delegate.listLedgers(); }
        @Override public LedgerMetadata readMetadata(long id) { return delegate.readMetadata(id); }
        @Override public void close() { }
    }
}
