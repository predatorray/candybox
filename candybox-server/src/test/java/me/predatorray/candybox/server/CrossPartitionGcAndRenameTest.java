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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.Partitioning;
import me.predatorray.candybox.common.auth.ObjectAcl;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.Lease;
import me.predatorray.candybox.coordination.LeaseInfo;
import me.predatorray.candybox.coordination.VersionedValue;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.lsm.manifest.RenameIntent;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.RequestHandler;
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
    private static final MessageCodec CODEC = new MessageCodec();

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
    void pendingRelayKeepsItsSyrupAliveAfterTheSourceIsDeleted() {
        assertDelayedZeroCopyPutStaysReadable(1, false);
    }

    @Test
    void pendingRelayKeepsItsSyrupAliveForASamePartitionDestination() {
        assertDelayedZeroCopyPutStaysReadable(0, false);
    }

    @Test
    void pendingRenameRelayKeepsItsSyrupAliveAfterTheSourceIsDeleted() {
        assertDelayedZeroCopyPutStaysReadable(1, true);
    }

    /**
     * Resolves a relay, then deletes, compacts and GCs the source so its Syrup would be reclaimable,
     * and only then consumes the relay: the relay must have pinned the Syrup.
     */
    private static void assertDelayedZeroCopyPutStaysReadable(int dstPartition, boolean rename) {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        try (CandyboxNode node = new CandyboxNode(1, aggressiveGcConfig(), store,
                new InMemoryCoordinationService(), new ManualClock(1000))) {
            node.createBox(BOX, PARTITIONS);
            String src = keyIn(0, "src");
            String dst = keyIn(dstPartition, "dst");
            long syrup = putAndFlush(node, src);
            RequestHandler handler = node.requestHandler();
            String renameToken = rename ? "late-rename" : null;
            String token = rename
                    ? ((Message.CandyLocatorResponse) send(handler, new Message.PrepareRenameRequest(
                            BOX.value(), src, dst, dstPartition, renameToken))).relayToken()
                    : relayToken(handler, src);

            orphanSource(node, src);
            node.collectGarbageOnce();
            assertThat(store.listLedgers()).contains(syrup);

            assertThat(send(handler, new Message.ZeroCopyPutRequest(BOX.value(), dst, 0, token,
                    null, List.of(), renameToken))).isInstanceOf(Message.HeadCandyResponse.class);
            node.collectGarbageOnce();
            assertThat(node.enginePartition(BOX, dstPartition).getCandy(CandyKey.of(dst)))
                    .isEqualTo(bytes("payload"));
        }
        store.close();
    }

    @Test
    void reapedRelayNoLongerPinsItsSyrup() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        ManualClock clock = new ManualClock(1000);
        try (CandyboxNode node = new CandyboxNode(1, aggressiveGcConfig(), store,
                new InMemoryCoordinationService(), clock)) {
            node.createBox(BOX, PARTITIONS);
            String src = keyIn(0, "src");
            long syrup = putAndFlush(node, src);
            relayToken(node.requestHandler(), src);
            orphanSource(node, src);
            node.collectGarbageOnce();
            assertThat(store.listLedgers()).contains(syrup);

            clock.advance(aggressiveGcConfig().renameIntentAbandonMillis() + 1);
            assertThat(node.sweepStaleLocatorRelaysOnce()).isEqualTo(1);
            node.collectGarbageOnce();
            assertThat(store.listLedgers()).doesNotContain(syrup);
        }
        store.close();
    }

    @Test
    void boxSitsOutGcWhenItsRelaysCannotBeRead() {
        InMemoryLedgerStore store = new InMemoryLedgerStore();
        FailingChildren coordination = new FailingChildren(new InMemoryCoordinationService());
        try (CandyboxNode node = new CandyboxNode(1, aggressiveGcConfig(), store, coordination,
                new ManualClock(1000))) {
            node.createBox(BOX, PARTITIONS);
            String src = keyIn(0, "src");
            long syrup = putAndFlush(node, src);
            orphanSource(node, src);

            coordination.failChildren = true;
            assertThat(node.collectGarbageOnce()).isZero();
            assertThat(store.listLedgers()).contains(syrup);

            coordination.failChildren = false;
            node.collectGarbageOnce();
            assertThat(store.listLedgers()).doesNotContain(syrup);
        }
        store.close();
    }

    /** Puts and flushes {@code key} in partition 0, returning the Syrup that holds its bytes. */
    private static long putAndFlush(CandyboxNode node, String key) {
        BoxEngine p0 = node.enginePartition(BOX, 0);
        p0.putCandy(CandyKey.of(key), bytes("payload"), "text/plain", Map.of(), null);
        p0.flush();
        return p0.resolveLocator(CandyKey.of(key)).parts().get(0).segments().get(0).syrupId();
    }

    /** Deletes {@code key} from partition 0 and compacts until its Syrup is a pending orphan. */
    private static void orphanSource(CandyboxNode node, String key) {
        BoxEngine p0 = node.enginePartition(BOX, 0);
        p0.deleteCandy(CandyKey.of(key));
        p0.putCandy(CandyKey.of(keyIn(0, "other")), bytes("x"), null, Map.of(), null); // rolls S
        p0.flush();
        for (int i = 0; i < 5; i++) {
            node.compactOwnedBoxesOnce();
        }
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

    @Test
    void unconsumedLocatorRelaysAreReapedPastTheWindow() {
        ManualClock clock = new ManualClock(1_000_000);
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        try (CandyboxNode node = new CandyboxNode(1, CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0).renameIntentAbandonMillis(1_000).build(),
                new InMemoryLedgerStore(), coordination, clock)) {
            node.createBox(BOX, PARTITIONS);
            String src = keyIn(0, "src");
            node.enginePartition(BOX, 0).putCandy(CandyKey.of(src), bytes("payload"), null,
                    Map.of(), null);
            RequestHandler handler = node.requestHandler();
            String stale = relayToken(handler, src);
            clock.advance(600);
            String fresh = relayToken(handler, src);
            String relays = CandyboxKeys.locatorRelaysPath(BOX.value(), 0);
            assertThat(coordination.children(relays)).containsExactlyInAnyOrder(stale, fresh);

            clock.advance(600);
            assertThat(node.sweepStaleLocatorRelaysOnce()).isEqualTo(1);
            assertThat(coordination.children(relays)).containsExactly(fresh);

            // Past half the window a relay is no longer consumable, even before the sweep reaps it.
            assertThat(send(handler, new Message.ZeroCopyPutRequest(BOX.value(), keyIn(1, "dst"),
                    0, fresh, null, List.of(), null))).isInstanceOf(Message.ErrorResponse.class);

            // Consuming a relay deletes it.
            String current = relayToken(handler, src);
            assertThat(send(handler, new Message.ZeroCopyPutRequest(BOX.value(), keyIn(1, "dst"),
                    0, current, null, List.of(), null)))
                    .isInstanceOf(Message.HeadCandyResponse.class);
            assertThat(coordination.children(relays)).containsExactly(fresh);
        }
    }

    @Test
    void deletingABoxDropsItsLocatorRelays() {
        InMemoryCoordinationService coordination = new InMemoryCoordinationService();
        try (CandyboxNode node = new CandyboxNode(1, CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0).build(), new InMemoryLedgerStore(), coordination,
                new ManualClock(1000))) {
            node.createBox(BOX, PARTITIONS);
            String src = keyIn(2, "src");
            node.enginePartition(BOX, 2).putCandy(CandyKey.of(src), bytes("payload"), null,
                    Map.of(), null);
            relayToken(node.requestHandler(), src);
            node.enginePartition(BOX, 2).deleteCandy(CandyKey.of(src));

            node.deleteBox(BOX, false);

            assertThat(coordination.children(CandyboxKeys.locatorRelaysPath(BOX.value(), 2)))
                    .isEmpty();
        }
    }

    @Test
    void anonymousCrossPartitionRenameRunsThroughTheHandler() {
        try (CandyboxNode node = new CandyboxNode(1, CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0).build(), new InMemoryLedgerStore(),
                new InMemoryCoordinationService(), new ManualClock(1000))) {
            node.createBox(BOX, PARTITIONS);
            String src = keyIn(0, "src");
            String dst = keyIn(1, "dst");
            node.enginePartition(BOX, 0).putCandy(CandyKey.of(src), bytes("payload"), "text/plain",
                    Map.of("m", "v"), null);
            RequestHandler handler = node.requestHandler();

            Message.CandyLocatorResponse relay = (Message.CandyLocatorResponse) send(handler,
                    new Message.PrepareRenameRequest(BOX.value(), src, dst, 1, "anon-rename"));
            Message.HeadCandyResponse head = (Message.HeadCandyResponse) send(handler,
                    new Message.ZeroCopyPutRequest(BOX.value(), dst, 0, relay.relayToken(), null,
                            List.of(), "anon-rename"));
            assertThat(head.contentType()).isEqualTo("text/plain");
            assertThat(head.userMetadata()).containsEntry("m", "v");
            assertThat(send(handler, new Message.CompleteRenameRequest(BOX.value(), src, 0,
                    "anon-rename", relay.hlc()))).isInstanceOf(Message.OkResponse.class);

            assertThat(node.enginePartition(BOX, 1).getCandy(CandyKey.of(dst)))
                    .isEqualTo(bytes("payload"));
            assertThatThrownBy(() -> node.enginePartition(BOX, 0).getCandy(CandyKey.of(src)))
                    .isInstanceOf(CandyNotFoundException.class);
        }
    }

    @Test
    void pendingRelaysAreCappedPerPartitionAndStaleOnesReapedOnWrite() {
        ManualClock clock = new ManualClock(1_000_000);
        try (CandyboxNode node = new CandyboxNode(1, CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0).renameIntentAbandonMillis(1_000).build(),
                new InMemoryLedgerStore(), new InMemoryCoordinationService(clock), clock)) {
            node.createBox(BOX, PARTITIONS);
            String src = keyIn(0, "src");
            node.enginePartition(BOX, 0).putCandy(CandyKey.of(src), bytes("payload"), null,
                    Map.of(), null);
            RequestHandler handler = node.requestHandler();
            for (int i = 0; i < CandyboxNode.MAX_PENDING_LOCATOR_RELAYS; i++) {
                relayToken(handler, src);
            }
            assertThat(send(handler, new Message.GetCandyLocatorRequest(BOX.value(), src)))
                    .isInstanceOf(Message.BusyResponse.class);

            clock.advance(1_001);
            assertThat(send(handler, new Message.GetCandyLocatorRequest(BOX.value(), src)))
                    .isInstanceOf(Message.CandyLocatorResponse.class);
        }
    }

    @Test
    void pendingRelayBytesAreBudgetedPerNodeAcrossBoxes() {
        ManualClock clock = new ManualClock(1_000_000);
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        BoxName other = BoxName.of("xp-other");
        try (CandyboxNode node = new CandyboxNode(1, CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0).renameIntentAbandonMillis(4_000)
                .zeroCopyRelayBudgetBytes(3 * relaySize(clock) + 8).build(),
                new InMemoryLedgerStore(), coordination, clock)) {
            node.createBox(BOX, PARTITIONS);
            node.createBox(other, PARTITIONS);
            String src = keyIn(0, "src");
            node.enginePartition(BOX, 0).putCandy(CandyKey.of(src), bytes("payload"), null,
                    Map.of(), null);
            node.enginePartition(other, 0).putCandy(CandyKey.of(src), bytes("payload"), null,
                    Map.of(), null);
            RequestHandler handler = node.requestHandler();
            Message locate = new Message.GetCandyLocatorRequest(other.value(), src);
            relayToken(handler, src);
            String consumable = relayToken(handler, src);
            assertThat(send(handler, locate)).isInstanceOf(Message.CandyLocatorResponse.class);
            assertThat(send(handler, locate)).isInstanceOf(Message.BusyResponse.class);

            // A consumed relay frees its bytes at the next recount, which is rate-limited.
            send(handler, new Message.ZeroCopyPutRequest(BOX.value(), keyIn(1, "dst"), 0,
                    consumable, null, List.of(), null));
            assertThat(send(handler, locate)).isInstanceOf(Message.BusyResponse.class);
            clock.advance(CandyboxNode.RELAY_RECOUNT_INTERVAL_MILLIS);
            assertThat(send(handler, locate)).isInstanceOf(Message.CandyLocatorResponse.class);

            // And stale ones are reaped by that recount.
            clock.advance(3_001);
            assertThat(send(handler, locate)).isInstanceOf(Message.CandyLocatorResponse.class);
        }
    }

    /** The serialized size of a relay for a small single-part Candy. */
    private static long relaySize(ManualClock clock) {
        InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
        try (CandyboxNode node = new CandyboxNode(9, CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0).build(), new InMemoryLedgerStore(), coordination,
                clock)) {
            node.createBox(BOX, PARTITIONS);
            String src = keyIn(0, "src");
            node.enginePartition(BOX, 0).putCandy(CandyKey.of(src), bytes("payload"), null,
                    Map.of(), null);
            String token = relayToken(node.requestHandler(), src);
            return coordination.get(CandyboxKeys.locatorRelayKey(BOX.value(), 0, token))
                    .orElseThrow().value().length;
        }
    }

    @Test
    void relaySweepSurvivesACoordinationError() {
        FailingChildren coordination = new FailingChildren(new InMemoryCoordinationService());
        try (CandyboxNode node = new CandyboxNode(1, CandyboxConfig.builder()
                .leaseRenewIntervalMillis(0).build(), new InMemoryLedgerStore(), coordination,
                new ManualClock(1000))) {
            node.createBox(BOX, PARTITIONS);
            coordination.failChildren = true;
            assertThat(node.sweepStaleLocatorRelaysOnce()).isZero();
        }
    }

    /** Delegates to a real fake, but {@link #children} fails on demand (a ZooKeeper outage). */
    private static final class FailingChildren implements CoordinationService {
        private final CoordinationService delegate;
        volatile boolean failChildren;

        FailingChildren(CoordinationService delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<String> children(String path) {
            if (failChildren) {
                throw new IllegalStateException("connection loss");
            }
            return delegate.children(path);
        }

        @Override
        public Optional<VersionedValue> get(String key) {
            return delegate.get(key);
        }

        @Override
        public long create(String key, byte[] value) {
            return delegate.create(key, value);
        }

        @Override
        public long compareAndSet(String key, byte[] value, long expectedVersion) {
            return delegate.compareAndSet(key, value, expectedVersion);
        }

        @Override
        public void delete(String key, long expectedVersion) {
            delegate.delete(key, expectedVersion);
        }

        @Override
        public Optional<Lease> tryAcquireLease(String resource, int nodeId, long ttlMillis) {
            return delegate.tryAcquireLease(resource, nodeId, ttlMillis);
        }

        @Override
        public Optional<LeaseInfo> leaseHolder(String resource) {
            return delegate.leaseHolder(resource);
        }

        @Override
        public void registerMember(int nodeId, byte[] info) {
            delegate.registerMember(nodeId, info);
        }

        @Override
        public void unregisterMember(int nodeId) {
            delegate.unregisterMember(nodeId);
        }

        @Override
        public List<Integer> members() {
            return delegate.members();
        }

        @Override
        public Optional<byte[]> memberInfo(int nodeId) {
            return delegate.memberInfo(nodeId);
        }

        @Override
        public boolean isConnected() {
            return delegate.isConnected();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static Message send(RequestHandler handler, Message request) {
        return CODEC.decode(handler.handle(CODEC.encode(request)));
    }

    private static String relayToken(RequestHandler handler, String key) {
        return ((Message.CandyLocatorResponse) send(handler,
                new Message.GetCandyLocatorRequest(BOX.value(), key))).relayToken();
    }
}
