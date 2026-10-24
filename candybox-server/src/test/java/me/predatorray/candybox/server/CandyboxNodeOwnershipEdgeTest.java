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
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.Lease;
import me.predatorray.candybox.coordination.LeaseInfo;
import me.predatorray.candybox.coordination.VersionedValue;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Multi-node ownership edges of {@link CandyboxNode}: takeover on delete, stale views, TTL knob. */
class CandyboxNodeOwnershipEdgeTest {

    private static final long TTL = 10_000;
    private static final BoxName X = BoxName.of("box-x");
    private static final BoxName Y = BoxName.of("box-y");
    private static final BoxName Z = BoxName.of("box-z");
    private static final BoxName D = BoxName.of("box-d");

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private final ManualClock clock = new ManualClock(1_000);
    private final InMemoryCoordinationService coordination = new InMemoryCoordinationService(clock);
    private final StaleViewCoordination staleCoordination = new StaleViewCoordination(coordination);
    private final CandyboxConfig config = baseConfig().build();
    private final CandyboxNode nodeA = new CandyboxNode(1, config, store, staleCoordination, clock);
    private final CandyboxNode nodeB = new CandyboxNode(2, config, store, coordination, clock);

    private static CandyboxConfig.Builder baseConfig() {
        return CandyboxConfig.builder()
                .l0CompactionTrigger(2)
                .ownershipLeaseTtlMillis(TTL)
                .leaseRenewIntervalMillis(0)
                .multipartUploadTtlMillis(TTL) // the takeover advance makes the upload stale
                .compactionIntervalMillis(0)
                .balancerIntervalMillis(0);
    }

    @AfterEach
    void tearDown() {
        nodeA.close();
        nodeB.close();
        store.close();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static void put(BoxEngine engine, String key) {
        engine.putCandy(CandyKey.of(key), bytes("v-" + key), null, Map.of(), null);
    }

    private boolean manifestExists(BoxName box, int partition) {
        return coordination.get(CandyboxKeys.manifestKey(box.value(), partition)).isPresent();
    }

    // ---- deleteBox takeover ----------------------------------------------------------------

    @Test
    void deleteBoxTakesOverAPartitionWhoseOwnerReleasedIt() {
        nodeA.createBox(Z, 2);
        nodeA.releasePartition(Z, 1);
        nodeB.openPartition(Z, 1);
        nodeB.releasePartition(Z, 1);

        nodeA.deleteBox(Z, false);

        assertThat(nodeA.boxExists(Z)).isFalse();
        assertThat(nodeB.boxExists(Z)).isFalse();
        assertThat(manifestExists(Z, 0)).isFalse();
        assertThat(manifestExists(Z, 1)).isFalse();
    }

    @Test
    void deleteBoxTakesOverAPartitionWhoseOwnerCrashed() {
        nodeA.createBox(Z, 2);
        nodeA.releasePartition(Z, 1);
        nodeB.openPartition(Z, 1);
        // B never renews (crash); A renews halfway so only B's lease lapses.
        clock.advance(TTL / 2 + 1);
        nodeA.renewLeases();
        clock.advance(TTL / 2 + 1);
        assertThat(nodeA.currentOwner(Z, 1)).isEmpty();

        nodeA.deleteBox(Z, false);

        assertThat(nodeA.boxExists(Z)).isFalse();
        assertThat(manifestExists(Z, 0)).isFalse();
        assertThat(manifestExists(Z, 1)).isFalse();
    }

    // ---- stale ownership view --------------------------------------------------------------

    /**
     * A owns X and Y and believes it still owns X after B took it over (fencing A's ledgers). Y's
     * lease is kept alive by renewing halfway through the TTL; X's renewals are suppressed so only X
     * lapses and can be taken over.
     */
    private void staleViewOfX(boolean withUpload) {
        nodeA.createBox(X, 1);
        nodeA.createBox(Y, 1);
        for (BoxName box : List.of(X, Y)) {
            BoxEngine engine = nodeA.enginePartition(box, 0);
            put(engine, "a");
            engine.flush();
            put(engine, "b");
            engine.flush();
        }
        if (withUpload) {
            nodeA.enginePartition(X, 0).createMultipartUpload(CandyKey.of("mpu"), null, Map.of());
        }
        staleCoordination.suppressRenewalOf(X.value());
        clock.advance(TTL / 2 + 1);
        nodeA.renewLeases();
        clock.advance(TTL / 2 + 1);
        nodeB.openBox(X);
        staleCoordination.reportValidFor(X.value());
        assertThat(nodeA.ownsPartition(X.value(), 0)).isTrue();
    }

    private void assertXIntactViaB() {
        BoxEngine engine = nodeB.enginePartition(X, 0);
        assertThat(engine.getCandy(CandyKey.of("a"))).isEqualTo(bytes("v-a"));
        assertThat(engine.getCandy(CandyKey.of("b"))).isEqualTo(bytes("v-b"));
    }

    @Test
    void compactionWithAStaleViewSkipsTheFencedPartitionAndStillCompactsOthers() {
        staleViewOfX(false);
        int[] performed = new int[1];

        assertThatCode(() -> performed[0] = nodeA.compactOwnedBoxesOnce()).doesNotThrowAnyException();

        assertThat(performed[0]).isPositive(); // Y still compacted
        assertThat(nodeA.enginePartition(Y, 0).getCandy(CandyKey.of("a"))).isEqualTo(bytes("v-a"));
        assertXIntactViaB();
    }

    @Test
    void gcWithAStaleViewDoesNotThrowAndLeavesTheNewOwnersDataIntact() {
        staleViewOfX(false);

        assertThatCode(nodeA::collectGarbageOnce).doesNotThrowAnyException();

        assertXIntactViaB();
        assertThat(nodeA.enginePartition(Y, 0).getCandy(CandyKey.of("b"))).isEqualTo(bytes("v-b"));
    }

    @Test
    void multipartSweepWithAStaleViewDoesNotThrowAndLeavesTheUploadWithTheNewOwner() {
        staleViewOfX(true);

        assertThatCode(nodeA::sweepStaleMultipartUploadsOnce).doesNotThrowAnyException();

        // B's own sweep may abort it; either way the new owner's state is consistent.
        assertThatCode(() -> nodeB.enginePartition(X, 0).listMultipartUploads())
                .doesNotThrowAnyException();
        assertXIntactViaB();
    }

    // ---- disabled multipart TTL ------------------------------------------------------------

    @Test
    void disabledMultipartTtlNeverSweeps() {
        CandyboxConfig noTtl = baseConfig().multipartUploadTtlMillis(0).build();
        try (CandyboxNode node = new CandyboxNode(3, noTtl, store, coordination, clock)) {
            node.createBox(X, 1);
            BoxEngine engine = node.enginePartition(X, 0);
            engine.createMultipartUpload(CandyKey.of("mpu"), null, Map.of());
            clock.advance(30L * 24 * 3600 * 1000);
            node.renewLeases(); // advancing past the TTL would otherwise lapse the lease

            assertThat(node.sweepStaleMultipartUploadsOnce()).isZero();
            assertThat(engine.listMultipartUploads()).hasSize(1);
        }
    }

    // ---- GC of a Box deleted under the owner -----------------------------------------------

    @Test
    void gcOfABoxForceDeletedByAnotherNodeDoesNotThrowAndTheSweepDropsOwnership() {
        nodeA.createBox(D, 1);
        put(nodeA.enginePartition(D, 0), "k");

        // A's lease is live, so B cannot take D over; force skips it and removes the descriptor.
        nodeB.deleteBox(D, true);
        assertThat(nodeB.boxExists(D)).isFalse();
        // Drops A's cached descriptor while A still holds its ownership object, so GC sees the
        // descriptor gone (readBoxSyrupRefs).
        assertThat(nodeA.boxExists(D)).isFalse();
        assertThat(nodeA.ownsPartition(D.value(), 0)).isTrue();

        assertThatCode(nodeA::collectGarbageOnce).doesNotThrowAnyException();

        nodeA.sweepDeletedBoxes();
        assertThat(nodeA.ownsPartition(D.value(), 0)).isFalse();
    }

    /**
     * Delegates to the shared service, except that leases of one Box can be made to ignore
     * {@code renew()} and, once reported valid, answer {@code isValid() == true} even after expiring
     * (a node paused through its lease lapse).
     */
    private static final class StaleViewCoordination implements CoordinationService {
        private final CoordinationService delegate;
        private volatile String noRenewFor;
        private volatile String alwaysValidFor;

        StaleViewCoordination(CoordinationService delegate) {
            this.delegate = delegate;
        }

        void suppressRenewalOf(String box) {
            noRenewFor = box;
        }

        void reportValidFor(String box) {
            alwaysValidFor = box;
        }

        @Override
        public Optional<Lease> tryAcquireLease(String resource, int nodeId, long ttlMillis) {
            return delegate.tryAcquireLease(resource, nodeId, ttlMillis).map(this::wrap);
        }

        private Lease wrap(Lease lease) {
            return new Lease() {
                @Override
                public String resource() {
                    return lease.resource();
                }

                @Override
                public int ownerNodeId() {
                    return lease.ownerNodeId();
                }

                @Override
                public long fencingToken() {
                    return lease.fencingToken();
                }

                @Override
                public boolean isValid() {
                    String stale = alwaysValidFor;
                    return (stale != null && lease.resource().contains(stale)) || lease.isValid();
                }

                @Override
                public void renew() {
                    String skip = noRenewFor;
                    if (skip == null || !lease.resource().contains(skip)) {
                        lease.renew();
                    }
                }

                @Override
                public void release() {
                    lease.release();
                }
            };
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
        public List<String> children(String path) {
            return delegate.children(path);
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
}
