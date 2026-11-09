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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.auth.BoxAcl;
import me.predatorray.candybox.common.auth.Principal;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.BoxAlreadyExistsException;
import me.predatorray.candybox.common.exception.BoxNotFoundException;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.CoordinationException;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.Lease;
import me.predatorray.candybox.coordination.LeaseInfo;
import me.predatorray.candybox.coordination.VersionedValue;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import me.predatorray.candybox.server.PartitionAssignment.BoxPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Deterministic coverage of the {@code CasConflictException} / unknown-outcome catches that keep
 * "a single fenced owner per partition" and "the manifest pointer advances only by versioned CAS"
 * true. Each race is injected through {@link RacingCoordination} hooks that write to the delegate
 * right before our call, so no threads or sleeps are involved.
 */
class CoordinationRaceTest {

    private static final BoxName BOX = BoxName.of("race");
    private static final String MANIFEST_KEY = CandyboxKeys.manifestKey("race", 0);
    private static final String OWNER_RESOURCE = CandyboxKeys.ownerResource("race", 0);
    private static final String ACL_KEY = CandyboxKeys.boxAclKey("race");
    private static final BoxAcl ACL_A = BoxAcl.privateTo(Principal.user("alice"));
    private static final BoxAcl ACL_B = BoxAcl.privateTo(Principal.user("bob"));
    private static final byte[] RACER_BYTES = "racer".getBytes(StandardCharsets.UTF_8);
    private static final CandyKey KEY = CandyKey.of("k");

    private final ManualClock clock = new ManualClock(1_000);
    private final CandyboxConfig config = CandyboxConfig.builder()
            .leaseRenewIntervalMillis(0).build();
    private InMemoryLedgerStore store;
    private InMemoryCoordinationService backing;
    private RacingCoordination coordination;

    @BeforeEach
    void setUp() {
        store = new InMemoryLedgerStore();
        backing = new InMemoryCoordinationService(clock);
        coordination = new RacingCoordination(backing);
    }

    @AfterEach
    void tearDown() {
        store.close();
    }

    private PartitionOwnership createNew(int nodeId) {
        return PartitionOwnership.createNew(BOX, 0, config, store, coordination, nodeId, clock);
    }

    private PartitionOwnership recover(int nodeId) {
        return PartitionOwnership.recover(BOX, 0, config, store, coordination, nodeId, clock);
    }

    /** Creates the partition on node 1, writes one Candy and releases it; returns the pointer. */
    private VersionedValue createWriteAndRelease() {
        PartitionOwnership first = createNew(1);
        first.engine().putCandy(KEY, "v1".getBytes(StandardCharsets.UTF_8), null, Map.of(), null);
        first.close();
        return backing.get(MANIFEST_KEY).orElseThrow();
    }

    /** A different-but-valid write to {@code key} through the delegate, bumping its version. */
    private void racerWrites(String key, byte[] value) {
        Optional<VersionedValue> current = backing.get(key);
        if (current.isEmpty()) {
            backing.create(key, value);
        } else {
            backing.compareAndSet(key, value, current.get().version());
        }
    }

    // ---- PartitionOwnership ------------------------------------------------------------------

    @Test
    void createNewLosingToConcurrentCreatorReleasesLeaseAndKeepsWinnersPointer() {
        coordination.beforeCreate = key -> {
            if (key.equals(MANIFEST_KEY)) {
                backing.create(key, RACER_BYTES);
            }
        };

        assertThatThrownBy(() -> createNew(1)).isInstanceOf(BoxAlreadyExistsException.class);

        assertThat(backing.leaseHolder(OWNER_RESOURCE)).isEmpty();
        assertThat(backing.get(MANIFEST_KEY).orElseThrow().value()).isEqualTo(RACER_BYTES);
        assertThat(backing.tryAcquireLease(OWNER_RESOURCE, 2, 10_000)).isPresent();
    }

    @Test
    void recoverLosingPointerCasThrowsNotOwnerAndLeavesWinnersPointer() {
        VersionedValue original = createWriteAndRelease();
        byte[] racerPointer = pointerLike(original, 99);
        coordination.beforeCompareAndSet = (key, expected) -> {
            if (key.equals(MANIFEST_KEY)) {
                racerWrites(key, racerPointer);
            }
        };

        assertThatThrownBy(() -> recover(2)).isInstanceOf(NotOwnerException.class);

        assertThat(backing.leaseHolder(OWNER_RESOURCE)).isEmpty();
        assertThat(backing.get(MANIFEST_KEY).orElseThrow().value()).isEqualTo(racerPointer);
    }

    @Test
    void recoverWithUnknownCasOutcomeRethrowsSameExceptionAndReleasesLease() {
        createWriteAndRelease();
        CoordinationException boom = new CoordinationException("boom");
        coordination.failCompareAndSetWith = boom;

        assertThatThrownBy(() -> recover(2)).isSameAs(boom);

        assertThat(backing.leaseHolder(OWNER_RESOURCE)).isEmpty();
        assertThat(coordination.failCompareAndSetWith).isNull();
    }

    @Test
    void cleanRetryAfterLostPointerCasRecoversNodeOnesData() {
        VersionedValue original = createWriteAndRelease();
        coordination.beforeCompareAndSet = (key, expected) -> {
            if (key.equals(MANIFEST_KEY)) {
                racerWrites(key, pointerLike(original, 99));
            }
        };
        assertThatThrownBy(() -> recover(2)).isInstanceOf(NotOwnerException.class);

        coordination.beforeCompareAndSet = (key, expected) -> { };
        try (PartitionOwnership second = recover(2)) {
            assertThat(second.engine().getCandy(KEY)).isEqualTo("v1".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void cleanRetryAfterUnknownCasOutcomeRecoversNodeOnesData() {
        createWriteAndRelease();
        coordination.failCompareAndSetWith = new CoordinationException("boom");
        assertThatThrownBy(() -> recover(2)).isInstanceOf(CoordinationException.class);

        try (PartitionOwnership second = recover(2)) {
            assertThat(second.engine().getCandy(KEY)).isEqualTo("v1".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void nodeHeartbeatIncludesBlockedRecovery() throws Exception {
        createWriteAndRelease();
        PausingStore paused = new PausingStore(store);
        var executor = Executors.newSingleThreadExecutor();
        try (CandyboxNode node = new CandyboxNode(2, config, paused, coordination, clock)) {
            var future = executor.submit(() -> node.openPartition(BOX, 0));
            assertThat(paused.entered.await(10, TimeUnit.SECONDS)).isTrue();
            for (int i = 0; i < 3; i++) {
                clock.advance(config.ownershipLeaseTtlMillis() * 2 / 3);
                node.renewLeases();
                assertThat(backing.tryAcquireLease(OWNER_RESOURCE, 3,
                        config.ownershipLeaseTtlMillis())).isEmpty();
            }
            paused.resume.countDown();
            future.get(10, TimeUnit.SECONDS);
            assertThat(node.enginePartition(BOX, 0).getCandy(KEY)).isEqualTo("v1".getBytes(StandardCharsets.UTF_8));
        } finally {
            paused.resume.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void nodeShutdownCancelsBlockedRecovery() throws Exception {
        createWriteAndRelease();
        PausingStore paused = new PausingStore(store);
        var executor = Executors.newSingleThreadExecutor();
        var closer = Executors.newSingleThreadExecutor();
        CandyboxNode node = new CandyboxNode(2, config, paused, coordination, clock);
        byte[] before = backing.get(MANIFEST_KEY).orElseThrow().value();
        try {
            var future = executor.submit(() -> node.openPartition(BOX, 0));
            assertThat(paused.entered.await(10, TimeUnit.SECONDS)).isTrue();
            CountDownLatch released = new CountDownLatch(1);
            coordination.afterRelease = released::countDown;
            var closed = closer.submit(node::close);
            assertThat(released.await(10, TimeUnit.SECONDS)).isTrue();
            paused.resume.countDown();
            assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(me.predatorray.candybox.coordination.LeaseExpiredException.class);
            closed.get(10, TimeUnit.SECONDS);
            assertThat(backing.get(MANIFEST_KEY).orElseThrow().value()).isEqualTo(before);
            assertThat(backing.leaseHolder(OWNER_RESOURCE)).isEmpty();
        } finally {
            paused.resume.countDown();
            node.close();
            executor.shutdownNow();
            closer.shutdownNow();
        }
    }

    @Test
    void pausedRecoveryRenewsBeyondOriginalTtlAndTransfersWithoutRegistrationGap() throws Exception {
        createWriteAndRelease();
        OwnershipLeases leases = new OwnershipLeases();
        PausingStore paused = new PausingStore(store);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var future = executor.submit(() -> PartitionOwnership.recover(BOX, 0, config, paused,
                    coordination, 2, clock, leases));
            assertThat(paused.entered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(leases.size()).isEqualTo(1);
            for (int i = 0; i < 3; i++) {
                clock.advance(config.ownershipLeaseTtlMillis() * 2 / 3);
                leases.renew();
                assertThat(backing.tryAcquireLease(OWNER_RESOURCE, 3,
                        config.ownershipLeaseTtlMillis())).isEmpty();
            }
            paused.resume.countDown();
            try (PartitionOwnership recovered = future.get(10, TimeUnit.SECONDS)) {
                assertThat(leases.size()).isEqualTo(1);
                assertThat(recovered.engine().getCandy(KEY)).isEqualTo("v1".getBytes(StandardCharsets.UTF_8));
            }
            assertThat(leases.size()).isZero();
        } finally {
            paused.resume.countDown();
            leases.close();
            executor.shutdownNow();
        }
    }

    @Test
    void ambiguousRenewalFailurePermanentlyCancelsEvenWhileRemoteLeaseRemainsValid() throws Exception {
        byte[] before = createWriteAndRelease().value();
        coordination.failRenewals = true;
        OwnershipLeases leases = new OwnershipLeases();
        PausingStore paused = new PausingStore(store);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var future = executor.submit(() -> PartitionOwnership.recover(BOX, 0, config, paused,
                    coordination, 2, clock, leases));
            assertThat(paused.entered.await(10, TimeUnit.SECONDS)).isTrue();
            leases.renew();
            assertThat(leases.size()).isZero();
            assertThat(backing.leaseHolder(OWNER_RESOURCE)).isPresent();
            coordination.failRenewals = false;
            leases.renew();
            paused.resume.countDown();
            assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(me.predatorray.candybox.coordination.LeaseExpiredException.class);
            assertThat(backing.get(MANIFEST_KEY).orElseThrow().value()).isEqualTo(before);
            assertThat(backing.leaseHolder(OWNER_RESOURCE)).isEmpty();
        } finally {
            paused.resume.countDown();
            leases.close();
            executor.shutdownNow();
        }
    }

    @Test
    void renewalFailureWhilePausedCannotOverwriteSuccessorOrServe() throws Exception {
        VersionedValue before = createWriteAndRelease();
        OwnershipLeases leases = new OwnershipLeases();
        PausingStore paused = new PausingStore(store);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var future = executor.submit(() -> PartitionOwnership.recover(BOX, 0, config, paused,
                    coordination, 2, clock, leases));
            assertThat(paused.entered.await(10, TimeUnit.SECONDS)).isTrue();
            clock.advance(config.ownershipLeaseTtlMillis() + 1);
            leases.renew();
            assertThat(leases.size()).isZero();
            try (PartitionOwnership winner = recover(3)) {
                byte[] committed = backing.get(MANIFEST_KEY).orElseThrow().value();
                assertThat(committed).isNotEqualTo(before.value());
                paused.resume.countDown();
                assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(me.predatorray.candybox.coordination.LeaseExpiredException.class);
                assertThat(backing.get(MANIFEST_KEY).orElseThrow().value()).isEqualTo(committed);
                assertThat(winner.engine().getCandy(KEY)).isEqualTo("v1".getBytes(StandardCharsets.UTF_8));
            }
        } finally {
            paused.resume.countDown();
            leases.close();
            executor.shutdownNow();
        }
    }

    @Test
    void shutdownCancelsPausedRecoveryAndRepeatedRetriesReleaseRegistrations() throws Exception {
        createWriteAndRelease();
        for (int attempt = 0; attempt < 3; attempt++) {
            OwnershipLeases leases = new OwnershipLeases();
            PausingStore paused = new PausingStore(store);
            var executor = Executors.newSingleThreadExecutor();
            byte[] before = backing.get(MANIFEST_KEY).orElseThrow().value();
            try {
                var future = executor.submit(() -> PartitionOwnership.recover(BOX, 0, config, paused,
                        coordination, 2, clock, leases));
                assertThat(paused.entered.await(10, TimeUnit.SECONDS)).isTrue();
                leases.close();
                assertThat(leases.size()).isZero();
                paused.resume.countDown();
                assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(me.predatorray.candybox.coordination.LeaseExpiredException.class);
                assertThat(backing.get(MANIFEST_KEY).orElseThrow().value()).isEqualTo(before);
                assertThat(backing.leaseHolder(OWNER_RESOURCE)).isEmpty();
            } finally {
                paused.resume.countDown();
                leases.close();
                executor.shutdownNow();
            }
        }
        try (PartitionOwnership recovered = recover(3)) {
            assertThat(recovered.engine().getCandy(KEY)).isEqualTo("v1".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void unknownCommittedPublicationKeepsEveryLedgerAndCanBeRecovered() {
        createWriteAndRelease();
        coordination.failAfterCompareAndSetWith = new CoordinationException("committed but response lost");
        var before = store.listLedgers();
        assertThatThrownBy(() -> recover(2)).isInstanceOf(CoordinationException.class);
        assertThat(store.listLedgers()).containsAll(before);
        long published = ManifestPointer.decode(backing.get(MANIFEST_KEY).orElseThrow().value()).ledgerId();
        assertThat(store.listLedgers()).contains(published);
        try (PartitionOwnership recovered = recover(3)) {
            assertThat(recovered.engine().getCandy(KEY)).isEqualTo("v1".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void cancelledServingOwnershipClosesHandlesBeforeRepeatedSameNodeRetries() {
        var closed = new java.util.ArrayList<java.util.concurrent.atomic.AtomicBoolean>();
        LedgerStore tracking = trackingStore(closed);
        try (var node = new CandyboxNode(1, config, tracking, coordination, clock)) {
            node.createBox(BOX, 1);
            node.enginePartition(BOX, 0).putCandy(KEY, new byte[]{7}, null, Map.of(), null);
            for (int attempt = 0; attempt < 3; attempt++) {
                var initialHandles = List.copyOf(closed);
                coordination.failRenewals = true;
                node.renewLeases();
                coordination.failRenewals = false;
                node.openPartition(BOX, 0);
                assertThat(initialHandles).isNotEmpty().allMatch(java.util.concurrent.atomic.AtomicBoolean::get,
                        "prior serving handles closed before retry returns");
                assertThat(node.enginePartition(BOX, 0).getCandy(KEY)).containsExactly((byte) 7);
            }
        }
        assertThat(closed).allMatch(java.util.concurrent.atomic.AtomicBoolean::get,
                "all created handles closed after shutdown");
    }

    @Test
    void registryShutdownClosesCancelledServingAcquisitionsWithoutPartitionMap() {
        createWriteAndRelease();
        var closed = new java.util.ArrayList<java.util.concurrent.atomic.AtomicBoolean>();
        try (OwnershipLeases leases = new OwnershipLeases();
             PartitionOwnership recovered = PartitionOwnership.recover(BOX, 0, config,
                     trackingStore(closed), coordination, 2, clock, leases)) {
            coordination.failRenewals = true;
            leases.renew();
            assertThat(leases.size()).isZero();
            leases.close();
            assertThat(closed).isNotEmpty().allMatch(java.util.concurrent.atomic.AtomicBoolean::get,
                    "cancelled acquisitions remain reachable for cleanup before map installation");
        }
    }

    private LedgerStore trackingStore(List<java.util.concurrent.atomic.AtomicBoolean> closed) {
        return new LedgerStore() {
            public WritableLedger createLedger(LedgerConfig cfg) {
                WritableLedger actual = store.createLedger(cfg);
                var wasClosed = new java.util.concurrent.atomic.AtomicBoolean();
                closed.add(wasClosed);
                return (WritableLedger) java.lang.reflect.Proxy.newProxyInstance(
                        WritableLedger.class.getClassLoader(), new Class<?>[]{WritableLedger.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("close")) { wasClosed.set(true); }
                            try { return method.invoke(actual, args); }
                            catch (java.lang.reflect.InvocationTargetException e) { throw e.getCause(); }
                        });
            }
            public ReadableLedger openLedger(long id) { return store.openLedger(id); }
            public ReadableLedger recoverOpen(long id) { return store.recoverOpen(id); }
            public List<Long> listLedgers() { return store.listLedgers(); }
            public void deleteLedger(long id) { store.deleteLedger(id); }
            public void close() { }
        };
    }

    private static final class PausingStore implements LedgerStore {
        private final LedgerStore delegate;
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch resume = new CountDownLatch(1);
        private final AtomicInteger opens = new AtomicInteger();
        PausingStore(LedgerStore delegate) { this.delegate = delegate; }
        public WritableLedger createLedger(LedgerConfig config) { return delegate.createLedger(config); }
        public ReadableLedger openLedger(long id) { return delegate.openLedger(id); }
        public ReadableLedger recoverOpen(long id) {
            if (opens.getAndIncrement() == 0) {
                entered.countDown();
                try {
                    if (!resume.await(10, TimeUnit.SECONDS)) { throw new IllegalStateException("resume timed out"); }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
            return delegate.recoverOpen(id);
        }
        public void deleteLedger(long id) { delegate.deleteLedger(id); }
        public List<Long> listLedgers() { return delegate.listLedgers(); }
        public void close() { }
    }

    private static byte[] pointerLike(VersionedValue pointer, long token) {
        return new ManifestPointer(ManifestPointer.decode(pointer.value()).ledgerId(), token).encode();
    }

    // ---- BoxAclStore -------------------------------------------------------------------------

    private BoxAclStore aclStore() {
        return new BoxAclStore(coordination, clock);
    }

    @Test
    void aclSetThatLosesEveryRetryThrowsInsteadOfDroppingTheWrite() {
        AtomicInteger races = new AtomicInteger();
        coordination.beforeCreate = key -> raceAcl(key, races);
        coordination.beforeCompareAndSet = (key, expected) -> raceAcl(key, races);

        assertThatThrownBy(() -> aclStore().set("race", ACL_A))
                .isInstanceOf(CandyboxException.class)
                .hasMessageContaining("Concurrent ACL update on Box race");
        assertThat(races.get()).isGreaterThan(1);
        assertThat(BoxAcl.fromBytes(backing.get(ACL_KEY).orElseThrow().value())).isNotEqualTo(ACL_A);
    }

    private void raceAcl(String key, AtomicInteger races) {
        if (key.equals(ACL_KEY)) {
            races.incrementAndGet();
            racerWrites(key, ACL_B.toBytes());
        }
    }

    @Test
    void aclSetRetriesPastOneConflictAndInvalidatesTheCache() {
        BoxAclStore acls = aclStore();
        assertThat(acls.get("race")).isEmpty(); // primes the TTL cache with "absent"
        AtomicInteger races = new AtomicInteger();
        coordination.beforeCreate = key -> {
            if (key.equals(ACL_KEY) && races.getAndIncrement() == 0) {
                racerWrites(key, ACL_B.toBytes());
            }
        };

        acls.set("race", ACL_A);

        assertThat(races.get()).isEqualTo(1);
        assertThat(acls.get("race")).contains(ACL_A);
    }

    @Test
    void aclSeedLosesQuietlyToTheFirstAcl() {
        BoxAclStore acls = aclStore();

        acls.seed("race", ACL_A);
        acls.seed("race", ACL_B);

        assertThat(acls.get("race")).contains(ACL_A);
    }

    @Test
    void aclDeleteUnderPersistentContentionReturnsAndLeavesTheDocument() {
        BoxAclStore acls = aclStore();
        acls.set("race", ACL_A);
        coordination.beforeDelete = key -> {
            if (key.equals(ACL_KEY)) {
                racerWrites(key, ACL_B.toBytes());
            }
        };

        acls.delete("race");

        assertThat(backing.get(ACL_KEY).orElseThrow().value()).isEqualTo(ACL_B.toBytes());
    }

    // ---- PartitionBalancer -------------------------------------------------------------------

    @Test
    void lostAssignmentPublishDoesNotThrowAndTheClusterConverges() {
        BoxName box = BoxName.of("spread");
        try (CandyboxNode node1 = new CandyboxNode(1, config, store, coordination, clock);
             CandyboxNode node2 = new CandyboxNode(2, config, store, coordination, clock)) {
            node1.createBox(box, 4);
            // A stale coordinator's assignment lands first, pinning everything on node 1.
            PartitionAssignment stale = new PartitionAssignment(Map.of(
                    new BoxPartition("spread", 0), 1, new BoxPartition("spread", 1), 1,
                    new BoxPartition("spread", 2), 1, new BoxPartition("spread", 3), 1));
            AtomicInteger races = new AtomicInteger();
            coordination.beforeCreate = key -> {
                if (key.equals(CandyboxKeys.ASSIGNMENT_KEY) && races.getAndIncrement() == 0) {
                    backing.create(key, stale.encode());
                }
            };
            coordination.beforeCompareAndSet = (key, expected) -> {
                if (key.equals(CandyboxKeys.ASSIGNMENT_KEY) && races.getAndIncrement() == 0) {
                    racerWrites(key, stale.encode());
                }
            };

            node1.runBalancerOnce(); // loses the publish race; must not throw
            assertThat(races.get()).isEqualTo(1);

            for (int round = 0; round < 3; round++) {
                node1.runBalancerOnce();
                node2.runBalancerOnce();
            }

            int owned1 = 0;
            int owned2 = 0;
            for (int p = 0; p < 4; p++) {
                Optional<LeaseInfo> holder = backing.leaseHolder(CandyboxKeys.ownerResource("spread", p));
                assertThat(holder).isPresent();
                boolean by1 = node1.ownsPartition("spread", p);
                boolean by2 = node2.ownsPartition("spread", p);
                assertThat(by1 ^ by2).as("partition %d has exactly one owner", p).isTrue();
                assertThat(holder.get().ownerNodeId()).isEqualTo(by1 ? 1 : 2);
                owned1 += by1 ? 1 : 0;
                owned2 += by2 ? 1 : 0;
            }
            assertThat(owned1).isEqualTo(2);
            assertThat(owned2).isEqualTo(2);
        }
    }

    @Test
    void failedTakeoverOfOnePartitionDoesNotBlockTheOthersInTheRound() {
        BoxName a = BoxName.of("box-a");
        BoxName b = BoxName.of("box-b");
        try (CandyboxNode node1 = new CandyboxNode(1, config, store, coordination, clock);
             CandyboxNode node2 = new CandyboxNode(2, config, store, coordination, clock)) {
            node1.createBox(a, 2);
            node1.createBox(b, 2);
            // Keep node 1 as the elected coordinator so node 2's round only applies the assignment.
            Optional<Lease> balancerLease = backing.tryAcquireLease(CandyboxKeys.BALANCER_RESOURCE,
                    1, 10_000_000);
            assertThat(balancerLease).isPresent();
            // Ordered so the failing takeovers (box-a) are attempted before the healthy ones.
            Map<BoxPartition, Integer> targets = new LinkedHashMap<>();
            targets.put(new BoxPartition("box-a", 0), 2);
            targets.put(new BoxPartition("box-a", 1), 2);
            targets.put(new BoxPartition("box-b", 0), 2);
            targets.put(new BoxPartition("box-b", 1), 2);
            backing.create(CandyboxKeys.ASSIGNMENT_KEY, new PartitionAssignment(targets).encode());
            for (BoxName box : List.of(a, b)) {
                for (int p = 0; p < 2; p++) {
                    node1.releasePartitionForHandover(box, p);
                }
            }
            for (int p = 0; p < 2; p++) { // Box "a" vanishes right before node 2's round
                VersionedValue pointer = backing.get(CandyboxKeys.manifestKey("box-a", p)).orElseThrow();
                backing.delete(CandyboxKeys.manifestKey("box-a", p), pointer.version());
            }

            node2.runBalancerOnce();

            assertThat(node2.ownsPartition("box-a", 0)).isFalse();
            assertThat(node2.ownsPartition("box-a", 1)).isFalse();
            assertThat(node2.ownsPartition("box-b", 0)).isTrue();
            assertThat(node2.ownsPartition("box-b", 1)).isTrue();
            assertThatThrownBy(() -> node2.openPartition(a, 0)).isInstanceOf(BoxNotFoundException.class);
        }
    }

    // ---- fault-injecting decorator -----------------------------------------------------------

    /**
     * Delegates to a real fake; the {@code before*} hooks run just before the corresponding call and
     * "race" by writing to the delegate directly. {@link #failCompareAndSetWith} is thrown instead
     * of delegating, once.
     */
    private static final class RacingCoordination implements CoordinationService {
        private final CoordinationService delegate;
        volatile BiConsumer<String, Long> beforeCompareAndSet = (key, expected) -> { };
        volatile Consumer<String> beforeCreate = key -> { };
        volatile Consumer<String> beforeDelete = key -> { };
        volatile RuntimeException failCompareAndSetWith;
        volatile RuntimeException failAfterCompareAndSetWith;
        volatile boolean failRenewals;
        volatile Runnable afterRelease = () -> { };

        RacingCoordination(CoordinationService delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<VersionedValue> get(String key) {
            return delegate.get(key);
        }

        @Override
        public long create(String key, byte[] value) {
            beforeCreate.accept(key);
            return delegate.create(key, value);
        }

        @Override
        public long compareAndSetWithLease(String key, byte[] value, long expectedVersion,
                                          String resource, long token) {
            RuntimeException failure = failCompareAndSetWith;
            if (failure != null) {
                failCompareAndSetWith = null;
                throw failure;
            }
            if (expectedVersion < 0) { beforeCreate.accept(key); }
            else { beforeCompareAndSet.accept(key, expectedVersion); }
            long version = delegate.compareAndSetWithLease(key, value, expectedVersion, resource, token);
            if (failAfterCompareAndSetWith != null) {
                RuntimeException failureAfter = failAfterCompareAndSetWith;
                failAfterCompareAndSetWith = null;
                throw failureAfter;
            }
            return version;
        }

        @Override
        public long compareAndSet(String key, byte[] value, long expectedVersion) {
            RuntimeException failure = failCompareAndSetWith;
            if (failure != null) {
                failCompareAndSetWith = null;
                throw failure;
            }
            beforeCompareAndSet.accept(key, expectedVersion);
            return delegate.compareAndSet(key, value, expectedVersion);
        }

        @Override
        public void delete(String key, long expectedVersion) {
            beforeDelete.accept(key);
            delegate.delete(key, expectedVersion);
        }

        @Override
        public List<String> children(String path) {
            return delegate.children(path);
        }

        @Override
        public Optional<Lease> tryAcquireLease(String resource, int nodeId, long ttlMillis) {
            return delegate.tryAcquireLease(resource, nodeId, ttlMillis).map(lease -> new Lease() {
                public String resource() { return lease.resource(); }
                public int ownerNodeId() { return lease.ownerNodeId(); }
                public long fencingToken() { return lease.fencingToken(); }
                public boolean isValid() { return lease.isValid(); }
                public void renew() {
                    if (failRenewals) { throw new CoordinationException("renewal response lost"); }
                    lease.renew();
                }
                public void release() { lease.release(); afterRelease.run(); }
            });
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
