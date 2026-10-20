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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.ManualClock;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.coordination.BoxDescriptor;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.CoordinationException;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.Lease;
import me.predatorray.candybox.coordination.fake.InMemoryCoordinationService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Background maintenance must keep going across transient coordination (ZooKeeper) errors. */
class MaintenanceResilienceTest {

    private static final BoxName BOX = BoxName.of("maint-box");
    private static final byte[] VALUE = "v".getBytes(StandardCharsets.UTF_8);
    private static final long GC_GRACE = 60_000;

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();

    @AfterEach
    void closeStore() {
        store.close();
    }

    @Test
    void scheduledMaintenanceKeepsCompactingAfterACoordinationError() throws Exception {
        AtomicBoolean failOnce = new AtomicBoolean(true);
        CoordinationService flaky = flaky(new InMemoryCoordinationService(SystemClock.INSTANCE),
                (method, key) -> method.equals("get") && key.endsWith("/refs")
                        && failOnce.compareAndSet(true, false),
                resource -> null);
        CandyboxConfig config = CandyboxConfig.builder()
                .compactionIntervalMillis(20).l0CompactionTrigger(2).build();
        try (CandyboxNode node = new CandyboxNode(1, config, store, flaky)) {
            node.createBox(BOX, 1);
            long deadline = System.currentTimeMillis() + 5_000;
            while (failOnce.get() && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertThat(failOnce).as("the injected error fired").isFalse();

            for (int i = 0; i < 6; i++) {
                node.engine(BOX, "k" + i).putCandy(CandyKey.of("k" + i), VALUE, null, Map.of(), null);
                node.enginePartition(BOX, 0).flush();
            }
            while (level0(node, 0) >= 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertThat(level0(node, 0)).isLessThan(2);
        }
    }

    @Test
    void compactionSkipsOnlyThePartitionWhoseOwnershipCheckFails() {
        AtomicReference<Fault> p0 = new AtomicReference<>(Fault.NONE);
        try (CandyboxNode node = twoPartitionNode(p0, CandyboxConfig.builder()
                .l0CompactionTrigger(2).build())) {
            for (int i = 0; i < 2; i++) {
                putAndFlush(node, keyIn(1, i));
            }
            p0.set(Fault.THROW);

            assertThat(node.compactOwnedBoxesOnce()).isPositive();
            assertThat(level0(node, 1)).isZero();
        }
    }

    @Test
    void gcSkipsABoxWhosePartitionFailedToPublishItsRefs() {
        AtomicBoolean failP0Publish = new AtomicBoolean();
        ManualClock clock = new ManualClock(1000);
        InMemoryCoordinationService real = new InMemoryCoordinationService();
        String p0Refs = CandyboxKeys.partitionRefsKey(BOX.value(), 0);
        CandyboxConfig config = CandyboxConfig.builder()
                .l0CompactionTrigger(2).ledgerGcGraceMillis(GC_GRACE).build();
        try (CandyboxNode node = new CandyboxNode(1, config, store, flaky(real,
                (method, key) -> failP0Publish.get() && !method.equals("get") && key.equals(p0Refs),
                resource -> null), clock)) {
            node.createBox(BOX, 2);
            node.collectGarbageOnce();
            for (int i = 0; i < 2; i++) {
                putAndFlush(node, keyIn(1, i));
            }
            assertThat(node.compactOwnedBoxesOnce()).isPositive();
            clock.advance(GC_GRACE + 1);

            // p0's published refs are now stale, so p1's obsolete ledgers must not be reclaimed yet.
            failP0Publish.set(true);
            assertThat(node.collectGarbageOnce()).isZero();
            assertThat(real.get(CandyboxKeys.partitionRefsKey(BOX.value(), 1))).isPresent();

            failP0Publish.set(false);
            assertThat(node.collectGarbageOnce()).isPositive();
        }
    }

    @Test
    void multipartSweepSkipsOnlyThePartitionWhoseOwnershipCheckFails() {
        AtomicReference<Fault> p0 = new AtomicReference<>(Fault.NONE);
        ManualClock clock = new ManualClock(1000);
        CandyboxConfig config = CandyboxConfig.builder().multipartUploadTtlMillis(60_000).build();
        try (CandyboxNode node = twoPartitionNode(p0, config, clock)) {
            node.enginePartition(BOX, 0).createMultipartUpload(CandyKey.of(keyIn(0, 0)), null, Map.of());
            node.enginePartition(BOX, 1).createMultipartUpload(CandyKey.of(keyIn(1, 0)), null, Map.of());
            clock.advance(120_000);
            p0.set(Fault.THROW);

            assertThat(node.sweepStaleMultipartUploadsOnce()).isEqualTo(1);
            assertThat(node.enginePartition(BOX, 1).listMultipartUploads()).isEmpty();
        }
    }

    @Test
    void renameFinalizeAndRelaySweepTolerateAFailingOwnershipCheck() {
        AtomicReference<Fault> p0 = new AtomicReference<>(Fault.NONE);
        try (CandyboxNode node = twoPartitionNode(p0, CandyboxConfig.defaults())) {
            p0.set(Fault.THROW);

            assertThat(node.finalizeRenameIntentsOnce()).isZero();
            assertThat(node.sweepStaleLocatorRelaysOnce()).isZero();
        }
    }

    @Test
    void everyPassSkipsAPartitionWhoseLeaseWasLost() {
        AtomicReference<Fault> p0 = new AtomicReference<>(Fault.NONE);
        ManualClock clock = new ManualClock(1000);
        CandyboxConfig config = CandyboxConfig.builder()
                .l0CompactionTrigger(2).multipartUploadTtlMillis(60_000).build();
        try (CandyboxNode node = twoPartitionNode(p0, config, clock)) {
            for (int i = 0; i < 2; i++) {
                putAndFlush(node, keyIn(1, i));
            }
            node.enginePartition(BOX, 1).createMultipartUpload(CandyKey.of(keyIn(1, 0)), null, Map.of());
            clock.advance(120_000);
            p0.set(Fault.LOST);

            assertThat(node.compactOwnedBoxesOnce()).isPositive();
            node.collectGarbageOnce();
            assertThat(node.sweepStaleMultipartUploadsOnce()).isEqualTo(1);
            assertThat(node.finalizeRenameIntentsOnce()).isZero();
            assertThat(node.sweepStaleLocatorRelaysOnce()).isZero();
        }
    }

    @Test
    void scheduledTasksSurviveAnErrorEscapingAPass() throws Exception {
        AtomicReference<Fault> p0 = new AtomicReference<>(Fault.NONE);
        CandyboxConfig config = CandyboxConfig.builder().compactionIntervalMillis(20)
                .balancerIntervalMillis(20).l0CompactionTrigger(2).build();
        try (CandyboxNode node = twoPartitionNode(p0, config, new ManualClock(1000))) {
            p0.set(Fault.ERROR_ONCE);
            long deadline = System.currentTimeMillis() + 5_000;
            while (p0.get() != Fault.NONE && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertThat(p0.get()).as("the injected error fired").isEqualTo(Fault.NONE);

            for (int i = 0; i < 2; i++) {
                putAndFlush(node, keyIn(0, i));
            }
            while (level0(node, 0) >= 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertThat(level0(node, 0)).isLessThan(2);
        }
    }

    private enum Fault { NONE, THROW, LOST, ERROR_ONCE }

    private CandyboxNode twoPartitionNode(AtomicReference<Fault> p0, CandyboxConfig config) {
        return twoPartitionNode(p0, config, new ManualClock(1000));
    }

    /** A node owning both partitions of {@link #BOX}, whose partition-0 lease check fails on demand. */
    private CandyboxNode twoPartitionNode(AtomicReference<Fault> p0, CandyboxConfig config,
                                          ManualClock clock) {
        String p0Resource = CandyboxKeys.ownerResource(BOX.value(), 0);
        CandyboxNode node = new CandyboxNode(1, config, store,
                flaky(new InMemoryCoordinationService(), (method, key) -> false, resource -> {
                    if (!resource.equals(p0Resource)) {
                        return null;
                    }
                    switch (p0.get()) {
                        case THROW -> throw new CoordinationException("simulated connection loss");
                        case LOST -> {
                            return false;
                        }
                        case ERROR_ONCE -> {
                            if (!Thread.currentThread().getName().startsWith("candybox-compaction-")) {
                                return null;
                            }
                            p0.set(Fault.NONE);
                            throw new AssertionError("simulated unexpected error");
                        }
                        default -> {
                            return null;
                        }
                    }
                }), clock);
        node.createBox(BOX, 2);
        return node;
    }

    private static void putAndFlush(CandyboxNode node, String key) {
        node.engine(BOX, key).putCandy(CandyKey.of(key), VALUE, null, Map.of(), null);
        node.engine(BOX, key).flush();
    }

    private static String keyIn(int partition, int nth) {
        BoxDescriptor descriptor = new BoxDescriptor(2);
        int found = 0;
        for (int i = 0; ; i++) {
            String key = "k" + i;
            if (descriptor.partitionOf(key) == partition && found++ == nth) {
                return key;
            }
        }
    }

    private static int level0(CandyboxNode node, int partition) {
        return node.enginePartition(BOX, partition).manifestState().level0().size();
    }

    /** Overrides a lease's {@code isValid()}: {@code null} defers to the real lease; may throw. */
    private interface LeaseCheck {
        Boolean isValid(String resource);
    }

    /**
     * Delegates to {@code real}, but a keyed call throws a {@link CoordinationException} (a ZooKeeper
     * connection loss) whenever {@code fail(method, key)} says so, and leases defer to {@code leaseCheck}.
     */
    private static CoordinationService flaky(CoordinationService real,
                                             BiPredicate<String, String> fail, LeaseCheck leaseCheck) {
        return (CoordinationService) Proxy.newProxyInstance(CoordinationService.class.getClassLoader(),
                new Class<?>[] {CoordinationService.class}, (proxy, method, args) -> {
                    if (args != null && args.length > 0 && args[0] instanceof String key
                            && fail.test(method.getName(), key)) {
                        throw new CoordinationException("simulated connection loss");
                    }
                    Object result = invoke(method, real, args);
                    if (method.getName().equals("tryAcquireLease")) {
                        String resource = (String) args[0];
                        return ((Optional<?>) result).map(lease -> flakyLease((Lease) lease,
                                () -> leaseCheck.isValid(resource)));
                    }
                    return result;
                });
    }

    private static Lease flakyLease(Lease real, java.util.function.Supplier<Boolean> check) {
        return (Lease) Proxy.newProxyInstance(Lease.class.getClassLoader(), new Class<?>[] {Lease.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("isValid")) {
                        Boolean valid = check.get();
                        if (valid != null) {
                            return valid;
                        }
                    }
                    return invoke(method, real, args);
                });
    }

    private static Object invoke(java.lang.reflect.Method method, Object target, Object[] args)
            throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
