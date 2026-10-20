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
import java.util.function.Predicate;
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

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();

    @AfterEach
    void closeStore() {
        store.close();
    }

    @Test
    void scheduledMaintenanceKeepsCompactingAfterACoordinationError() throws Exception {
        AtomicBoolean failOnce = new AtomicBoolean(true);
        CoordinationService flaky = flaky(new InMemoryCoordinationService(SystemClock.INSTANCE),
                key -> key.endsWith("/refs") && failOnce.compareAndSet(true, false),
                resource -> false);
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
        AtomicBoolean failP0 = new AtomicBoolean();
        try (CandyboxNode node = twoPartitionNode(failP0, CandyboxConfig.builder()
                .l0CompactionTrigger(2).build())) {
            for (int i = 0; i < 2; i++) {
                putAndFlush(node, keyIn(1, i));
            }
            failP0.set(true);

            assertThat(node.compactOwnedBoxesOnce()).isPositive();
            assertThat(level0(node, 1)).isZero();
        }
    }

    @Test
    void gcPublishesOtherPartitionsRefsWhenOneFails() {
        AtomicBoolean failP0 = new AtomicBoolean();
        InMemoryCoordinationService real = new InMemoryCoordinationService(new ManualClock(1000));
        try (CandyboxNode node = new CandyboxNode(1, CandyboxConfig.defaults(), store,
                flaky(real, key -> failP0.get() && key.equals(CandyboxKeys.partitionRefsKey(
                        BOX.value(), 0)), resource -> false), new ManualClock(1000))) {
            node.createBox(BOX, 2);
            failP0.set(true);

            node.collectGarbageOnce();

            assertThat(real.get(CandyboxKeys.partitionRefsKey(BOX.value(), 1))).isPresent();
        }
    }

    @Test
    void multipartSweepSkipsOnlyThePartitionWhoseOwnershipCheckFails() {
        AtomicBoolean failP0 = new AtomicBoolean();
        ManualClock clock = new ManualClock(1000);
        CandyboxConfig config = CandyboxConfig.builder().multipartUploadTtlMillis(60_000).build();
        try (CandyboxNode node = twoPartitionNode(failP0, config, clock)) {
            node.enginePartition(BOX, 0).createMultipartUpload(CandyKey.of(keyIn(0, 0)), null, Map.of());
            node.enginePartition(BOX, 1).createMultipartUpload(CandyKey.of(keyIn(1, 0)), null, Map.of());
            clock.advance(120_000);
            failP0.set(true);

            assertThat(node.sweepStaleMultipartUploadsOnce()).isEqualTo(1);
            assertThat(node.enginePartition(BOX, 1).listMultipartUploads()).isEmpty();
        }
    }

    @Test
    void renameFinalizeAndRelaySweepTolerateAFailingOwnershipCheck() {
        AtomicBoolean failP0 = new AtomicBoolean();
        try (CandyboxNode node = twoPartitionNode(failP0, CandyboxConfig.defaults())) {
            failP0.set(true);

            assertThat(node.finalizeRenameIntentsOnce()).isZero();
            assertThat(node.sweepStaleLocatorRelaysOnce()).isZero();
        }
    }

    private CandyboxNode twoPartitionNode(AtomicBoolean failP0, CandyboxConfig config) {
        return twoPartitionNode(failP0, config, new ManualClock(1000));
    }

    /** A node owning both partitions of {@link #BOX}, whose partition-0 lease check throws on demand. */
    private CandyboxNode twoPartitionNode(AtomicBoolean failP0, CandyboxConfig config, ManualClock clock) {
        String p0 = CandyboxKeys.ownerResource(BOX.value(), 0);
        CandyboxNode node = new CandyboxNode(1, config, store,
                flaky(new InMemoryCoordinationService(), key -> false,
                        resource -> failP0.get() && resource.equals(p0)), clock);
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

    /**
     * Delegates to {@code real}, but {@code get(key)} and the {@code isValid()} of leases on
     * {@code resource} throw a {@link CoordinationException} (a ZooKeeper connection loss) whenever the
     * matching predicate says so.
     */
    private static CoordinationService flaky(CoordinationService real, Predicate<String> failGet,
                                             Predicate<String> failLease) {
        return (CoordinationService) Proxy.newProxyInstance(CoordinationService.class.getClassLoader(),
                new Class<?>[] {CoordinationService.class}, (proxy, method, args) -> {
                    if (method.getName().equals("get") && failGet.test((String) args[0])) {
                        throw new CoordinationException("simulated connection loss");
                    }
                    Object result = invoke(method, real, args);
                    if (method.getName().equals("tryAcquireLease")) {
                        String resource = (String) args[0];
                        return ((Optional<?>) result).map(lease -> flakyLease((Lease) lease,
                                () -> failLease.test(resource)));
                    }
                    return result;
                });
    }

    private static Lease flakyLease(Lease real, java.util.function.BooleanSupplier fail) {
        return (Lease) Proxy.newProxyInstance(Lease.class.getClassLoader(), new Class<?>[] {Lease.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("isValid") && fail.getAsBoolean()) {
                        throw new CoordinationException("simulated connection loss");
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
