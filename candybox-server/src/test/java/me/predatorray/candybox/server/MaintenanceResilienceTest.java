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
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
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
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.lsm.manifest.RenameIntent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Background maintenance must keep going across transient coordination (ZooKeeper) errors. */
class MaintenanceResilienceTest {

    private static final BoxName BOX = BoxName.of("maint-box");
    private static final byte[] VALUE = "v".getBytes(StandardCharsets.UTF_8);
    private static final long WINDOW = 60_000; // GC grace, multipart TTL and rename abandon window

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
                        && failOnce.compareAndSet(true, false) ? connectionLoss() : null);
        CandyboxConfig config = CandyboxConfig.builder()
                .compactionIntervalMillis(20).l0CompactionTrigger(2).build();
        try (CandyboxNode node = new CandyboxNode(1, config, store, flaky)) {
            node.createBox(BOX, 1);
            awaitTrue(() -> !failOnce.get());

            for (int i = 0; i < 6; i++) {
                node.engine(BOX, "k" + i).putCandy(CandyKey.of("k" + i), VALUE, null, Map.of(), null);
                node.enginePartition(BOX, 0).flush();
            }
            awaitTrue(() -> level0(node, 0) < 2);
        }
    }

    @Test
    void scheduledTasksSurviveAnErrorEscapingThem() throws Exception {
        Set<String> pending = ConcurrentHashMap.newKeySet();
        pending.addAll(Set.of("candybox-compaction-", "candybox-lease-", "candybox-balancer-"));
        Map<String, AtomicInteger> ticksAfterError = new ConcurrentHashMap<>();
        String p0Resource = CandyboxKeys.ownerResource(BOX.value(), 0);
        // One Error per scheduled task, each from a call only that task makes; later calls are counted.
        CoordinationService flaky = flaky(new InMemoryCoordinationService(), (method, key) -> {
            String task = switch (method) {
                case "lease.isValid" -> key.equals(p0Resource) ? "candybox-compaction-" : null;
                case "lease.renew" -> "candybox-lease-";
                case "tryAcquireLease" -> key.equals(CandyboxKeys.BALANCER_RESOURCE)
                        ? "candybox-balancer-" : null;
                default -> null;
            };
            if (task == null || !Thread.currentThread().getName().startsWith(task)) {
                return null;
            }
            if (pending.remove(task)) {
                throw new AssertionError("simulated unexpected error in " + task);
            }
            ticksAfterError.computeIfAbsent(task, t -> new AtomicInteger()).incrementAndGet();
            return null;
        });
        CandyboxConfig config = CandyboxConfig.builder().compactionIntervalMillis(20)
                .leaseRenewIntervalMillis(20).balancerIntervalMillis(20).l0CompactionTrigger(2).build();
        try (CandyboxNode node = new CandyboxNode(1, config, store, flaky)) {
            node.createBox(BOX, 2);
            awaitTrue(pending::isEmpty);

            for (int i = 0; i < 2; i++) {
                putAndFlush(node, keyIn(0, i));
            }
            awaitTrue(() -> level0(node, 0) < 2);
            awaitTrue(() -> ticksAfterError.keySet().size() == 3);
        }
    }

    @ParameterizedTest
    @EnumSource(value = Fault.class, names = {"THROW", "ERROR"})
    void compactionSkipsOnlyThePartitionWhoseOwnershipCheckFails(Fault fault) {
        AtomicReference<Fault> p0 = new AtomicReference<>(Fault.NONE);
        try (CandyboxNode node = twoPartitionNode(p0, CandyboxConfig.builder()
                .l0CompactionTrigger(2).build(), new ManualClock(1000))) {
            for (int i = 0; i < 2; i++) {
                putAndFlush(node, keyIn(1, i));
            }
            p0.set(fault);

            assertThat(node.compactOwnedBoxesOnce()).isPositive();
            assertThat(level0(node, 1)).isZero();
        }
    }

    @ParameterizedTest
    @EnumSource(value = Fault.class, names = {"THROW", "ERROR"})
    void gcSkipsABoxWhosePartitionFailedToPublishItsRefs(Fault fault) {
        AtomicBoolean failP0Publish = new AtomicBoolean();
        ManualClock clock = new ManualClock(1000);
        InMemoryCoordinationService real = new InMemoryCoordinationService();
        String p0Refs = CandyboxKeys.partitionRefsKey(BOX.value(), 0);
        CandyboxConfig config = CandyboxConfig.builder()
                .l0CompactionTrigger(2).ledgerGcGraceMillis(WINDOW).build();
        try (CandyboxNode node = new CandyboxNode(1, config, store, flaky(real, (method, key) ->
                failP0Publish.get() && !method.equals("get") && key.equals(p0Refs)
                        ? fail(fault) : null), clock)) {
            node.createBox(BOX, 2);
            node.collectGarbageOnce();
            for (int i = 0; i < 2; i++) {
                putAndFlush(node, keyIn(1, i));
            }
            assertThat(node.compactOwnedBoxesOnce()).isPositive();
            putAndFlush(node, keyIn(0, 0)); // p0's refs change, so its next publish must write
            clock.advance(WINDOW + 1);

            // p0's published refs are now stale, so p1's obsolete ledgers must not be reclaimed yet.
            failP0Publish.set(true);
            assertThat(node.collectGarbageOnce()).isZero();
            assertThat(real.get(CandyboxKeys.partitionRefsKey(BOX.value(), 1))).isPresent();

            failP0Publish.set(false);
            assertThat(node.collectGarbageOnce()).isPositive();
        }
    }

    @ParameterizedTest
    @EnumSource(value = Fault.class, names = {"THROW", "ERROR"})
    void multipartSweepSkipsOnlyThePartitionWhoseOwnershipCheckFails(Fault fault) {
        AtomicReference<Fault> p0 = new AtomicReference<>(Fault.NONE);
        ManualClock clock = new ManualClock(1000);
        CandyboxConfig config = CandyboxConfig.builder().multipartUploadTtlMillis(WINDOW).build();
        try (CandyboxNode node = twoPartitionNode(p0, config, clock)) {
            node.enginePartition(BOX, 0).createMultipartUpload(CandyKey.of(keyIn(0, 0)), null, Map.of());
            node.enginePartition(BOX, 1).createMultipartUpload(CandyKey.of(keyIn(1, 0)), null, Map.of());
            clock.advance(WINDOW + 1);
            p0.set(fault);

            assertThat(node.sweepStaleMultipartUploadsOnce()).isEqualTo(1);
            assertThat(node.enginePartition(BOX, 1).listMultipartUploads()).isEmpty();
        }
    }

    @ParameterizedTest
    @EnumSource(value = Fault.class, names = {"THROW", "ERROR"})
    void renameFinalizeAndRelaySweepSkipOnlyThePartitionWhoseOwnershipCheckFails(Fault fault) {
        AtomicReference<Fault> p0 = new AtomicReference<>(Fault.NONE);
        ManualClock clock = new ManualClock(1000);
        CandyboxConfig config = CandyboxConfig.builder().renameIntentAbandonMillis(WINDOW).build();
        try (CandyboxNode node = twoPartitionNode(p0, config, clock)) {
            seedStaleRenameIntentAndRelayInP1(node, clock);
            p0.set(fault);

            org.junit.jupiter.api.Assertions.assertAll(
                    () -> {
                        node.finalizeRenameIntentsOnce();
                        assertThat(node.enginePartition(BOX, 1).listRenameIntents()).isEmpty();
                    },
                    () -> assertThat(node.sweepStaleLocatorRelaysOnce()).isEqualTo(1));
        }
    }

    @Test
    void everyPassSkipsAPartitionWhoseLeaseWasLost() {
        AtomicReference<Fault> p0 = new AtomicReference<>(Fault.NONE);
        ManualClock clock = new ManualClock(1000);
        CandyboxConfig config = CandyboxConfig.builder().l0CompactionTrigger(2)
                .ledgerGcGraceMillis(WINDOW).multipartUploadTtlMillis(WINDOW)
                .renameIntentAbandonMillis(WINDOW).build();
        try (CandyboxNode node = twoPartitionNode(p0, config, clock)) {
            BoxEngine p1 = node.enginePartition(BOX, 1);
            for (int i = 0; i < 2; i++) {
                putAndFlush(node, keyIn(1, i));
            }
            p1.createMultipartUpload(CandyKey.of(keyIn(1, 0)), null, Map.of());
            seedStaleRenameIntentAndRelayInP1(node, clock);
            p0.set(Fault.LOST);

            assertThat(node.compactOwnedBoxesOnce()).isPositive();
            clock.advance(WINDOW + 1);
            assertThat(node.collectGarbageOnce()).isPositive();
            assertThat(node.sweepStaleMultipartUploadsOnce()).isEqualTo(1);
            node.finalizeRenameIntentsOnce();
            assertThat(p1.listRenameIntents()).isEmpty();
            assertThat(node.sweepStaleLocatorRelaysOnce()).isEqualTo(1);
        }
    }

    /** Leaves p1 with a rename intent and a locator relay that both fall out of their window. */
    private static void seedStaleRenameIntentAndRelayInP1(CandyboxNode node, ManualClock clock) {
        BoxEngine p1 = node.enginePartition(BOX, 1);
        String src = keyIn(1, 7);
        p1.putCandy(CandyKey.of(src), VALUE, null, Map.of(), null);
        var locator = p1.resolveLocator(CandyKey.of(src));
        p1.recordRenameIntent(new RenameIntent("stuck", src, locator.hlc(), "dst", 0,
                clock.currentTimeMillis()));
        node.writeLocatorRelay(BOX.value(), 1, new LocatorRelay("User:test", src, null, null,
                clock.currentTimeMillis(), locator));
        clock.advance(WINDOW + 1);
    }

    private enum Fault { NONE, THROW, ERROR, LOST }

    /** A node owning both partitions of {@link #BOX}, whose partition-0 lease check fails on demand. */
    private CandyboxNode twoPartitionNode(AtomicReference<Fault> p0, CandyboxConfig config,
                                          ManualClock clock) {
        String p0Resource = CandyboxKeys.ownerResource(BOX.value(), 0);
        CandyboxNode node = new CandyboxNode(1, config, store,
                flaky(new InMemoryCoordinationService(), (method, key) -> {
                    if (!method.equals("lease.isValid") || !key.equals(p0Resource)) {
                        return null;
                    }
                    return switch (p0.get()) {
                        case THROW -> connectionLoss();
                        case ERROR -> throw new AssertionError("persistent partition error");
                        case LOST -> false;
                        case NONE -> null;
                    };
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

    private static void awaitTrue(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private static Object fail(Fault fault) {
        if (fault == Fault.ERROR) {
            throw new AssertionError("persistent publish error");
        }
        return connectionLoss();
    }

    private static Object connectionLoss() {
        throw new CoordinationException("simulated connection loss");
    }

    /**
     * Sees every keyed coordination call as {@code (method, key)} and every lease call as
     * {@code ("lease.<method>", resource)}. Returns {@code null} to defer to the real call, or a
     * value to return instead; may throw.
     */
    private interface Interceptor {
        Object intercept(String method, String key);
    }

    private static CoordinationService flaky(CoordinationService real, Interceptor interceptor) {
        return proxy(CoordinationService.class, real, (method, args) -> {
            if (args != null && args.length > 0 && args[0] instanceof String key) {
                Object override = interceptor.intercept(method.getName(), key);
                if (override != null) {
                    return override;
                }
            }
            Object result = invoke(method, real, args);
            if (method.getName().equals("tryAcquireLease")) {
                return ((Optional<?>) result).map(lease -> flakyLease((Lease) lease, interceptor));
            }
            return result;
        });
    }

    private static Lease flakyLease(Lease real, Interceptor interceptor) {
        return proxy(Lease.class, real, (method, args) -> {
            Object override = interceptor.intercept("lease." + method.getName(), real.resource());
            return override != null ? override : invoke(method, real, args);
        });
    }

    private interface Handler {
        Object handle(Method method, Object[] args) throws Throwable;
    }

    private static <T> T proxy(Class<T> type, T real, Handler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (proxy, method, args) -> handler.handle(method, args)));
    }

    private static Object invoke(Method method, Object target, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
