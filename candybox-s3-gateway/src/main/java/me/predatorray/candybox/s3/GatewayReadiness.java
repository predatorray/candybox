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
package me.predatorray.candybox.s3;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.coordination.CoordinationService;

/**
 * Gateway readiness: the coordination session is connected and at least one node is registered to
 * route to. The connection check is local and never cached. The membership read is a ZooKeeper round
 * trip, so it is cached for {@code ttlMillis} and refreshed on a background thread: a probe never
 * waits on ZooKeeper, which keeps the health server's single dispatcher thread free for
 * {@code /healthz}.
 */
final class GatewayReadiness implements BooleanSupplier, AutoCloseable {

    static final long DEFAULT_TTL_MILLIS = 1_000;

    private final BooleanSupplier connected;
    private final Supplier<List<Integer>> members;
    private final Clock clock;
    private final long ttlMillis;
    private final Executor refresher;
    private final ExecutorService ownedRefresher;
    private final AtomicBoolean refreshing = new AtomicBoolean();
    private volatile Snapshot snapshot;

    private record Snapshot(boolean hasMembers, long atMillis) {
    }

    GatewayReadiness(CoordinationService coordination, Clock clock, long ttlMillis) {
        this(coordination::isConnected, coordination::members, clock, ttlMillis,
                Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r, "candybox-s3-gateway-readiness");
                    t.setDaemon(true);
                    return t;
                }), true);
    }

    /**
     * @param refresher      runs the membership refresh; {@code Runnable::run} refreshes inline
     * @param ownsRefresher  whether {@link #close} shuts {@code refresher} down (it must then be an
     *                       {@link ExecutorService})
     */
    GatewayReadiness(BooleanSupplier connected, Supplier<List<Integer>> members, Clock clock,
                     long ttlMillis, Executor refresher, boolean ownsRefresher) {
        this.connected = connected;
        this.members = members;
        this.clock = clock;
        this.ttlMillis = ttlMillis;
        this.refresher = refresher;
        this.ownedRefresher = ownsRefresher ? (ExecutorService) refresher : null;
    }

    @Override
    public boolean getAsBoolean() {
        if (!connected.getAsBoolean()) {
            snapshot = null; // after reconnecting, answer from fresh membership only
            return false;
        }
        Snapshot current = snapshot;
        long now = clock.currentTimeMillis();
        if ((current == null || now - current.atMillis() >= ttlMillis)
                && refreshing.compareAndSet(false, true)) {
            try {
                refresher.execute(() -> {
                    try {
                        snapshot = new Snapshot(hasMembers(), now);
                    } finally {
                        refreshing.set(false);
                    }
                });
            } catch (RejectedExecutionException e) {
                refreshing.set(false);
            }
            current = snapshot;
        }
        return current != null && current.hasMembers();
    }

    private boolean hasMembers() {
        try {
            return !members.get().isEmpty();
        } catch (RuntimeException e) {
            return false;
        }
    }

    @Override
    public void close() {
        if (ownedRefresher != null) {
            ownedRefresher.shutdownNow();
        }
    }
}
