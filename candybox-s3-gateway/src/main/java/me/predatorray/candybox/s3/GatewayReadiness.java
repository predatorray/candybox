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
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.coordination.CoordinationService;

/**
 * Gateway readiness: the coordination session is connected and at least one node is registered to
 * route to. The membership read is cached for {@code ttlMillis} so frequent probes don't hit
 * ZooKeeper; the connection check is local and never cached.
 */
final class GatewayReadiness implements BooleanSupplier {

    static final long DEFAULT_TTL_MILLIS = 1_000;

    private final BooleanSupplier connected;
    private final Supplier<List<Integer>> members;
    private final Clock clock;
    private final long ttlMillis;
    private final ReentrantLock refreshLock = new ReentrantLock();
    private volatile Snapshot snapshot;

    private record Snapshot(boolean hasMembers, long atMillis) {
    }

    GatewayReadiness(CoordinationService coordination, Clock clock, long ttlMillis) {
        this(coordination::isConnected, coordination::members, clock, ttlMillis);
    }

    GatewayReadiness(BooleanSupplier connected, Supplier<List<Integer>> members, Clock clock,
                     long ttlMillis) {
        this.connected = connected;
        this.members = members;
        this.clock = clock;
        this.ttlMillis = ttlMillis;
    }

    @Override
    public boolean getAsBoolean() {
        if (!connected.getAsBoolean()) {
            return false;
        }
        Snapshot current = snapshot;
        long now = clock.currentTimeMillis();
        // One caller refreshes; concurrent probes answer from the last snapshot instead of queueing
        // behind a slow ZooKeeper read.
        if ((current == null || now - current.atMillis() >= ttlMillis) && refreshLock.tryLock()) {
            try {
                current = new Snapshot(hasMembers(), now);
                snapshot = current;
            } finally {
                refreshLock.unlock();
            }
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
}
