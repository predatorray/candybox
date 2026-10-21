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

import java.util.function.BooleanSupplier;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.coordination.CoordinationService;

/**
 * Gateway readiness: the coordination session is connected and at least one node is registered to
 * route to. The membership read is cached for {@code ttlMillis} so frequent probes don't hit
 * ZooKeeper; the connection check is local and never cached.
 */
final class GatewayReadiness implements BooleanSupplier {

    static final long DEFAULT_TTL_MILLIS = 1_000;

    private final CoordinationService coordination;
    private final Clock clock;
    private final long ttlMillis;
    private boolean cachedHasMembers;
    private long cachedAtMillis;
    private boolean cached;

    GatewayReadiness(CoordinationService coordination, Clock clock, long ttlMillis) {
        this.coordination = coordination;
        this.clock = clock;
        this.ttlMillis = ttlMillis;
    }

    @Override
    public synchronized boolean getAsBoolean() {
        if (!coordination.isConnected()) {
            return false;
        }
        long now = clock.currentTimeMillis();
        if (!cached || now - cachedAtMillis >= ttlMillis) {
            cachedHasMembers = hasMembers();
            cachedAtMillis = now;
            cached = true;
        }
        return cachedHasMembers;
    }

    private boolean hasMembers() {
        try {
            return !coordination.members().isEmpty();
        } catch (RuntimeException e) {
            return false;
        }
    }
}
