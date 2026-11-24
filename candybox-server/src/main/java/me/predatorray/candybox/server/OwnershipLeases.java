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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.Lease;
import me.predatorray.candybox.coordination.LeaseExpiredException;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Node-scoped renewal registry: recovering and serving partitions have the same registration. */
final class OwnershipLeases implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(OwnershipLeases.class);
    // Cleanup ownership outlives renewal eligibility: a cancelled engine must remain reachable
    // until its caller releases it or node shutdown closes it.
    private final Set<Held> held = ConcurrentHashMap.newKeySet();
    private final Set<Held> renewing = ConcurrentHashMap.newKeySet();
    private boolean closed;

    synchronized Held register(Lease lease) {
        if (closed) {
            lease.release();
            throw new LeaseExpiredException(lease.resource(), lease.fencingToken());
        }
        Held registration = new Held(lease);
        held.add(registration);
        renewing.add(registration);
        return registration;
    }

    void renew() {
        for (Held lease : renewing) {
            lease.renew();
        }
    }

    int size() { return renewing.size(); }

    @Override
    public synchronized void close() {
        closed = true;
        for (Held lease : held) {
            try {
                lease.release();
            } catch (RuntimeException failure) {
                LOG.warn("Failed to release ownership", failure);
            }
        }
    }

    final class Held implements Lease {
        private final Lease delegate;
        private volatile boolean cancelled;
        private BoxEngine engine;

        synchronized void attach(BoxEngine recovered) {
            try {
                if (!isValid()) { throw new LeaseExpiredException(resource(), fencingToken()); }
                engine = recovered;
            } catch (RuntimeException failure) {
                recovered.close();
                throw failure;
            }
        }

        private Held(Lease delegate) { this.delegate = delegate; }
        public String resource() { return delegate.resource(); }
        public int ownerNodeId() { return delegate.ownerNodeId(); }
        public long fencingToken() { return delegate.fencingToken(); }
        public boolean isValid() { return !cancelled && delegate.isValid(); }

        public synchronized void renew() {
            if (cancelled) { return; }
            try {
                delegate.renew();
            } catch (RuntimeException failure) {
                // Fail closed even for an ambiguous renewal result. Never revive this acquisition.
                cancelled = true;
                renewing.remove(this);
                LOG.warn(
                        "Cancelled ownership of {} after lease renewal failure", resource(), failure);
            }
        }

        synchronized void publish(CoordinationService coordination, String key, byte[] value, long version) {
            if (!isValid()) { throw new LeaseExpiredException(resource(), fencingToken()); }
            coordination.compareAndSetWithLease(key, value, version, resource(), fencingToken());
            if (!isValid()) { throw new LeaseExpiredException(resource(), fencingToken()); }
        }

        public synchronized void release() {
            cancelled = true;
            held.remove(this);
            renewing.remove(this);
            try {
                if (engine != null) {
                    engine.close();
                    engine = null;
                }
            } finally {
                delegate.release();
            }
        }
    }
}
