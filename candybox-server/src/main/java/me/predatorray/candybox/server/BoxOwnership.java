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

import java.util.Optional;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.BoxAlreadyExistsException;
import me.predatorray.candybox.common.exception.BoxNotFoundException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.CasConflictException;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.Lease;
import me.predatorray.candybox.coordination.VersionedValue;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One node's fenced ownership of one Box: the ZK lease plus the {@link BoxEngine} it guards, tied
 * together by the per-Box manifest pointer in coordination.
 *
 * <p>Acquisition is the Phase 2 handover sequence:
 * <ol>
 *   <li>acquire the {@code boxes/&lt;box&gt;/owner} lease (the single serialization point);</li>
 *   <li>read the {@code boxes/&lt;box&gt;/manifest} pointer;</li>
 *   <li>create a brand-new engine (pointer absent) or recover the prior one (pointer present), stamping
 *       every manifest edit with the lease's fencing token;</li>
 *   <li>publish the new manifest ledger id by creating / compare-and-setting the pointer — aborting and
 *       releasing the lease if the CAS loses, which (given we hold the lease) only happens on a stale
 *       read.</li>
 * </ol>
 *
 * <p>The lease is the real serialization point; the pointer CAS is a backstop. Engine access is gated
 * on the lease still being valid, so a lost/expired owner stops serving.
 */
final class BoxOwnership implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BoxOwnership.class);

    private final BoxName box;
    private final CoordinationService coordination;
    private final Lease lease;
    private final BoxEngine engine;

    private BoxOwnership(BoxName box, CoordinationService coordination, Lease lease, BoxEngine engine) {
        this.box = box;
        this.coordination = coordination;
        this.lease = lease;
        this.engine = engine;
    }

    static String ownerResource(BoxName box) {
        return CandyboxKeys.ownerResource(box.value());
    }

    static String manifestKey(BoxName box) {
        return CandyboxKeys.manifestKey(box.value());
    }

    /** Acquires ownership of a brand-new Box (the manifest pointer must not already exist). */
    static BoxOwnership createNew(BoxName box, CandyboxConfig config, LedgerStore store,
                                  CoordinationService coordination, int nodeId, Clock clock) {
        Lease lease = acquireLease(box, coordination, nodeId, config);
        try {
            if (coordination.get(manifestKey(box)).isPresent()) {
                throw new BoxAlreadyExistsException(box.value());
            }
            BoxEngine engine = BoxEngine.createNew(box, config, store, nodeId, clock,
                    lease.fencingToken());
            try {
                coordination.create(manifestKey(box),
                        new ManifestPointer(engine.manifestLedgerId(), lease.fencingToken()).encode());
            } catch (CasConflictException raced) {
                engine.close(); // a concurrent creator won; abandon our ledgers (GC: phase-3)
                throw new BoxAlreadyExistsException(box.value());
            }
            return new BoxOwnership(box, coordination, lease, engine);
        } catch (RuntimeException e) {
            lease.release();
            throw e;
        }
    }

    /** Acquires ownership of an existing Box by recovering its manifest (the pointer must exist). */
    static BoxOwnership recover(BoxName box, CandyboxConfig config, LedgerStore store,
                                CoordinationService coordination, int nodeId, Clock clock) {
        Lease lease = acquireLease(box, coordination, nodeId, config);
        try {
            VersionedValue pointer = coordination.get(manifestKey(box))
                    .orElseThrow(() -> new BoxNotFoundException(box.value()));
            long priorManifestLedgerId = ManifestPointer.decode(pointer.value()).ledgerId();

            BoxEngine engine = BoxEngine.recover(box, config, store, nodeId, clock,
                    priorManifestLedgerId, lease.fencingToken());
            try {
                coordination.compareAndSet(manifestKey(box),
                        new ManifestPointer(engine.manifestLedgerId(), lease.fencingToken()).encode(),
                        pointer.version());
            } catch (CasConflictException raced) {
                engine.close();
                throw new NotOwnerException(box.value()); // someone advanced the pointer; we lost
            }
            LOG.info("Node {} recovered ownership of box {} (token {})", nodeId, box,
                    lease.fencingToken());
            return new BoxOwnership(box, coordination, lease, engine);
        } catch (RuntimeException e) {
            lease.release();
            throw e;
        }
    }

    private static Lease acquireLease(BoxName box, CoordinationService coordination, int nodeId,
                                      CandyboxConfig config) {
        Optional<Lease> lease = coordination.tryAcquireLease(ownerResource(box), nodeId,
                config.ownershipLeaseTtlMillis());
        return lease.orElseThrow(() -> new NotOwnerException(box.value()));
    }

    /** The engine, if this node still holds the lease; otherwise throws {@link NotOwnerException}. */
    BoxEngine engine() {
        if (!lease.isValid()) {
            throw new NotOwnerException(box.value());
        }
        return engine;
    }

    boolean isOwner() {
        return lease.isValid();
    }

    long fencingToken() {
        return lease.fencingToken();
    }

    /** Renews the lease; returns whether ownership is still held afterwards. */
    boolean renew() {
        try {
            lease.renew();
            return true;
        } catch (RuntimeException e) {
            LOG.warn("Lost lease on box {}: {}", box, e.getMessage());
            return false;
        }
    }

    /** The coordination key + version are needed to delete the pointer when the Box is dropped. */
    Optional<VersionedValue> currentPointer() {
        return coordination.get(manifestKey(box));
    }

    @Override
    public void close() {
        try {
            engine.close();
        } finally {
            lease.release();
        }
    }
}
