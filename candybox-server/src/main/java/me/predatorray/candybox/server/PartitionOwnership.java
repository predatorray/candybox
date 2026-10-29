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
import me.predatorray.candybox.coordination.BoxDescriptor;
import me.predatorray.candybox.coordination.CasConflictException;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.Lease;
import me.predatorray.candybox.coordination.VersionedValue;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One node's fenced ownership of one <em>partition</em> of a Box: the ZK lease plus the
 * {@link BoxEngine} it guards, tied together by the per-partition manifest pointer in coordination.
 * Each partition is an independent LSM engine, so the partitions of one Box can be owned and served
 * by different nodes concurrently.
 *
 * <p>Acquisition is the handover sequence, unchanged from the per-Box days but at partition
 * granularity:
 * <ol>
 *   <li>acquire the {@code boxes/&lt;box&gt;/partitions/&lt;p&gt;/owner} lease (the single
 *       serialization point for this partition);</li>
 *   <li>read the {@code boxes/&lt;box&gt;/partitions/&lt;p&gt;/manifest} pointer;</li>
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
final class PartitionOwnership implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionOwnership.class);

    private final BoxName box;
    private final int partition;
    private final CoordinationService coordination;
    private final Lease lease;
    private final BoxEngine engine;
    private LedgerRetention.ScopedStore retentionScope;
    private String pointerKey;
    private String incarnation;

    private PartitionOwnership(BoxName box, int partition, CoordinationService coordination,
                               Lease lease, BoxEngine engine) {
        this.box = box;
        this.partition = partition;
        this.coordination = coordination;
        this.lease = lease;
        this.engine = engine;
        this.pointerKey = manifestKey(box, partition);
    }

    static String ownerResource(BoxName box, int partition) {
        return CandyboxKeys.ownerResource(box.value(), partition);
    }

    static String manifestKey(BoxName box, int partition) {
        return CandyboxKeys.manifestKey(box.value(), partition);
    }

    /** Acquires ownership of a brand-new partition (the manifest pointer must not already exist). */
    static PartitionOwnership createNew(BoxName box, int partition, CandyboxConfig config,
                                        LedgerStore store, CoordinationService coordination,
                                        int nodeId, Clock clock) {
        Lease lease = acquireLease(box, partition, coordination, nodeId, config);
        try {
            LedgerRetention.ScopedStore scope = null;
            String incarnation = coordination.get(CandyboxKeys.boxMetaKey(box.value()))
                    .map(value -> BoxDescriptor.decode(value.value()).incarnation()).orElse("");
            String pointerKey = manifestKey(box, partition);
            if (!incarnation.isEmpty()) {
                String generationKey = CandyboxKeys.incarnationManifestKey(box.value(), partition, incarnation);
                if (coordination.get(generationKey).isPresent()) { pointerKey = generationKey; }
            }
            if (config.ledgerEnumerationGraceMillis() > 0) {
                var descriptor = BoxDescriptor.decode(coordination.get(CandyboxKeys.boxMetaKey(box.value()))
                        .orElseThrow(() -> new BoxNotFoundException(box.value())).value());
                incarnation = descriptor.incarnation();
                pointerKey = CandyboxKeys.incarnationManifestKey(box.value(), partition, incarnation);
                scope = new LedgerRetention(store, coordination, clock)
                        .allocate(box.value(), descriptor.incarnation(), partition, lease.fencingToken());
                store = scope;
            }
            var existingPointer = coordination.get(pointerKey);
            if (existingPointer.isPresent() && !incarnation.isEmpty()) {
                byte[] priorValue = existingPointer.get().value();
                String priorIncarnation = "";
                if (priorValue.length != 0) {
                    try { priorIncarnation = ManifestPointer.decode(priorValue).incarnation(); }
                    catch (RuntimeException malformed) {
                        // Unknown nonempty state remains occupied; never overwrite it.
                    }
                }
                if (priorValue.length == 0 || (!priorIncarnation.isEmpty() && !priorIncarnation.equals(incarnation))) {
                    pointerKey = CandyboxKeys.incarnationManifestKey(box.value(), partition, incarnation);
                    existingPointer = coordination.get(pointerKey);
                }
            }
            if (existingPointer.isPresent()) {
                throw new BoxAlreadyExistsException(box.value());
            }
            BoxEngine engine = BoxEngine.createNew(box, config, store, nodeId, clock,
                    lease.fencingToken());
            try {
                coordination.create(pointerKey,
                        new ManifestPointer(engine.manifestLedgerId(), lease.fencingToken(), incarnation).encode());
            } catch (CasConflictException raced) {
                engine.close(); // a concurrent creator won; abandon our ledgers (GC: phase-3)
                throw new BoxAlreadyExistsException(box.value());
            }
            if (scope != null) {
                try {
                    scope.inheritAndRetire(engine.retainedLedgers(), engine.manifestState().pendingDeletes().keySet());
                } catch (RuntimeException failure) {
                    engine.close();
                    throw failure;
                }
            }
            PartitionOwnership owned = new PartitionOwnership(box, partition, coordination, lease, engine);
            owned.retentionScope = scope;
            owned.pointerKey = pointerKey;
            owned.incarnation = incarnation;
            return owned;
        } catch (RuntimeException e) {
            lease.release();
            throw e;
        }
    }

    /** Acquires ownership of an existing partition by recovering its manifest (pointer must exist). */
    static PartitionOwnership recover(BoxName box, int partition, CandyboxConfig config,
                                      LedgerStore store, CoordinationService coordination,
                                      int nodeId, Clock clock) {
        Lease lease = acquireLease(box, partition, coordination, nodeId, config);
        try {
            LedgerRetention.ScopedStore scope = null;
            String incarnation = coordination.get(CandyboxKeys.boxMetaKey(box.value()))
                    .map(value -> BoxDescriptor.decode(value.value()).incarnation()).orElse("");
            String pointerKey = manifestKey(box, partition);
            if (!incarnation.isEmpty()) {
                String generationKey = CandyboxKeys.incarnationManifestKey(box.value(), partition, incarnation);
                if (coordination.get(generationKey).isPresent()) { pointerKey = generationKey; }
            }
            if (config.ledgerEnumerationGraceMillis() > 0) {
                var descriptor = BoxDescriptor.decode(coordination.get(CandyboxKeys.boxMetaKey(box.value()))
                        .orElseThrow(() -> new BoxNotFoundException(box.value())).value());
                incarnation = descriptor.incarnation();
                pointerKey = CandyboxKeys.incarnationManifestKey(box.value(), partition, incarnation);
                scope = new LedgerRetention(store, coordination, clock)
                        .allocate(box.value(), descriptor.incarnation(), partition, lease.fencingToken());
                store = scope;
            }
            Optional<VersionedValue> incarnationPointer = coordination.get(pointerKey);
            VersionedValue pointer = incarnationPointer.or(() -> coordination.get(manifestKey(box, partition)))
                    .orElseThrow(() -> new BoxNotFoundException(box.value()));
            ManifestPointer previous = ManifestPointer.decode(pointer.value());
            if ((scope != null && !previous.incarnation().equals(incarnation))
                    || (!previous.incarnation().isEmpty() && !incarnation.isEmpty()
                        && !previous.incarnation().equals(incarnation))) {
                throw new NotOwnerException("Manifest pointer has no matching Box incarnation; "
                        + "recover once with enumeration disabled before enabling it");
            }
            long priorManifestLedgerId = previous.ledgerId();

            BoxEngine engine = BoxEngine.recover(box, config, store, nodeId, clock,
                    priorManifestLedgerId, lease.fencingToken());
            try {
                byte[] next = new ManifestPointer(engine.manifestLedgerId(), lease.fencingToken(), incarnation).encode();
                if (incarnationPointer.isEmpty() && scope != null) {
                    coordination.create(pointerKey, next);
                } else {
                    coordination.compareAndSet(pointerKey, next, pointer.version());
                }
            } catch (CasConflictException raced) {
                engine.close();
                throw new NotOwnerException(box.value()); // someone advanced the pointer; we lost
            } catch (RuntimeException e) {
                engine.close(); // the CAS outcome is unknown; the next owner recovers either way
                throw e;
            }
            LOG.info("Node {} recovered ownership of box {} partition {} (token {})", nodeId, box,
                    partition, lease.fencingToken());
            if (scope != null) {
                try {
                    scope.inheritAndRetire(engine.retainedLedgers(), engine.manifestState().pendingDeletes().keySet());
                } catch (RuntimeException failure) {
                    engine.close();
                    throw failure;
                }
            }
            PartitionOwnership owned = new PartitionOwnership(box, partition, coordination, lease, engine);
            owned.retentionScope = scope;
            owned.pointerKey = pointerKey;
            owned.incarnation = incarnation;
            return owned;
        } catch (RuntimeException e) {
            lease.release();
            throw e;
        }
    }

    private static Lease acquireLease(BoxName box, int partition, CoordinationService coordination,
                                      int nodeId, CandyboxConfig config) {
        Optional<Lease> lease = coordination.tryAcquireLease(ownerResource(box, partition), nodeId,
                config.ownershipLeaseTtlMillis());
        return lease.orElseThrow(() -> new NotOwnerException(box.value()));
    }

    BoxName box() {
        return box;
    }

    int partition() {
        return partition;
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
            LOG.warn("Lost lease on box {} partition {}: {}", box, partition, e.getMessage());
            return false;
        }
    }

    /** The coordination key + version are needed to delete the pointer when the Box is dropped. */
    Optional<VersionedValue> currentPointer() {
        return coordination.get(pointerKey);
    }

    /**
     * Closes for a planned handover: flushes the memtable first (best effort) so the next owner's WAL
     * replay window is small, then releases the lease.
     */
    void closeForHandover() {
        try {
            if (lease.isValid()) {
                engine.flush();
            }
        } catch (RuntimeException e) {
            LOG.warn("Pre-handover flush failed for box {} partition {}: {}", box, partition,
                    e.getMessage());
        }
        close();
    }

    boolean allocationRotationDue(long graceMillis) {
        return retentionScope != null && retentionScope.rotationDue(graceMillis);
    }

    String pointerKey() { return pointerKey; }

    String incarnation() { return incarnation; }

    void retireDeletedAllocations() {
        if (retentionScope != null) { retentionScope.retireDeleted(); }
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
