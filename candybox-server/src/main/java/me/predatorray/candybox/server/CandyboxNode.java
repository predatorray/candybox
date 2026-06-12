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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.auth.Authorizer;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.BoxAlreadyExistsException;
import me.predatorray.candybox.common.exception.BoxNotEmptyException;
import me.predatorray.candybox.common.exception.BoxNotFoundException;
import me.predatorray.candybox.common.exception.FencedException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.coordination.BoxDescriptor;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.CasConflictException;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.VersionedValue;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import me.predatorray.candybox.server.PartitionAssignment.BoxPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Candybox storage node. Every Box is split into a creation-time-fixed number of hash partitions
 * (its {@link BoxDescriptor}); the node owns a set of <em>partitions</em> under fenced ZooKeeper
 * leases (via {@link PartitionOwnership}), serving each from its own {@link BoxEngine}, so the
 * write load of one Box spreads across the cluster. {@link #createBox} creates the descriptor and
 * takes initial ownership of every partition; the {@link PartitionBalancer} then spreads ownership
 * evenly and {@link #openPartition} is the per-partition failover/takeover path. A background
 * heartbeat renews the leases.
 */
public final class CandyboxNode implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CandyboxNode.class);

    private final int nodeId;
    private final CandyboxConfig config;
    private final LedgerStore ledgerStore;
    private final CoordinationService coordination;
    private final Clock clock;
    private final BoxAclStore aclStore;
    private volatile Authorizer authorizer = Authorizer.ALLOW_ALL;
    private final ConcurrentMap<BoxPartition, PartitionOwnership> partitions =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<String, BoxDescriptor> descriptorCache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService leaseHeartbeat;
    private final ScheduledExecutorService compactionWorker;
    private final ScheduledExecutorService balancerWorker;
    private final CompactionService compactionService;
    private final GarbageCollector garbageCollector;
    private final PartitionBalancer balancer;

    /** Bounded compaction passes per partition per worker tick, so one cannot starve the others. */
    private static final int MAX_COMPACTIONS_PER_TICK = 8;

    public CandyboxNode(int nodeId, CandyboxConfig config, LedgerStore ledgerStore,
                        CoordinationService coordination) {
        this(nodeId, config, ledgerStore, coordination, SystemClock.INSTANCE);
    }

    public CandyboxNode(int nodeId, CandyboxConfig config, LedgerStore ledgerStore,
                        CoordinationService coordination, Clock clock) {
        this(nodeId, config, ledgerStore, coordination, clock, "node-" + nodeId);
    }

    /**
     * @param advertisedAddress this node's reachable {@code host:port}, published to membership so
     *                          clients can route to it. (Non-routable placeholder for in-JVM tests.)
     */
    public CandyboxNode(int nodeId, CandyboxConfig config, LedgerStore ledgerStore,
                        CoordinationService coordination, Clock clock, String advertisedAddress) {
        this.nodeId = nodeId;
        this.config = config;
        this.ledgerStore = ledgerStore;
        this.coordination = coordination;
        this.clock = clock;
        this.aclStore = new BoxAclStore(coordination, clock);
        coordination.registerMember(nodeId, advertisedAddress.getBytes(StandardCharsets.UTF_8));
        this.compactionService = new CompactionService(ledgerStore, config, clock);
        this.garbageCollector = new GarbageCollector(ledgerStore, config.ledgerGcGraceMillis(), clock);
        this.balancer = new PartitionBalancer(this, coordination, config);

        long renewInterval = config.leaseRenewIntervalMillis();
        if (renewInterval > 0) {
            this.leaseHeartbeat = daemonScheduler("candybox-lease-" + nodeId);
            this.leaseHeartbeat.scheduleAtFixedRate(this::renewLeases, renewInterval, renewInterval,
                    TimeUnit.MILLISECONDS);
        } else {
            this.leaseHeartbeat = null;
        }

        long compactionInterval = config.compactionIntervalMillis();
        if (compactionInterval > 0) {
            this.compactionWorker = daemonScheduler("candybox-compaction-" + nodeId);
            this.compactionWorker.scheduleWithFixedDelay(this::runMaintenance, compactionInterval,
                    compactionInterval, TimeUnit.MILLISECONDS);
        } else {
            this.compactionWorker = null;
        }

        long balancerInterval = config.balancerIntervalMillis();
        if (balancerInterval > 0) {
            this.balancerWorker = daemonScheduler("candybox-balancer-" + nodeId);
            this.balancerWorker.scheduleWithFixedDelay(this::runBalancerOnce, balancerInterval,
                    balancerInterval, TimeUnit.MILLISECONDS);
        } else {
            this.balancerWorker = null;
        }
    }

    /**
     * One background maintenance tick: compact owned partitions, GC their obsoleted ledgers, and sweep
     * any abandoned in-flight multipart uploads (older than {@code multipartUploadTtlMillis}).
     */
    private void runMaintenance() {
        compactOwnedBoxesOnce();
        collectGarbageOnce();
        sweepStaleMultipartUploadsOnce();
    }

    private static ScheduledExecutorService daemonScheduler(String name) {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        });
    }

    public int nodeId() {
        return nodeId;
    }

    /**
     * Installs the {@link Authorizer} consulted on every request (default {@code ALLOW_ALL} when
     * authentication is disabled). The composition root calls this before serving traffic.
     */
    public void authorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    Authorizer authorizer() {
        return authorizer;
    }

    BoxAclStore aclStore() {
        return aclStore;
    }

    /** Creates a Box with the configured default partition count and owns all partitions here. */
    public void createBox(BoxName box) {
        createBox(box, 0);
    }

    /**
     * Creates a brand-new Box: publishes its descriptor ({@code partitionCount}, or the configured
     * default if {@code 0}) and takes initial ownership of every partition on this node — the
     * balancer spreads them across the cluster afterwards.
     */
    public void createBox(BoxName box, int partitionCount) {
        int count = partitionCount > 0 ? partitionCount : config.partitionsPerBoxDefault();
        BoxDescriptor descriptor = new BoxDescriptor(count);
        try {
            coordination.create(CandyboxKeys.boxMetaKey(box.value()), descriptor.encode());
        } catch (CasConflictException exists) {
            throw new BoxAlreadyExistsException(box.value());
        }
        LOG.info("Creating box {} with {} partitions on node {}", box, count, nodeId);
        List<PartitionOwnership> created = new ArrayList<>(count);
        try {
            for (int p = 0; p < count; p++) {
                PartitionOwnership ownership = PartitionOwnership.createNew(box, p, config,
                        ledgerStore, coordination, nodeId, clock);
                created.add(ownership);
                partitions.put(new BoxPartition(box.value(), p), ownership);
            }
            descriptorCache.put(box.value(), descriptor);
        } catch (RuntimeException e) {
            // Roll back the half-created Box so a retry starts clean.
            for (PartitionOwnership ownership : created) {
                dropPartition(ownership);
            }
            deleteMetaQuietly(box);
            throw e;
        }
    }

    /**
     * Takes over ownership of one existing partition (failover/handover): acquires its lease,
     * recovers the manifest + WAL from the current pointer, and advances the pointer.
     */
    public void openPartition(BoxName box, int partition) {
        partitions.compute(new BoxPartition(box.value(), partition), (bp, existing) -> {
            if (existing != null && existing.isOwner()) {
                return existing; // already owned here
            }
            LOG.info("Opening (taking over) box {} partition {} on node {}", bp.box(),
                    bp.partition(), nodeId);
            return PartitionOwnership.recover(box, partition, config, ledgerStore, coordination,
                    nodeId, clock);
        });
    }

    /** Takes over ownership of every partition of an existing Box (test/operational convenience). */
    public void openBox(BoxName box) {
        BoxDescriptor descriptor = descriptor(box);
        for (int p = 0; p < descriptor.partitionCount(); p++) {
            openPartition(box, p);
        }
    }

    /** Relinquishes one partition (releases the lease, closes the engine); does not delete data. */
    public void releasePartition(BoxName box, int partition) {
        PartitionOwnership ownership = partitions.remove(new BoxPartition(box.value(), partition));
        if (ownership != null) {
            ownership.close();
        }
    }

    /** Like {@link #releasePartition} but flushes first so the next owner's WAL replay is small. */
    void releasePartitionForHandover(BoxName box, int partition) {
        PartitionOwnership ownership = partitions.remove(new BoxPartition(box.value(), partition));
        if (ownership != null) {
            ownership.closeForHandover();
        }
    }

    /** Relinquishes every locally owned partition of a Box; does not delete data. */
    public void releaseBox(BoxName box) {
        for (BoxPartition bp : ownedPartitionsOf(box)) {
            releasePartition(box, bp.partition());
        }
    }

    /**
     * Deletes a Box: takes over every partition this node does not own (their leases must be free —
     * with the balancer running, other owners release within a round of the descriptor disappearing),
     * checks emptiness across all partitions unless {@code force}, drops every manifest pointer, and
     * finally the descriptor.
     *
     * @throws NotOwnerException if {@code !force} and another live node still owns a partition
     */
    public void deleteBox(BoxName box, boolean force) {
        BoxDescriptor descriptor = descriptor(box);
        List<PartitionOwnership> owned = new ArrayList<>(descriptor.partitionCount());
        for (int p = 0; p < descriptor.partitionCount(); p++) {
            PartitionOwnership ownership = partitions.get(new BoxPartition(box.value(), p));
            if (ownership != null && ownership.isOwner()) {
                owned.add(ownership);
                continue;
            }
            try {
                openPartition(box, p);
                owned.add(partitions.get(new BoxPartition(box.value(), p)));
            } catch (RuntimeException e) {
                if (!force) {
                    throw new NotOwnerException(box.value());
                }
                // force: the live owner cleans its partition up once it sees the descriptor gone.
            }
        }
        if (!force) {
            for (PartitionOwnership ownership : owned) {
                if (!ownership.engine().listCandies("", null, 1).entries().isEmpty()) {
                    throw new BoxNotEmptyException(box.value());
                }
            }
        }
        for (PartitionOwnership ownership : owned) {
            partitions.remove(new BoxPartition(box.value(), ownership.partition()));
            dropPartition(ownership);
        }
        descriptorCache.remove(box.value());
        deleteMetaQuietly(box);
        aclStore.delete(box.value());
        // TODO(phase-3): reference-counted GC of the Box's SSTable/Syrup/WAL/manifest ledgers.
    }

    /** Drops a partition's manifest pointer (so it no longer exists) and closes its engine/lease. */
    private void dropPartition(PartitionOwnership ownership) {
        ownership.currentPointer().ifPresent(p -> {
            try {
                coordination.delete(
                        PartitionOwnership.manifestKey(ownership.box(), ownership.partition()),
                        p.version());
            } catch (RuntimeException e) {
                LOG.warn("Failed to delete manifest pointer for box {} partition {}: {}",
                        ownership.box(), ownership.partition(), e.getMessage());
            }
        });
        ownership.close();
    }

    private void deleteMetaQuietly(BoxName box) {
        try {
            Optional<VersionedValue> meta = coordination.get(CandyboxKeys.boxMetaKey(box.value()));
            if (meta.isPresent()) {
                coordination.delete(CandyboxKeys.boxMetaKey(box.value()), meta.get().version());
            }
        } catch (RuntimeException e) {
            LOG.warn("Failed to delete descriptor for box {}: {}", box, e.getMessage());
        }
    }

    /** Every existing Box in the cluster (descriptor present), sorted by name. */
    public List<String> listBoxes() {
        List<String> names = new ArrayList<>();
        for (String boxName : coordination.children(CandyboxKeys.BOXES_ROOT)) {
            if (coordination.get(CandyboxKeys.boxMetaKey(boxName)).isPresent()) {
                names.add(boxName);
            }
        }
        names.sort(String::compareTo);
        return names;
    }

    /**
     * A point-in-time snapshot of {@link me.predatorray.candybox.lsm.engine.BoxEngineStats} for every
     * partition this node currently owns, keyed by {@code box/partition}. Used by the health/metrics
     * endpoint; a partition that has just lost ownership is omitted rather than failing the snapshot.
     */
    public java.util.Map<String, me.predatorray.candybox.lsm.engine.BoxEngineStats> ownedBoxStats() {
        java.util.Map<String, me.predatorray.candybox.lsm.engine.BoxEngineStats> out =
                new java.util.TreeMap<>();
        for (java.util.Map.Entry<BoxPartition, PartitionOwnership> e : partitions.entrySet()) {
            PartitionOwnership ownership = e.getValue();
            if (!ownership.isOwner()) {
                continue;
            }
            try {
                out.put(e.getKey().box() + "/" + e.getKey().partition(), ownership.engine().stats());
            } catch (RuntimeException ignore) {
                // Ownership lost between the check and the read; drop this partition.
            }
        }
        return out;
    }

    /** Whether the Box exists in the cluster (its descriptor is present in coordination). */
    public boolean boxExists(BoxName box) {
        return findDescriptor(box).isPresent();
    }

    /** The Box's descriptor, or throws {@link BoxNotFoundException}. Cached (it is immutable). */
    public BoxDescriptor descriptor(BoxName box) {
        return findDescriptor(box).orElseThrow(() -> new BoxNotFoundException(box.value()));
    }

    private Optional<BoxDescriptor> findDescriptor(BoxName box) {
        BoxDescriptor cached = descriptorCache.get(box.value());
        if (cached != null) {
            return Optional.of(cached);
        }
        Optional<BoxDescriptor> loaded = coordination.get(CandyboxKeys.boxMetaKey(box.value()))
                .map(v -> BoxDescriptor.decode(v.value()));
        loaded.ifPresent(d -> descriptorCache.put(box.value(), d));
        return loaded;
    }

    /** The node currently owning one partition of {@code box} (from the lease), if any. */
    public Optional<Integer> currentOwner(BoxName box, int partition) {
        return coordination.leaseHolder(PartitionOwnership.ownerResource(box, partition))
                .map(me.predatorray.candybox.coordination.LeaseInfo::ownerNodeId);
    }

    /** Whether this node currently owns the given partition. */
    boolean ownsPartition(String box, int partition) {
        PartitionOwnership ownership = partitions.get(new BoxPartition(box, partition));
        return ownership != null && ownership.isOwner();
    }

    /**
     * Returns the engine of the partition holding {@code key}. Throws {@link BoxNotFoundException}
     * if the Box does not exist or this node has no ownership object for the partition, or
     * {@link NotOwnerException} if its lease is no longer valid.
     */
    public BoxEngine engine(BoxName box, String key) {
        return enginePartition(box, descriptor(box).partitionOf(key));
    }

    /** Returns the engine of one partition this node owns (see {@link #engine}). */
    public BoxEngine enginePartition(BoxName box, int partition) {
        PartitionOwnership ownership = partitions.get(new BoxPartition(box.value(), partition));
        if (ownership == null) {
            throw new BoxNotFoundException(box.value());
        }
        return ownership.engine();
    }

    /**
     * Drops locally owned partitions whose Box descriptor no longer exists — the convergence path of
     * a (force) {@link #deleteBox} issued on another node while this one owned some partitions.
     */
    void sweepDeletedBoxes() {
        for (BoxPartition bp : partitions.keySet()) {
            if (coordination.get(CandyboxKeys.boxMetaKey(bp.box())).isEmpty()) {
                PartitionOwnership ownership = partitions.remove(bp);
                if (ownership != null) {
                    LOG.info("Dropping box {} partition {} on node {}: the Box was deleted",
                            bp.box(), bp.partition(), nodeId);
                    dropPartition(ownership);
                }
                descriptorCache.remove(bp.box());
            }
        }
    }

    private List<BoxPartition> ownedPartitionsOf(BoxName box) {
        List<BoxPartition> owned = new ArrayList<>();
        for (BoxPartition bp : partitions.keySet()) {
            if (bp.box().equals(box.value())) {
                owned.add(bp);
            }
        }
        return owned;
    }

    CandyboxConfig config() {
        return config;
    }

    /** A request handler that serves this node's partitions; wire it into a Transport or loopback. */
    public RequestHandler requestHandler() {
        return new NodeRequestHandler(this);
    }

    /**
     * Runs one partition-balancing round (coordinate if elected, then converge on the assignment).
     * Driven by the background worker when {@code balancerIntervalMillis > 0}; exposed so tests and
     * operational tooling can drive it manually.
     */
    public void runBalancerOnce() {
        balancer.runOnce();
    }

    private void renewLeases() {
        for (PartitionOwnership ownership : partitions.values()) {
            try {
                ownership.renew();
            } catch (RuntimeException e) {
                LOG.warn("Lease renewal error for a partition on node {}", nodeId, e);
            }
        }
    }

    /**
     * Runs one bounded round of compaction over every partition this node still owns. The commit is
     * gated on the owner's fencing token by the manifest, so a partition whose ownership was lost
     * mid-round fails the commit ({@link FencedException}) and is simply skipped — a zombie owner
     * cannot corrupt state.
     *
     * <p>Exposed so it can be driven manually (tests, operational triggers) without the scheduler.
     *
     * @return the number of compactions performed across all partitions
     */
    public int compactOwnedBoxesOnce() {
        int performed = 0;
        for (PartitionOwnership ownership : partitions.values()) {
            if (!ownership.isOwner()) {
                continue;
            }
            try {
                BoxEngine engine = ownership.engine();
                for (int pass = 0; pass < MAX_COMPACTIONS_PER_TICK; pass++) {
                    if (!compactionService.compactOnce(engine)) {
                        break;
                    }
                    performed++;
                }
            } catch (FencedException | NotOwnerException lostOwnership) {
                LOG.info("Stopping compaction of a partition on node {}: {}", nodeId,
                        lostOwnership.getMessage());
            } catch (RuntimeException e) {
                LOG.warn("Compaction error on node {}", nodeId, e);
            }
        }
        return performed;
    }

    /**
     * Runs one GC pass over every partition this node owns, deleting ledgers obsoleted by committed
     * compactions past the grace period. Exposed for manual/operational triggering.
     *
     * @return the number of ledgers deleted
     */
    public int collectGarbageOnce() {
        int deleted = 0;
        for (PartitionOwnership ownership : partitions.values()) {
            if (!ownership.isOwner()) {
                continue;
            }
            try {
                deleted += garbageCollector.collect(ownership.engine());
            } catch (NotOwnerException lostOwnership) {
                LOG.info("Skipping GC of a partition on node {}: {}", nodeId,
                        lostOwnership.getMessage());
            } catch (RuntimeException e) {
                LOG.warn("GC error on node {}", nodeId, e);
            }
        }
        return deleted;
    }

    /**
     * Aborts in-flight multipart uploads whose {@code createdAtMillis + multipartUploadTtlMillis} is
     * already in the past. Reuses the engine's {@link me.predatorray.candybox.lsm.engine.BoxEngine
     * #abortMultipartUpload} (fencing-gated; orphaned Syrup segments enter the normal reclaim path).
     *
     * @return the number of uploads aborted across all owned partitions
     */
    public int sweepStaleMultipartUploadsOnce() {
        long ttl = config.multipartUploadTtlMillis();
        if (ttl <= 0) {
            return 0;
        }
        long cutoff = clock.currentTimeMillis() - ttl;
        int aborted = 0;
        for (PartitionOwnership ownership : partitions.values()) {
            if (!ownership.isOwner()) {
                continue;
            }
            try {
                for (me.predatorray.candybox.lsm.manifest.MultipartUploadState u
                        : ownership.engine().listMultipartUploads()) {
                    if (u.createdAtMillis() <= cutoff) {
                        ownership.engine().abortMultipartUpload(u.uploadId());
                        aborted++;
                    }
                }
            } catch (NotOwnerException lostOwnership) {
                LOG.info("Skipping multipart TTL sweep on a partition on node {}: {}", nodeId,
                        lostOwnership.getMessage());
            } catch (RuntimeException e) {
                LOG.warn("Multipart TTL sweep error on node {}", nodeId, e);
            }
        }
        return aborted;
    }

    @Override
    public void close() {
        if (leaseHeartbeat != null) {
            leaseHeartbeat.shutdownNow();
        }
        if (compactionWorker != null) {
            compactionWorker.shutdownNow();
        }
        if (balancerWorker != null) {
            balancerWorker.shutdownNow();
        }
        for (PartitionOwnership ownership : partitions.values()) {
            ownership.close();
        }
        partitions.clear();
        coordination.unregisterMember(nodeId);
    }
}
