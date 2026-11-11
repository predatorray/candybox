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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.auth.Authorizer;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.BoxAlreadyExistsException;
import me.predatorray.candybox.common.exception.BoxNotEmptyException;
import me.predatorray.candybox.common.exception.BoxNotFoundException;
import me.predatorray.candybox.common.exception.BusyException;
import me.predatorray.candybox.common.exception.FencedException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.coordination.BoxDescriptor;
import me.predatorray.candybox.coordination.CandyboxKeys;
import me.predatorray.candybox.coordination.CasConflictException;
import me.predatorray.candybox.coordination.CoordinationCas;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.VersionedValue;
import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.BinaryWriter;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.lsm.engine.CandyMetadata;
import me.predatorray.candybox.lsm.manifest.RenameIntent;
import me.predatorray.candybox.lsm.manifest.RenameReceipt;
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

    private static final int MAX_REFS_SNAPSHOT_ATTEMPTS = 3;

    static final int MAX_PENDING_LOCATOR_RELAYS = 1024;
    static final long RELAY_RECOUNT_INTERVAL_MILLIS = 1_000;

    private final int nodeId;
    private final CandyboxConfig config;
    private final LedgerStore ledgerStore;
    private final CoordinationService coordination;
    private final Clock clock;
    private final BoxAclStore aclStore;
    private volatile boolean closed;
    private final AtomicLong pendingRelayBytes = new AtomicLong();
    private final AtomicLong lastRelayRecountMillis = new AtomicLong(Long.MIN_VALUE / 2);
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
    private final LedgerRetention retention;
    private long lastEnumeration;
    private volatile LedgerRetention.Report enumerationReport = new LedgerRetention.Report(0, 0, 0, 0, 0);

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
        this.retention = new LedgerRetention(ledgerStore, coordination, clock);
        this.garbageCollector = new GarbageCollector(ledgerStore, config.ledgerGcGraceMillis(), clock,
                (engine, ledger) -> config.ledgerEnumerationGraceMillis() == 0
                        || retention.deletePending(engine, ledger));
        this.balancer = new PartitionBalancer(this, coordination, config);

        long renewInterval = config.leaseRenewIntervalMillis();
        if (renewInterval > 0) {
            this.leaseHeartbeat = daemonScheduler("candybox-lease-" + nodeId);
            this.leaseHeartbeat.scheduleAtFixedRate(guarded("lease renewal", this::renewLeases),
                    renewInterval, renewInterval, TimeUnit.MILLISECONDS);
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
            this.balancerWorker.scheduleWithFixedDelay(guarded("balancer", this::runBalancerOnce),
                    balancerInterval, balancerInterval, TimeUnit.MILLISECONDS);
        } else {
            this.balancerWorker = null;
        }
    }

    /**
     * One background maintenance tick: compact owned partitions, GC their obsoleted ledgers, and sweep
     * abandoned multipart uploads, finalize rename intents, and sweep stale locator relays.
     * Every phase is guarded so a failed phase cannot escape the tick or suppress later phases.
     */
    private void runMaintenance() {
        guarded("allocation epoch rotation", this::rotateAllocationEpochs).run();
        guarded("compaction", this::compactOwnedBoxesOnce).run();
        guarded("GC", this::collectGarbageOnce).run();
        guarded("multipart TTL sweep", this::sweepStaleMultipartUploadsOnce).run();
        guarded("rename-intent finalize", this::finalizeRenameIntentsOnce).run();
        guarded("locator relay sweep", this::sweepStaleLocatorRelaysOnce).run();
    }

    /**
     * A periodic task that throws is silently cancelled by its {@link ScheduledExecutorService}, so
     * every scheduled task (and each maintenance phase, which is what keeps maintenance itself from
     * throwing) logs and swallows its failure instead.
     */
    private Runnable guarded(String what, Runnable task) {
        return () -> {
            try {
                task.run();
            } catch (Throwable t) {
                LOG.error("Background {} failed on node {}", what, nodeId, t);
            }
        };
    }

    private static ScheduledExecutorService daemonScheduler(String name) {
        return Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Readiness: not {@link #close}d (membership was registered at construction) and the
     * coordination session is connected. Deliberately independent of partition ownership, since a
     * fresh node owns none.
     */
    public boolean isReady() {
        return !closed && coordination.isConnected();
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
        createBox(box, partitionCount, false);
    }

    public void createBox(BoxName box, int partitionCount, boolean namespaceManaged) {
        int count = partitionCount > 0 ? partitionCount : config.partitionsPerBoxDefault();
        BoxDescriptor descriptor = new BoxDescriptor(count, java.util.UUID.randomUUID().toString(), namespaceManaged);
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
            cleanupDeletedConsumerClaims(ownership);
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
        if (!boxExists(box)) {
            for (PartitionOwnership ownership : owned) { ownership.retireDeletedAllocations(); }
        }
        aclStore.delete(box.value());
        deleteLocatorRelays(box, descriptor.partitionCount(), owned.size() == descriptor.partitionCount());
        // TODO(phase-3): reference-counted GC of the Box's SSTable/Syrup/WAL/manifest ledgers.
    }

    /** Drops a partition's manifest pointer (so it no longer exists) and closes its engine/lease. */
    private void dropPartition(PartitionOwnership ownership) {
        ownership.currentPointer().ifPresent(p -> {
            try {
                coordination.delete(
                        ownership.pointerKey(),
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

    /**
     * Whether the Box exists in the cluster. Reads coordination directly, not the descriptor cache,
     * because another node may have deleted the Box since it was cached here.
     */
    public boolean boxExists(BoxName box) {
        boolean exists = coordination.get(CandyboxKeys.boxMetaKey(box.value())).isPresent();
        if (!exists) {
            descriptorCache.remove(box.value());
        }
        return exists;
    }

    /** The Box's descriptor, or throws {@link BoxNotFoundException}. Cached (it is immutable). */
    public BoxDescriptor descriptor(BoxName box) {
        return findDescriptor(box).orElseThrow(() -> new BoxNotFoundException(box.value()));
    }

    private Optional<BoxDescriptor> findDescriptor(BoxName box) {
        BoxDescriptor cached = descriptorCache.get(box.value());
        if (cached != null && config.ledgerEnumerationGraceMillis() == 0) {
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
     * if the Box does not exist, or {@link NotOwnerException} if this node does not hold the
     * partition or its lease is no longer valid.
     */
    public BoxEngine engine(BoxName box, String key) {
        return enginePartition(box, descriptor(box).partitionOf(key));
    }

    /** Returns the engine of one partition this node owns (see {@link #engine}). */
    public BoxEngine enginePartition(BoxName box, int partition) {
        PartitionOwnership ownership = partitions.get(new BoxPartition(box.value(), partition));
        if (ownership != null && config.ledgerEnumerationGraceMillis() > 0
                && !descriptor(box).incarnation().equals(ownership.incarnation())) {
            throw new NotOwnerException("Box incarnation changed");
        }
        if (ownership == null) {
            if (!boxExists(box)) {
                throw new BoxNotFoundException(box.value());
            }
            throw new NotOwnerException("box " + box.value() + " partition " + partition
                    + " is not served by node " + nodeId);
        }
        return ownership.engine();
    }

    /**
     * Drops locally owned partitions whose Box descriptor no longer exists — the convergence path of
     * a (force) {@link #deleteBox} issued on another node while this one owned some partitions.
     */
    void sweepDeletedBoxes() {
        for (BoxPartition bp : partitions.keySet()) {
            var descriptor = coordination.get(CandyboxKeys.boxMetaKey(bp.box()));
            PartitionOwnership existing = partitions.get(bp);
            if (descriptor.isEmpty() || (config.ledgerEnumerationGraceMillis() > 0 && existing != null
                    && !BoxDescriptor.decode(descriptor.get().value()).incarnation().equals(existing.incarnation()))) {
                PartitionOwnership ownership = partitions.remove(bp);
                if (ownership != null) {
                    LOG.info("Dropping box {} partition {} on node {}: the Box was deleted",
                            bp.box(), bp.partition(), nodeId);
                    dropPartition(ownership);
                    ownership.retireDeletedAllocations();
                    cleanupDeletedConsumerClaims(ownership);
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

    void renewLeases() {
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
            try {
                if (!ownership.isOwner()) {
                    continue;
                }
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
            } catch (Throwable e) {
                LOG.warn("Compaction error on node {}", nodeId, e);
            }
        }
        return performed;
    }

    /**
     * Runs one GC pass over every partition this node owns, deleting its pending-delete ledgers past
     * the grace period after claiming orphaned Syrups. Exposed for manual/operational triggering.
     *
     * @return the number of ledgers deleted
     */
    public int collectGarbageOnce() {
        reconcileClaimedLocatorRelaysOnce();
        // Publish every owned partition's held-Syrup set first, so this pass's Box-global gate
        // sees up-to-date references from this node's partitions (cross-node freshness rides the other
        // owners' own publishes plus the GC grace period). A Box with a failed publish sits this pass
        // out rather than gate on its stale references.
        Set<String> unpublished = new HashSet<>();
        for (PartitionOwnership ownership : partitions.values()) {
            try {
                if (ownership.isOwner()) {
                    publishPartitionRefs(ownership);
                }
            } catch (Throwable e) {
                unpublished.add(ownership.box().value());
                LOG.warn("Held-Syrup publish error on node {}: {}", nodeId, e.getMessage());
            }
        }
        int deleted = 0;
        // Per pass, each Box's references are read once; an empty Optional means they could not be,
        // and the Box claims no Syrups, only deleting what is already pending.
        Map<String, Optional<BoxSyrupRefs>> boxRefs = new HashMap<>();
        for (PartitionOwnership ownership : partitions.values()) {
            try {
                String box = ownership.box().value();
                if (unpublished.contains(box) || !ownership.isOwner()) {
                    continue;
                }
                Optional<BoxSyrupRefs> refs = boxRefs.computeIfAbsent(box, this::tryReadBoxSyrupRefs);
                deleted += refs.isPresent()
                        ? garbageCollector.collect(ownership.engine(),
                                refs.get().foreignTo(ownership.partition()), refs.get().readAtMillis())
                        : garbageCollector.collectWithoutSyrups(ownership.engine());
            } catch (NotOwnerException lostOwnership) {
                LOG.info("Skipping GC of a partition on node {}: {}", nodeId,
                        lostOwnership.getMessage());
            } catch (Throwable e) {
                LOG.warn("GC error on node {}", nodeId, e);
            }
        }
        return deleted;
    }

    synchronized void rotateAllocationEpochs() {
        long grace = config.ledgerEnumerationGraceMillis();
        if (grace == 0) { return; }
        for (BoxPartition bp : new ArrayList<>(partitions.keySet())) {
            if (!ownsPartition(bp.box(), bp.partition())) { continue; }
            try {
                PartitionOwnership ownership = partitions.get(bp);
                if (ownership == null || !ownership.allocationRotationDue(grace)) { continue; }
                BoxName box = BoxName.of(bp.box());
                releasePartitionForHandover(box, bp.partition());
                openPartition(box, bp.partition());
            } catch (RuntimeException failure) {
                LOG.warn("Allocation epoch rotation failed for {}", bp, failure);
            }
        }
    }

    /** Low-frequency enumeration; only the elected balancer invokes this pass. */
    synchronized void reconcileAllocations() {
        long grace = config.ledgerEnumerationGraceMillis();
        long now = clock.currentTimeMillis();
        long interval = Math.min(grace, 60_000);
        if (grace == 0 || (lastEnumeration != 0 && now - lastEnumeration < interval)) { return; }
        lastEnumeration = now;
        enumerationReport = retention.reconcile(grace, 1024, false);
    }

    public LedgerRetention.Report enumerationReport() { return enumerationReport; }

    public LedgerRetention retention() {
        if (config.ledgerEnumerationGraceMillis() == 0) {
            throw new IllegalStateException("Retention pins require ledger enumeration to be enabled");
        }
        return retention;
    }

    /**
     * Finalizes committed renames using the source intent's expected HLC and acknowledges the
     * durable source delete. Timed-out preparations may be aborted only by winning the pending
     * relay's CAS against destination consumption. Claimed transfers retain their intent until a
     * commit receipt is published or a recovered destination has fenced and aborted the consumer.
     *
     * @return the number of committed intents acknowledged (a rewritten source is retained)
     */
    public int finalizeRenameIntentsOnce() {
        long abandon = config.renameIntentAbandonMillis();
        int finalized = 0;
        for (PartitionOwnership ownership : partitions.values()) {
            BoxEngine engine;
            List<RenameIntent> intents;
            String box = ownership.box().value();
            try {
                if (!ownership.isOwner()) {
                    continue;
                }
                engine = ownership.engine();
                intents = engine.listRenameIntents();
            } catch (NotOwnerException lost) {
                continue;
            } catch (Throwable e) {
                LOG.warn("Rename-intent finalize error on node {}: {}", nodeId, e.getMessage());
                continue;
            }
            for (RenameIntent intent : intents) {
                try {
                    synchronized (engine) {
                        // A fresh Prepare may have refreshed this token since the maintenance snapshot.
                        if (!engine.listRenameIntents().contains(intent)) { continue; }
                        if (finalizeRenameIntent(engine, box, intent.token(), intent.srcKey())) {
                            finalized++;
                        } else if (clock.currentTimeMillis() - intent.createdAtMillis() > abandon
                                && abortUnclaimedRename(box, ownership.partition(), intent.token())) {
                            engine.clearRenameIntent(intent.token());
                        }
                    }
                } catch (NotOwnerException lost) {
                    break;
                } catch (Throwable e) {
                    LOG.warn("Rename-intent finalize error on node {} for token {}: {}", nodeId,
                            intent.token(), e.getMessage());
                }
            }
        }
        return finalized;
    }

    /**
     * Deletes the unclaimed locator relays of every owned partition that no destination claimed within
     * {@code renameIntentAbandonMillis} (the client gave up between resolving the source and the
     * zero-copy put), and recounts the bytes of the ones still pending against the node's relay
     * budget. Exposed for manual/operational triggering and tests.
     *
     * @return the number of relays deleted
     */
    public int sweepStaleLocatorRelaysOnce() {
        int deleted = 0;
        long liveBytes = 0;
        boolean complete = true;
        for (PartitionOwnership ownership : partitions.values()) {
            boolean owner;
            try {
                owner = ownership.isOwner();
            } catch (Throwable e) {
                LOG.warn("Locator relay sweep error on node {}: {}", nodeId, e.getMessage());
                complete = false;
                continue;
            }
            if (owner) {
                RelaySweep sweep = sweepStaleLocatorRelays(ownership.box().value(),
                        ownership.partition());
                deleted += sweep.deleted();
                liveBytes += sweep.liveBytes();
                complete &= sweep.complete();
            }
        }
        if (complete) {
            pendingRelayBytes.set(liveBytes);
        }
        return deleted;
    }

    private record RelaySweep(int deleted, long liveBytes, boolean complete) {
    }

    private RelaySweep sweepStaleLocatorRelays(String box, int partition) {
        long cutoff = clock.currentTimeMillis() - config.renameIntentAbandonMillis();
        int deleted = 0;
        long liveBytes = 0;
        boolean complete = true;
        List<String> tokens;
        try {
            tokens = coordination.children(CandyboxKeys.locatorRelaysPath(box, partition));
        } catch (Throwable e) {
            LOG.warn("Locator relay sweep error on node {} for box {} partition {}: {}", nodeId, box,
                    partition, e.getMessage());
            return new RelaySweep(0, 0, false);
        }
        for (String token : tokens) {
            String key = CandyboxKeys.locatorRelayKey(box, partition, token);
            try {
                Optional<VersionedValue> value = coordination.get(key);
                if (value.isEmpty()) {
                    continue;
                }
                long writtenAt;
                LocatorRelay relay = null;
                try {
                    relay = LocatorRelay.deserialize(value.get().value());
                    writtenAt = relay.createdAtMillis();
                } catch (RuntimeException undecodable) {
                    // Unknown formats retain their full window before reaping.
                    writtenAt = value.get().modifiedAtMillis();
                    if (writtenAt < 0) { throw undecodable; }
                }
                if ((relay == null || relay.consumer() == null) && writtenAt <= cutoff) {
                    coordination.delete(key, value.get().version());
                    if (config.ledgerEnumerationGraceMillis() > 0 && relay != null) { releaseRelayPins(token, relay); }
                    deleted++;
                } else {
                    liveBytes += value.get().value().length;
                }
            } catch (CasConflictException raced) {
                // Consumed or deleted concurrently.
            } catch (Throwable e) {
                complete = false;
                LOG.warn("Locator relay sweep error on node {} for {}: {}", nodeId, key,
                        e.getMessage());
            }
        }
        return new RelaySweep(deleted, liveBytes, complete);
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
            try {
                if (!ownership.isOwner()) {
                    continue;
                }
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
            } catch (Throwable e) {
                LOG.warn("Multipart TTL sweep error on node {}", nodeId, e);
            }
        }
        return aborted;
    }

    // ---- Box-global GC support + cross-partition rename rendezvous ---------------------------

    long currentTimeMillis() {
        return clock.currentTimeMillis();
    }

    /**
     * Publishes a partition's held-Syrup set, or throws: a zero-copy put deletes its relay
     * only after this, so the Box-global GC never loses sight of the Syrups it now references.
     *
     * @throws NotOwnerException if this node no longer owns the partition
     */
    void publishPartitionRefs(BoxName box, int partition) {
        PartitionOwnership ownership = partitions.get(new BoxPartition(box.value(), partition));
        if (ownership == null || !ownership.isOwner()) {
            throw new NotOwnerException(box.value());
        }
        for (int attempt = 0; !tryPublishPartitionRefs(ownership); attempt++) {
            if (attempt >= 3) {
                throw new BusyException("Could not publish held Syrups of box " + box
                        + " partition " + partition);
            }
        }
    }

    /** Publishes one owned partition's held-Syrup set (Box-global GC input), best effort. */
    private void publishPartitionRefs(PartitionOwnership ownership) {
        try {
            tryPublishPartitionRefs(ownership);
        } catch (NotOwnerException lost) {
            // The new owner publishes.
        }
    }

    /**
     * One CAS attempt. The refs are computed only after the current version is read, so a stale set
     * can never overwrite a fresher publish; an unchanged set is not rewritten, which keeps the
     * versions GC compares across its snapshot stable.
     *
     * @return false if a concurrent publish won
     */
    private boolean tryPublishPartitionRefs(PartitionOwnership ownership) {
        String key = CandyboxKeys.partitionRefsKey(ownership.box().value(), ownership.partition());
        Optional<VersionedValue> current = coordination.get(key);
        byte[] refs = encodeLongSet(ownership.engine().heldSyrups(
                clock.currentTimeMillis() - config.ledgerGcGraceMillis()));
        try {
            if (current.isEmpty()) {
                coordination.create(key, refs);
            } else if (!decodeLongSet(current.get().value()).equals(decodeLongSet(refs))) {
                coordination.compareAndSet(key, refs, current.get().version());
            }
            return true;
        } catch (CasConflictException raced) {
            return false;
        }
    }

    private Optional<BoxSyrupRefs> tryReadBoxSyrupRefs(String box) {
        try {
            return Optional.of(readBoxSyrupRefs(box));
        } catch (RuntimeException e) {
            LOG.warn("Skipping Syrup GC of box {} on node {}: {}", box, nodeId, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * The Syrups a Box's partitions reference, as seen by GC: each partition's published refs, plus
     * every Syrup a pending locator relay points at, since a zero-copy put may still commit it.
     * The refs are read both before and after the relays, and the snapshot only counts if no refs
     * changed in between: a relay is only written while its source still references its Syrups, and
     * is only deleted after the destination published its refs, so with no publish between the two
     * reads no reference can fall through. Syrups are then claimed only if orphaned a grace period
     * before {@code readAtMillis}, which relies on every path that re-references a Syrup clearing
     * its pending-orphan stamp (and a handover restarting it). Costs one read per pending relay of
     * the Box, bounded by the relay caps.
     *
     * @throws IllegalStateException if no stable snapshot could be read, or a relay is undecodable
     */
    private BoxSyrupRefs readBoxSyrupRefs(String box) {
        int partitionCount;
        try {
            partitionCount = descriptor(BoxName.of(box)).partitionCount();
        } catch (BoxNotFoundException gone) {
            return new BoxSyrupRefs(List.of(), Set.of(), clock.currentTimeMillis());
        }
        for (int attempt = 0; attempt < MAX_REFS_SNAPSHOT_ATTEMPTS; attempt++) {
            long readAt = clock.currentTimeMillis();
            List<Optional<VersionedValue>> before = publishedPartitionRefs(box, partitionCount);
            Set<Long> relayed = relayedSyrups(box, partitionCount);
            List<Optional<VersionedValue>> after = publishedPartitionRefs(box, partitionCount);
            if (versions(before).equals(versions(after))) {
                List<Set<Long>> published = new ArrayList<>(partitionCount);
                for (Optional<VersionedValue> refs : before) {
                    published.add(refs.map(v -> decodeLongSet(v.value())).orElseGet(Set::of));
                }
                return new BoxSyrupRefs(published, relayed, readAt);
            }
        }
        throw new IllegalStateException("Referenced Syrups kept changing while being read");
    }

    private List<Optional<VersionedValue>> publishedPartitionRefs(String box, int partitionCount) {
        List<Optional<VersionedValue>> refs = new ArrayList<>(partitionCount);
        for (int p = 0; p < partitionCount; p++) {
            refs.add(coordination.get(CandyboxKeys.partitionRefsKey(box, p)));
        }
        return refs;
    }

    private static List<Long> versions(List<Optional<VersionedValue>> refs) {
        return refs.stream().map(v -> v.map(VersionedValue::version).orElse(-1L)).toList();
    }

    private Set<Long> relayedSyrups(String box, int partitionCount) {
        Set<Long> relayed = new HashSet<>();
        for (int p = 0; p < partitionCount; p++) {
            for (String token : coordination.children(CandyboxKeys.locatorRelaysPath(box, p))) {
                String key = CandyboxKeys.locatorRelayKey(box, p, token);
                Optional<VersionedValue> value = coordination.get(key);
                if (value.isEmpty()) {
                    continue;
                }
                LocatorRelay relay;
                try {
                    relay = LocatorRelay.deserialize(value.get().value());
                } catch (RuntimeException e) {
                    throw new IllegalStateException("Undecodable locator relay " + key, e);
                }
                for (SegmentRef seg : relay.locator().segments()) {
                    relayed.add(seg.syrupId());
                }
            }
        }
        return relayed;
    }

    private record BoxSyrupRefs(List<Set<Long>> publishedByPartition, Set<Long> relayed,
                                long readAtMillis) {

        /** The Syrups GC of {@code partition} must keep: other partitions' refs and all relays. */
        Set<Long> foreignTo(int partition) {
            Set<Long> referenced = new HashSet<>(relayed);
            for (int p = 0; p < publishedByPartition.size(); p++) {
                if (p != partition) {
                    referenced.addAll(publishedByPartition.get(p));
                }
            }
            return referenced;
        }
    }

    /** Called under the destination engine lock, serializing with receipt removal. */
    void requirePendingRename(String box, int srcPartition, String relayToken) {
        var relayValue = coordination.get(CandyboxKeys.locatorRelayKey(box, srcPartition, relayToken));
        if (relayValue.isEmpty()) { throw new BusyException("Rename attempt already retired"); }
        var relay = LocatorRelay.deserialize(relayValue.get().value());
        String key = CandyboxKeys.renameMarkerKey(box, relay.renameToken());
        for (int attempt = 0; attempt < 4; attempt++) {
            var decision = coordination.get(key);
            if (decision.isEmpty()) { throw new BusyException("Rename decision already retired"); }
            var marker = RenameMarker.deserialize(decision.get().value());
            if (marker.completed()) { throw new BusyException("Rename decision already acknowledged"); }
            try {
                // Refresh even an existing reservation: a newer generation must invalidate
                // a fenced claimant's captured version before it appends another receipt.
                coordination.compareAndSet(key, marker.reserve().serialize(), decision.get().version());
                return;
            } catch (CasConflictException raced) { /* retry decision; never append after abort */ }
        }
        throw new BusyException("Rename reservation raced repeatedly");
    }

    /** Only preparation may create a decision. Publication can never recreate a cleaned token. */
    void prepareRenameMarker(String box, String token, String srcKey, int srcPartition, Hlc srcHlc,
                             String dstKey) {
        var prepared = new RenameMarker(descriptor(BoxName.of(box)).incarnation(), srcKey,
                srcPartition, srcHlc, dstKey, false, false);
        String key = CandyboxKeys.renameMarkerKey(box, token);
        try { coordination.create(key, prepared.serialize()); }
        catch (CasConflictException exists) {
            var current = RenameMarker.deserialize(coordination.get(key).orElseThrow().value());
            if (!current.commit().acknowledge().equals(prepared.commit().acknowledge())) {
                throw new IllegalStateException("Conflicting rename preparation " + token);
            }
        }
    }

    void publishRenameMarker(String box, String token, String srcKey, int srcPartition, Hlc srcHlc,
                             String dstKey, String incarnation) {
        var expected = new RenameMarker(incarnation, srcKey, srcPartition, srcHlc, dstKey, false);
        String key = CandyboxKeys.renameMarkerKey(box, token);
        for (int attempt = 0; attempt < 4; attempt++) {
            var value = coordination.get(key);
            if (value.isEmpty()) { return; } // terminal cleanup won; never create here
            var current = RenameMarker.deserialize(value.get().value());
            if (!current.commit().acknowledge().equals(expected.acknowledge())) {
                throw new IllegalStateException("Conflicting rename decision " + token);
            }
            if (current.committed()) { return; }
            try {
                coordination.compareAndSet(key, current.commit().serialize(), value.get().version());
                return;
            } catch (CasConflictException raced) { /* ACK or terminal cleanup won; reread. */ }
        }
        throw new BusyException("Rename publication raced repeatedly");
    }

    /**
     * Writes the rendezvous marker telling the source partition's owner that a cross-partition
     * rename's destination put is durable. Idempotent (a retried write is a no-op).
     */
    void writeRenameMarker(String box, String token, String srcKey, int srcPartition, Hlc srcHlc,
                           String dstKey) {
        writeRenameMarker(box, token, srcKey, srcPartition, srcHlc, dstKey,
                descriptor(BoxName.of(box)).incarnation());
    }

    void writeRenameMarker(String box, String token, String srcKey, int srcPartition, Hlc srcHlc,
                           String dstKey, String incarnation) {
        if (!descriptor(BoxName.of(box)).incarnation().equals(incarnation)) {
            throw new NotOwnerException("Rename belongs to a deleted Box incarnation");
        }
        var marker = new RenameMarker(incarnation, srcKey,
                srcPartition, srcHlc, dstKey, false);
        String key = CandyboxKeys.renameMarkerKey(box, token);
        try {
            coordination.create(key, marker.serialize());
        } catch (CasConflictException alreadyThere) {
            var value = coordination.get(key).orElseThrow();
            var existing = RenameMarker.deserialize(value.value());
            if (existing.incarnation().isEmpty() && existing.srcKey().equals(srcKey)
                    && existing.srcPartition() == srcPartition && existing.srcHlc().equals(srcHlc)
                    && existing.dstKey().equals(dstKey)) {
                coordination.compareAndSet(key, (existing.completed() ? marker.acknowledge() : marker).serialize(),
                        value.version());
                return;
            }
            if (!existing.acknowledge().equals(marker.acknowledge())) {
                throw new IllegalStateException("Conflicting rename decision " + token);
            }
        }
    }

    /** Uses only the source's durable expected HLC; a client cannot choose the deleted version. */
    boolean finalizeRenameIntent(BoxEngine engine, String box, String token, String srcKey) {
        synchronized (engine) {
            return finalizeRenameIntentLocked(engine, box, token, srcKey);
        }
    }

    private boolean finalizeRenameIntentLocked(BoxEngine engine, String box, String token, String srcKey) {
        var intent = engine.listRenameIntents().stream().filter(i -> i.token().equals(token))
                .findFirst().orElse(null);
        if (intent == null || !intent.srcKey().equals(srcKey)) { return false; }
        String key = CandyboxKeys.renameMarkerKey(box, token);
        var value = coordination.get(key);
        if (value.isEmpty()) { return false; }
        var marker = RenameMarker.deserialize(value.get().value());
        if ((!marker.incarnation().isEmpty() && !marker.incarnation().equals(descriptor(BoxName.of(box)).incarnation()))
                || marker.srcPartition() != descriptor(BoxName.of(box)).partitionOf(intent.srcKey())
                || !marker.srcKey().equals(intent.srcKey()) || !marker.srcHlc().equals(intent.srcHlc())
                || !marker.dstKey().equals(intent.dstKey())
                || descriptor(BoxName.of(box)).partitionOf(marker.dstKey()) != intent.dstPartition()) {
            throw new IllegalStateException("Rename marker does not match source intent " + token);
        }
        if (!marker.committed()) { return false; }
        engine.deleteCandyConditional(CandyKey.of(intent.srcKey()), intent.srcHlc());
        // ACK follows the durable tombstone, precedes intent removal. Recovery can repeat both.
        if (!marker.completed()) {
            coordination.compareAndSet(key, marker.acknowledge().serialize(), value.get().version());
        }
        engine.clearRenameIntent(token);
        // Legacy/manual markers without a transfer need no destination receipt cleanup.
        if (!hasRenameRelay(box, marker.srcPartition(), token)) { deleteRenameMarker(box, token); }
        return true;
    }

    private boolean hasRenameRelay(String box, int partition, String renameToken) {
        for (String token : coordination.children(CandyboxKeys.locatorRelaysPath(box, partition))) {
            var value = coordination.get(CandyboxKeys.locatorRelayKey(box, partition, token));
            if (value.isPresent() && renameToken.equals(LocatorRelay.deserialize(value.get().value()).renameToken())) {
                return true;
            }
        }
        return false;
    }

    /** Pending-to-aborted races the destination's pending-to-claimed CAS on the same record. */
    private boolean abortUnclaimedRename(String box, int partition, String renameToken) {
        String markerKey = CandyboxKeys.renameMarkerKey(box, renameToken);
        var decision = coordination.get(markerKey);
        if (decision.isPresent()) {
            var marker = RenameMarker.deserialize(decision.get().value());
            if (marker.reserved() || marker.committed()) { return false; }
        }
        for (String token : coordination.children(CandyboxKeys.locatorRelaysPath(box, partition))) {
            var value = coordination.get(CandyboxKeys.locatorRelayKey(box, partition, token));
            if (value.isEmpty()) { continue; }
            var relay = LocatorRelay.deserialize(value.get().value());
            if (!renameToken.equals(relay.renameToken())) { continue; }
            if (relay.consumer() != null) { return false; }
            coordination.delete(CandyboxKeys.locatorRelayKey(box, partition, token), value.get().version());
            if (config.ledgerEnumerationGraceMillis() > 0) { releaseRelayPins(token, relay); }
        }
        if (decision.isPresent()) { coordination.delete(markerKey, decision.get().version()); }
        return true;
    }

    /**
     * Stashes a source locator for a cross-partition zero-copy put and returns its relay token. The
     * pending relays a node writes are bounded in bytes ({@code zeroCopyRelayBudgetBytes}) and per
     * partition in count, stale ones being reaped first, so clients cannot grow the coordination
     * store without bound. The byte count is an upper bound: relays consumed by other nodes are
     * only subtracted when the next sweep recounts.
     */
    String writeLocatorRelay(String box, int srcPartition, LocatorRelay relay) {
        byte[] value = relay.serialize(config.sizeLimits().maxLocatorBytes());
        long budget = config.zeroCopyRelayBudgetBytes();
        if (pendingRelayBytes.get() + value.length > budget) {
            if (tryStartRelayRecount()) {
                sweepStaleLocatorRelaysOnce();
            }
            if (pendingRelayBytes.get() + value.length > budget) {
                throw new BusyException("Zero-copy relay budget exhausted on node " + nodeId);
            }
        }
        String relays = CandyboxKeys.locatorRelaysPath(box, srcPartition);
        if (coordination.children(relays).size() >= MAX_PENDING_LOCATOR_RELAYS
                && (!tryStartRelayRecount()
                        || sweepStaleLocatorRelays(box, srcPartition).deleted() == 0
                        || coordination.children(relays).size() >= MAX_PENDING_LOCATOR_RELAYS)) {
            throw new BusyException("Too many pending zero-copy relays for box " + box
                    + " partition " + srcPartition);
        }
        String token = UUID.randomUUID().toString();
        if (config.ledgerEnumerationGraceMillis() > 0) {
            byte[] epoch = enginePartition(BoxName.of(box), srcPartition)
                    .sstableLedgerConfig().customMetadata().get(LedgerRetention.EPOCH_TAG);
            retention.pinRelay(new String(epoch, StandardCharsets.UTF_8), token,
                    relay.locator().segments().stream().map(SegmentRef::syrupId).toList());
        }
        coordination.create(CandyboxKeys.locatorRelayKey(box, srcPartition, token), value);
        pendingRelayBytes.addAndGet(value.length);
        return token;
    }

    /**
     * Whether an over-budget write may reap and recount now. At most one such pass runs per
     * {@link #RELAY_RECOUNT_INTERVAL_MILLIS}; the others are refused without touching coordination.
     */
    private boolean tryStartRelayRecount() {
        long now = clock.currentTimeMillis();
        long last = lastRelayRecountMillis.get();
        return now - last >= RELAY_RECOUNT_INTERVAL_MILLIS
                && lastRelayRecountMillis.compareAndSet(last, now);
    }

    /**
     * Claims a fresh, acceptable pending relay for exactly one request. The CAS serializes against
     * expiry: once claimed, its pin cannot expire even if this process pauses before the WAL append.
     * The captured engine and ownership token must agree; recovery fences that engine before it can
     * clean a claim from an older ownership generation.
     */
    Optional<LocatorRelay> claimLocatorRelay(String box, int srcPartition, String token,
                                             String dstKey, BoxEngine engine,
                                             Predicate<LocatorRelay> acceptable) {
        int dstPartition = descriptor(BoxName.of(box)).partitionOf(dstKey);
        PartitionOwnership ownership = partitions.get(new BoxPartition(box, dstPartition));
        if (ownership == null || ownership.engine() != engine) {
            throw new NotOwnerException(box);
        }
        String key = CandyboxKeys.locatorRelayKey(box, srcPartition, token);
        var value = coordination.get(key);
        if (value.isEmpty()) { return Optional.empty(); }
        LocatorRelay relay = LocatorRelay.deserialize(value.get().value());
        if (!acceptable.test(relay)) { return Optional.empty(); }
        if (relay.consumer() != null) {
            // A committed retry only publishes refs/markers; it must never append another locator.
            return relay.consumer().committed() != null && relay.consumer().key().equals(dstKey)
                    && relay.consumer().incarnation().equals(ownership.incarnation())
                    ? Optional.of(relay) : Optional.empty();
        }
        if (clock.currentTimeMillis() - relay.createdAtMillis()
                > config.renameIntentAbandonMillis() / 2) {
            return Optional.empty();
        }
        LocatorRelay claimed = relay.claim(dstPartition, ownership.fencingToken(), dstKey,
                ownership.incarnation());
        try {
            coordination.compareAndSet(key, claimed.serialize(config.sizeLimits().maxLocatorBytes()),
                    value.get().version());
            return Optional.of(claimed);
        } catch (CasConflictException expiredOrClaimed) {
            return Optional.empty();
        }
    }

    LocatorRelay recordCommittedLocatorRelay(String box, int srcPartition, String token,
                                             LocatorRelay claim, CandyMetadata metadata) {
        String key = CandyboxKeys.locatorRelayKey(box, srcPartition, token);
        var value = coordination.get(key);
        if (value.isEmpty() || !LocatorRelay.deserialize(value.get().value()).equals(claim)) {
            throw new NotOwnerException("Locator relay was reconciled by a newer owner");
        }
        LocatorRelay committed = claim.committed(metadata);
        coordination.compareAndSet(key, committed.serialize(config.sizeLimits().maxLocatorBytes()),
                value.get().version());
        return committed;
    }

    void deleteLocatorRelay(String box, int srcPartition, String token) {
        deleteLocatorRelay(box, srcPartition, token, null, false);
    }

    void completeLocatorRelay(String box, int srcPartition, String token, LocatorRelay claim) {
        deleteLocatorRelay(box, srcPartition, token, claim, false);
    }

    private void deleteLocatorRelay(String box, int srcPartition, String token,
                                    LocatorRelay expectedClaim, boolean includeClaimed) {
        try {
            String key = CandyboxKeys.locatorRelayKey(box, srcPartition, token);
            var value = coordination.get(key);
            if (value.isEmpty()) { return; }
            LocatorRelay relay = LocatorRelay.deserialize(value.get().value());
            if (!includeClaimed && (expectedClaim == null ? relay.consumer() != null
                    : !expectedClaim.equals(relay))) { return; }
            coordination.delete(key, value.get().version());
            if (config.ledgerEnumerationGraceMillis() > 0) {
                releaseRelayPins(token, relay);
            }
        } catch (CasConflictException raced) {
            // Already consumed by a concurrent retry, or reaped.
        }
    }

    /**
     * A newer destination owner has recover-opened the prior WAL. It publishes the recovered refs
     * before dropping any older consumer claim. An old consumer can no longer append, and an append
     * that made it to the WAL is now protected by those refs. Failed/uncertain coordination keeps the
     * pin for a later pass. Same-generation failures deliberately keep their claims until handover.
     */
    public int reconcileClaimedLocatorRelaysOnce() {
        int reconciled = reconcileRenameReceiptsOnce();
        Set<String> boxes = new HashSet<>();
        for (BoxPartition bp : partitions.keySet()) { boxes.add(bp.box()); }
        for (String box : boxes) {
            try {
                int count = descriptor(BoxName.of(box)).partitionCount();
                for (int src = 0; src < count; src++) {
                    for (String token : coordination.children(CandyboxKeys.locatorRelaysPath(box, src))) {
                        var value = coordination.get(CandyboxKeys.locatorRelayKey(box, src, token));
                        if (value.isEmpty()) { continue; }
                        LocatorRelay relay = LocatorRelay.deserialize(value.get().value());
                        if (relay.consumer() == null) { continue; }
                        PartitionOwnership destination = partitions.get(
                                new BoxPartition(box, relay.consumer().partition()));
                        if (destination == null || !destination.isOwner()
                                || !destination.incarnation().equals(relay.consumer().incarnation())) {
                            continue;
                        }
                        synchronized (destination.engine()) {
                            boolean acknowledged = false;
                            String markerKey = null;
                            Optional<VersionedValue> decision = Optional.empty();
                            RenameMarker marker = null;
                            if (relay.isRename()) {
                                // Admission shares this monitor, so a receipt cannot appear between
                                // this absence check and the versioned release of the reservation.
                                boolean committed = destination.engine().listRenameReceipts().stream()
                                        .anyMatch(r -> r.token().equals(relay.renameToken()));
                                if (committed) { continue; }
                                markerKey = CandyboxKeys.renameMarkerKey(box, relay.renameToken());
                                decision = coordination.get(markerKey);
                                if (decision.isPresent()) {
                                    marker = RenameMarker.deserialize(decision.get().value());
                                    acknowledged = marker.completed();
                                    if (marker.committed() && !acknowledged) { continue; }
                                }
                            }
                            if (!acknowledged && destination.fencingToken() <= relay.consumer().fencingToken()) {
                                continue; // no commit/abort proof until this consumer is fenced
                            }
                            if (tryPublishPartitionRefs(destination)) {
                                if (marker != null && !marker.committed()) {
                                    // Retire only this captured reservation, never the logical token.
                                    // Fresh admission always CAS-refreshes it, including after handover.
                                    var prepared = new RenameMarker(marker.incarnation(), marker.srcKey(),
                                            marker.srcPartition(), marker.srcHlc(), marker.dstKey(), false, false);
                                    coordination.compareAndSet(markerKey, prepared.serialize(), decision.get().version());
                                }
                                completeLocatorRelay(box, src, token, relay);
                                reconciled++;
                            }
                        }
                    }
                }
            } catch (RuntimeException failure) {
                LOG.warn("Claimed relay reconciliation failed for box {}: {}", box,
                        failure.getMessage());
            }
        }
        cleanupAcknowledgedRenameMarkers();
        return reconciled;
    }

    /** Completes cleanup if the previous pass crashed after deleting its relay. */
    private void cleanupAcknowledgedRenameMarkers() {
        Set<String> boxes = new HashSet<>();
        for (BoxPartition bp : partitions.keySet()) { boxes.add(bp.box()); }
        for (String box : boxes) {
            try {
                for (String token : coordination.children(CandyboxKeys.BOXES_ROOT + "/" + box + "/renames")) {
                    String key = CandyboxKeys.renameMarkerKey(box, token);
                    var value = coordination.get(key);
                    if (value.isEmpty()) { continue; }
                    var marker = RenameMarker.deserialize(value.get().value());
                    var source = partitions.get(new BoxPartition(box, marker.srcPartition()));
                    if (!marker.committed() && source != null && source.isOwner()
                            && source.incarnation().equals(marker.incarnation())
                            && value.get().modifiedAtMillis() >= 0
                            && clock.currentTimeMillis() - value.get().modifiedAtMillis() > config.renameIntentAbandonMillis()
                            && source.engine().listRenameIntents().stream().noneMatch(i -> i.token().equals(token))
                            && !hasRenameRelay(box, marker.srcPartition(), token)) {
                        coordination.delete(key, value.get().version());
                        continue;
                    }
                    var destination = partitions.get(new BoxPartition(box,
                            descriptor(BoxName.of(box)).partitionOf(marker.dstKey())));
                    if (marker.completed() && destination != null && destination.isOwner()
                            && destination.incarnation().equals(marker.incarnation())
                            && destination.engine().listRenameReceipts().stream().noneMatch(r -> r.token().equals(token))
                            && !hasRenameRelay(box, marker.srcPartition(), token)) {
                        coordination.delete(key, value.get().version());
                    }
                }
            } catch (RuntimeException failure) {
                LOG.warn("Acknowledged rename cleanup failed for box {}: {}", box, failure.getMessage());
            }
        }
    }

    /** Replays destination decisions without depending on a client or a marker having survived. */
    public int reconcileRenameReceiptsOnce() {
        int reconciled = 0;
        for (PartitionOwnership ownership : partitions.values()) {
            try {
                if (!ownership.isOwner()) { continue; }
                var engine = ownership.engine();
                String box = ownership.box().value();
                for (RenameReceipt receipt : engine.listRenameReceipts()) {
                    if (!ownership.incarnation().equals(receipt.incarnation())) {
                        throw new IllegalStateException("Receipt from a different Box incarnation");
                    }
                    if (!tryPublishPartitionRefs(ownership)) { continue; }
                    publishRenameMarker(box, receipt.token(), receipt.srcKey(), receipt.srcPartition(),
                            receipt.srcHlc(), receipt.dstKey(), receipt.incarnation());
                    var value = coordination.get(CandyboxKeys.renameMarkerKey(box, receipt.token()));
                    if (value.isEmpty() || !RenameMarker.deserialize(value.get().value()).completed()) { continue; }
                    engine.clearRenameReceipt(receipt.token());
                    // Every retry can have its own claim. ACK authorizes releasing all attempts.
                    for (String relayToken : coordination.children(CandyboxKeys.locatorRelaysPath(box, receipt.srcPartition()))) {
                        var relayValue = coordination.get(CandyboxKeys.locatorRelayKey(box, receipt.srcPartition(), relayToken));
                        if (relayValue.isEmpty()) { continue; }
                        var relay = LocatorRelay.deserialize(relayValue.get().value());
                        if (receipt.token().equals(relay.renameToken())) {
                            completeLocatorRelay(box, receipt.srcPartition(), relayToken, relay);
                        }
                    }
                    deleteRenameMarker(box, receipt.token());
                    reconciled++;
                }
            } catch (RuntimeException failure) {
                LOG.warn("Rename receipt reconciliation failed on node {}: {}", nodeId, failure.getMessage());
            }
        }
        return reconciled;
    }

    /** A closed destination can release its deleted incarnation's claims without a live descriptor. */
    private void cleanupDeletedConsumerClaims(PartitionOwnership closedOwnership) {
        String box = closedOwnership.box().value();
        try {
            var descriptor = coordination.get(CandyboxKeys.boxMetaKey(box));
            if (descriptor.isPresent() && BoxDescriptor.decode(descriptor.get().value()).incarnation()
                    .equals(closedOwnership.incarnation())) { return; }
            for (String source : coordination.children(CandyboxKeys.BOXES_ROOT + "/" + box + "/partitions")) {
                int partition = Integer.parseInt(source);
                for (String token : coordination.children(CandyboxKeys.locatorRelaysPath(box, partition))) {
                    var value = coordination.get(CandyboxKeys.locatorRelayKey(box, partition, token));
                    if (value.isEmpty()) { continue; }
                    LocatorRelay relay = LocatorRelay.deserialize(value.get().value());
                    var consumer = relay.consumer();
                    if (consumer != null && consumer.partition() == closedOwnership.partition()
                            && consumer.incarnation().equals(closedOwnership.incarnation())
                            && consumer.fencingToken() <= closedOwnership.fencingToken()) {
                        completeLocatorRelay(box, partition, token, relay);
                    }
                }
            }
        } catch (RuntimeException failure) {
            LOG.warn("Deleted consumer claim cleanup failed for box {}: {}", box, failure.getMessage());
        }
    }

    private void releaseRelayPins(String token, LocatorRelay relay) {
        retention.releaseRelay(token,
                relay.locator().segments().stream().map(SegmentRef::syrupId).toList());
    }

    /** Claims survive force deletion when a remote consumer has not yet been fenced. */
    private void deleteLocatorRelays(BoxName box, int partitionCount, boolean includeClaimed) {
        for (int p = 0; p < partitionCount; p++) {
            try {
                for (String token : coordination.children(
                        CandyboxKeys.locatorRelaysPath(box.value(), p))) {
                    deleteLocatorRelay(box.value(), p, token, null, includeClaimed);
                }
            } catch (RuntimeException e) {
                LOG.warn("Failed to delete locator relays of box {} partition {}: {}", box, p,
                        e.getMessage());
            }
        }
    }

    boolean renameMarkerPresent(String box, String token) {
        return coordination.get(CandyboxKeys.renameMarkerKey(box, token)).isPresent();
    }

    void deleteRenameMarker(String box, String token) {
        try {
            CoordinationCas.deleteIfPresent(coordination, CandyboxKeys.renameMarkerKey(box, token), 0);
        } catch (CasConflictException raced) {
            // Already deleted (or rewritten); nothing to do.
        }
    }

    private static byte[] encodeLongSet(Set<Long> values) {
        BinaryWriter w = new BinaryWriter(Math.max(16, values.size() * 2));
        w.writeVarInt(values.size());
        for (long v : values) {
            w.writeVarLong(v);
        }
        return w.toByteArray();
    }

    private static Set<Long> decodeLongSet(byte[] data) {
        BinaryReader r = new BinaryReader(data);
        int count = r.readVarInt();
        Set<Long> values = new HashSet<>(Math.max(4, count * 2));
        for (int i = 0; i < count; i++) {
            values.add(r.readVarLong());
        }
        return values;
    }

    @Override
    public void close() {
        closed = true;
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
            cleanupDeletedConsumerClaims(ownership);
        }
        partitions.clear();
        coordination.unregisterMember(nodeId);
    }
}
