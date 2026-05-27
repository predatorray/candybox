package me.predatorray.candybox.server;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.BoxNotEmptyException;
import me.predatorray.candybox.common.exception.BoxNotFoundException;
import me.predatorray.candybox.common.exception.FencedException;
import me.predatorray.candybox.common.exception.NotOwnerException;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.VersionedValue;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Candybox storage node. It owns a set of Boxes under fenced ZooKeeper leases (via
 * {@link BoxOwnership}), serving each from its local {@link BoxEngine}. {@link #createBox} takes
 * ownership of a brand-new Box; {@link #openBox} takes over an existing Box (the failover/handover
 * path), recovering its manifest and WAL. A background heartbeat renews the leases.
 *
 * <p>Still scaffolded for later: request <em>routing</em> across the cluster (a non-owner currently
 * surfaces {@link me.predatorray.candybox.common.exception.NotOwnerException}; WS5 turns that into a
 * {@code MOVED} response), and background compaction/GC workers (Phase 3).
 */
public final class CandyboxNode implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CandyboxNode.class);

    private final int nodeId;
    private final CandyboxConfig config;
    private final LedgerStore ledgerStore;
    private final CoordinationService coordination;
    private final Clock clock;
    private final ConcurrentMap<String, BoxOwnership> boxes = new ConcurrentHashMap<>();
    private final ScheduledExecutorService leaseHeartbeat;
    private final ScheduledExecutorService compactionWorker;
    private final CompactionService compactionService;
    private final GarbageCollector garbageCollector;

    /** Bounded compaction passes per Box per worker tick, so one Box cannot starve the others. */
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
        coordination.registerMember(nodeId, advertisedAddress.getBytes(StandardCharsets.UTF_8));
        this.compactionService = new CompactionService(ledgerStore, config, clock);
        this.garbageCollector = new GarbageCollector(ledgerStore, config.ledgerGcGraceMillis(), clock);

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
    }

    /** One background maintenance tick: compact owned Boxes, then GC their obsoleted ledgers. */
    private void runMaintenance() {
        compactOwnedBoxesOnce();
        collectGarbageOnce();
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

    /** Creates and takes ownership of a brand-new Box on this node. */
    public void createBox(BoxName box) {
        boxes.compute(box.value(), (name, existing) -> {
            if (existing != null && existing.isOwner()) {
                throw new me.predatorray.candybox.common.exception.BoxAlreadyExistsException(name);
            }
            LOG.info("Creating box {} on node {}", name, nodeId);
            return BoxOwnership.createNew(box, config, ledgerStore, coordination, nodeId, clock);
        });
    }

    /**
     * Takes over ownership of an existing Box (failover/handover): acquires the lease, recovers the
     * manifest + WAL from the current pointer, and advances the pointer.
     */
    public void openBox(BoxName box) {
        boxes.compute(box.value(), (name, existing) -> {
            if (existing != null && existing.isOwner()) {
                return existing; // already owned here
            }
            LOG.info("Opening (taking over) box {} on node {}", name, nodeId);
            return BoxOwnership.recover(box, config, ledgerStore, coordination, nodeId, clock);
        });
    }

    /** Relinquishes ownership of a Box (releases the lease, closes the engine); does not delete data. */
    public void releaseBox(BoxName box) {
        BoxOwnership ownership = boxes.remove(box.value());
        if (ownership != null) {
            ownership.close();
        }
    }

    /** Deletes a Box. Requires this node to own it and (unless {@code force}) for it to be empty. */
    public void deleteBox(BoxName box, boolean force) {
        BoxOwnership ownership = require(box);
        if (!force && !ownership.engine().listCandies("", null, 1).entries().isEmpty()) {
            throw new BoxNotEmptyException(box.value());
        }
        boxes.remove(box.value());
        // Drop the manifest pointer (CAS on its version) so the Box no longer exists, then release.
        ownership.currentPointer().ifPresent(p -> {
            try {
                coordination.delete(BoxOwnership.manifestKey(box), p.version());
            } catch (RuntimeException e) {
                LOG.warn("Failed to delete manifest pointer for box {}: {}", box, e.getMessage());
            }
        });
        ownership.close();
        // TODO(phase-3): reference-counted GC of the Box's SSTable/Syrup/WAL/manifest ledgers.
    }

    public List<String> listBoxes() {
        List<String> names = new ArrayList<>(boxes.keySet());
        names.sort(String::compareTo);
        return names;
    }

    public boolean boxExists(BoxName box) {
        BoxOwnership o = boxes.get(box.value());
        return o != null && o.isOwner();
    }

    /** The node id currently owning {@code box} (from the coordination lease), if any. */
    public java.util.Optional<Integer> currentOwner(BoxName box) {
        return coordination.leaseHolder(BoxOwnership.ownerResource(box))
                .map(me.predatorray.candybox.coordination.LeaseInfo::ownerNodeId);
    }

    /**
     * Returns the engine for a Box this node owns. Throws {@link BoxNotFoundException} if this node has
     * no ownership object for the Box, or {@link me.predatorray.candybox.common.exception.NotOwnerException}
     * if its lease is no longer valid.
     */
    public BoxEngine engine(BoxName box) {
        return require(box).engine();
    }

    private BoxOwnership require(BoxName box) {
        BoxOwnership ownership = boxes.get(box.value());
        if (ownership == null) {
            throw new BoxNotFoundException(box.value());
        }
        return ownership;
    }

    CandyboxConfig config() {
        return config;
    }

    /** A request handler that serves this node's Boxes; wire it into a Transport server or loopback. */
    public RequestHandler requestHandler() {
        return new NodeRequestHandler(this);
    }

    private void renewLeases() {
        for (BoxOwnership ownership : boxes.values()) {
            try {
                ownership.renew();
            } catch (RuntimeException e) {
                LOG.warn("Lease renewal error for a box on node {}", nodeId, e);
            }
        }
    }

    /**
     * Runs one bounded round of compaction over every Box this node still owns. The commit is gated on
     * the owner's fencing token by the manifest, so a Box whose ownership was lost mid-round fails the
     * commit ({@link FencedException}) and is simply skipped — a zombie owner cannot corrupt state.
     *
     * <p>Package-visible so tests can drive it deterministically without the background scheduler.
     *
     * @return the number of compactions performed across all Boxes
     */
    int compactOwnedBoxesOnce() {
        int performed = 0;
        for (BoxOwnership ownership : boxes.values()) {
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
                LOG.info("Stopping compaction of a box on node {}: {}", nodeId,
                        lostOwnership.getMessage());
            } catch (RuntimeException e) {
                LOG.warn("Compaction error on node {}", nodeId, e);
            }
        }
        return performed;
    }

    /**
     * Runs one GC pass over every Box this node owns, deleting SSTable ledgers obsoleted by committed
     * compactions past the grace period. Package-visible for deterministic tests.
     *
     * @return the number of ledgers deleted
     */
    int collectGarbageOnce() {
        int deleted = 0;
        for (BoxOwnership ownership : boxes.values()) {
            if (!ownership.isOwner()) {
                continue;
            }
            try {
                deleted += garbageCollector.collect(ownership.engine());
            } catch (NotOwnerException lostOwnership) {
                LOG.info("Skipping GC of a box on node {}: {}", nodeId, lostOwnership.getMessage());
            } catch (RuntimeException e) {
                LOG.warn("GC error on node {}", nodeId, e);
            }
        }
        return deleted;
    }

    @Override
    public void close() {
        if (leaseHeartbeat != null) {
            leaseHeartbeat.shutdownNow();
        }
        if (compactionWorker != null) {
            compactionWorker.shutdownNow();
        }
        for (BoxOwnership ownership : boxes.values()) {
            ownership.close();
        }
        boxes.clear();
        coordination.unregisterMember(nodeId);
    }
}
