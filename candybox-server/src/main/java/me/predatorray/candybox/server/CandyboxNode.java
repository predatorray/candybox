package me.predatorray.candybox.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.SystemClock;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.BoxAlreadyExistsException;
import me.predatorray.candybox.common.exception.BoxNotEmptyException;
import me.predatorray.candybox.common.exception.BoxNotFoundException;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.lsm.engine.BoxEngine;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Candybox storage node. In this run it wires the LSM {@link BoxEngine} behind the protocol handler
 * so the client can drive real put/get/delete/list over a {@link me.predatorray.candybox.protocol.transport.Transport};
 * each Box it owns gets its own engine.
 *
 * <p>Scaffolded for later phases (see TODOs): ZooKeeper-leased Box ownership and request routing
 * across the cluster (Phase 2), and background compaction/GC workers (Phase 3). For now a node owns
 * every Box created against it and serves it locally.
 */
public final class CandyboxNode implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CandyboxNode.class);

    private final int nodeId;
    private final CandyboxConfig config;
    private final LedgerStore ledgerStore;
    private final CoordinationService coordination;
    private final Clock clock;
    private final ConcurrentMap<String, BoxEngine> engines = new ConcurrentHashMap<>();

    public CandyboxNode(int nodeId, CandyboxConfig config, LedgerStore ledgerStore,
                        CoordinationService coordination) {
        this(nodeId, config, ledgerStore, coordination, SystemClock.INSTANCE);
    }

    public CandyboxNode(int nodeId, CandyboxConfig config, LedgerStore ledgerStore,
                        CoordinationService coordination, Clock clock) {
        this.nodeId = nodeId;
        this.config = config;
        this.ledgerStore = ledgerStore;
        this.coordination = coordination;
        this.clock = clock;
        coordination.registerMember(nodeId, ("node-" + nodeId).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        // TODO(phase-2): acquire per-Box ZK ownership leases and route requests to the owning node.
    }

    public int nodeId() {
        return nodeId;
    }

    /** Creates a Box and its engine on this node. */
    public void createBox(BoxName box) {
        engines.compute(box.value(), (name, existing) -> {
            if (existing != null) {
                throw new BoxAlreadyExistsException(name);
            }
            LOG.info("Creating box {} on node {}", name, nodeId);
            return BoxEngine.createNew(box, config, ledgerStore, nodeId, clock);
        });
    }

    /** Deletes a Box. Requires it to be empty unless {@code force}. */
    public void deleteBox(BoxName box, boolean force) {
        BoxEngine engine = requireEngine(box);
        if (!force && !engine.listCandies("", null, 1).entries().isEmpty()) {
            throw new BoxNotEmptyException(box.value());
        }
        engines.remove(box.value());
        engine.close();
        // TODO(phase-3): reference-counted GC of the Box's SSTable/Syrup/WAL/manifest ledgers.
    }

    public List<String> listBoxes() {
        List<String> names = new ArrayList<>(engines.keySet());
        names.sort(String::compareTo);
        return names;
    }

    public boolean boxExists(BoxName box) {
        return engines.containsKey(box.value());
    }

    /** Returns the engine for an owned Box, or throws {@link BoxNotFoundException}. */
    public BoxEngine engine(BoxName box) {
        return requireEngine(box);
    }

    private BoxEngine requireEngine(BoxName box) {
        BoxEngine engine = engines.get(box.value());
        if (engine == null) {
            throw new BoxNotFoundException(box.value());
        }
        return engine;
    }

    CandyboxConfig config() {
        return config;
    }

    /** A request handler that serves this node's Boxes; wire it into a Transport server or loopback. */
    public RequestHandler requestHandler() {
        return new NodeRequestHandler(this);
    }

    @Override
    public void close() {
        for (BoxEngine engine : engines.values()) {
            engine.close();
        }
        engines.clear();
        coordination.unregisterMember(nodeId);
    }
}
