package me.predatorray.candybox.coordination.zk;

import java.util.List;
import java.util.Optional;
import me.predatorray.candybox.coordination.CoordinationService;
import me.predatorray.candybox.coordination.Lease;
import me.predatorray.candybox.coordination.VersionedValue;

/**
 * ZooKeeper-backed {@link CoordinationService}. Scaffolded in this run; wired up in Phase 2.
 *
 * <p>Intended mapping (documented now so the SPI shape is right):
 * <ul>
 *   <li><b>Versioned KV / manifest pointer</b> → a znode per key; ZooKeeper's znode {@code version}
 *       is the CAS version, and {@code setData(path, data, expectedVersion)} is the compare-and-set.</li>
 *   <li><b>Leases / leader election</b> → an ephemeral znode per resource tied to the owner's session;
 *       the creation {@code czxid} (or a monotonic counter znode) is the fencing token. Session loss
 *       expires the lease. Renewal is the ZooKeeper session heartbeat.</li>
 *   <li><b>Membership</b> → ephemeral child znodes under a {@code /members} path.</li>
 * </ul>
 *
 * <p>TODO(phase-2): implement using Apache Curator (recipes for leader latch, shared count, and
 * persistent/ephemeral nodes), running against the in-process ZooKeeper test server in integration
 * tests. Until then every method throws {@link UnsupportedOperationException}.
 */
public final class ZooKeeperCoordinationService implements CoordinationService {

    private static final String TODO = "TODO(phase-2): ZooKeeperCoordinationService not yet implemented";

    @Override
    public Optional<VersionedValue> get(String key) {
        throw new UnsupportedOperationException(TODO);
    }

    @Override
    public long create(String key, byte[] value) {
        throw new UnsupportedOperationException(TODO);
    }

    @Override
    public long compareAndSet(String key, byte[] value, long expectedVersion) {
        throw new UnsupportedOperationException(TODO);
    }

    @Override
    public void delete(String key, long expectedVersion) {
        throw new UnsupportedOperationException(TODO);
    }

    @Override
    public Optional<Lease> tryAcquireLease(String resource, int nodeId, long ttlMillis) {
        throw new UnsupportedOperationException(TODO);
    }

    @Override
    public void registerMember(int nodeId, byte[] info) {
        throw new UnsupportedOperationException(TODO);
    }

    @Override
    public void unregisterMember(int nodeId) {
        throw new UnsupportedOperationException(TODO);
    }

    @Override
    public List<Integer> members() {
        throw new UnsupportedOperationException(TODO);
    }

    @Override
    public void close() {
        // nothing to close yet
    }
}
