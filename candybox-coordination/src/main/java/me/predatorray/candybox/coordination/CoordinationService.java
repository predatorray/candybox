package me.predatorray.candybox.coordination;

import java.util.List;
import java.util.Optional;

/**
 * The coordination SPI Candybox builds on, backed in production by ZooKeeper and in tests by an
 * in-memory fake. It provides exactly three capabilities the design relies on:
 *
 * <ol>
 *   <li><b>Versioned key-value with compare-and-set</b> — holds the per-Box pointer to the current
 *       manifest ledger. The pointer is always advanced with {@link #compareAndSet} on the expected
 *       version (never a blind set), so a checkpoint and a concurrent edit cannot silently race.</li>
 *   <li><b>Leases / leader election</b> — {@link #tryAcquireLease} grants movable, fenced, single
 *       ownership of a Box (and of compaction tasks). The {@link Lease#fencingToken()} fences zombies.</li>
 *   <li><b>Membership</b> — register/list cluster nodes for routing and rebalancing.</li>
 * </ol>
 *
 * <p>Implementations are thread-safe. Watch-based notifications are intentionally omitted in v1;
 * callers poll. (TODO(phase-2): add watches to avoid polling.)
 */
public interface CoordinationService extends AutoCloseable {

    // ---- versioned key-value (CAS) ----------------------------------------------------------

    /** Reads a key, or {@link Optional#empty()} if absent. */
    Optional<VersionedValue> get(String key);

    /**
     * Creates a key that must not already exist.
     *
     * @return the initial version (0)
     * @throws CasConflictException if the key already exists
     */
    long create(String key, byte[] value);

    /**
     * Atomically sets {@code key} to {@code value} iff its current version equals
     * {@code expectedVersion}.
     *
     * @return the new version
     * @throws CasConflictException if the key is absent or at a different version
     */
    long compareAndSet(String key, byte[] value, long expectedVersion);

    /**
     * Deletes {@code key} iff its current version equals {@code expectedVersion}.
     *
     * @throws CasConflictException if the key is absent or at a different version
     */
    void delete(String key, long expectedVersion);

    // ---- leases / leader election -----------------------------------------------------------

    /**
     * Attempts to acquire (or re-acquire) the lease on {@code resource} for {@code nodeId}.
     *
     * @param resource  the resource path (e.g. {@code "box/my-box/owner"})
     * @param nodeId    the acquiring node
     * @param ttlMillis lease time-to-live; must be renewed within this window
     * @return a held {@link Lease}, or empty if another node currently holds a valid lease
     */
    Optional<Lease> tryAcquireLease(String resource, int nodeId, long ttlMillis);

    /**
     * Reads the current valid holder of {@code resource} without acquiring it, for routing.
     *
     * @param resource the resource path
     * @return the holder, or empty if the lease is free/expired/released
     */
    Optional<LeaseInfo> leaseHolder(String resource);

    // ---- membership -------------------------------------------------------------------------

    /** Registers (or refreshes) this node in the cluster with opaque info bytes. */
    void registerMember(int nodeId, byte[] info);

    /** Removes this node from the cluster. */
    void unregisterMember(int nodeId);

    /** Lists currently registered node ids, ascending. */
    List<Integer> members();

    /**
     * Returns a registered member's opaque info bytes (e.g. its advertised {@code host:port}).
     *
     * @param nodeId the node id
     * @return the info, or empty if the node is not registered
     */
    Optional<byte[]> memberInfo(int nodeId);

    @Override
    void close();
}
