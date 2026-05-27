package me.predatorray.candybox.lsm.engine;

import java.io.ByteArrayInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.common.BoxName;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Clock;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.HybridLogicalClock;
import me.predatorray.candybox.common.LocatorType;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.Validation;
import me.predatorray.candybox.common.checksum.Crc32c;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.common.exception.BusyException;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.common.exception.StorageException;
import me.predatorray.candybox.lsm.manifest.Manifest;
import me.predatorray.candybox.lsm.manifest.ManifestEdit;
import me.predatorray.candybox.lsm.manifest.ManifestState;
import me.predatorray.candybox.lsm.memtable.Memtable;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;
import me.predatorray.candybox.lsm.sstable.SSTableReader;
import me.predatorray.candybox.lsm.sstable.SSTableWriter;
import me.predatorray.candybox.lsm.syrup.SyrupManager;
import me.predatorray.candybox.lsm.syrup.SyrupReader;
import me.predatorray.candybox.lsm.syrup.SyrupWriteResult;
import me.predatorray.candybox.lsm.wal.WriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The single-Box LSM storage engine: the Phase 1 deliverable wiring memtable + WAL + L0 SSTable flush
 * + Syrup chunking + the merged read path under LWW with HLC timestamps and tombstones.
 *
 * <p>One node owns a Box at a time and serializes its writes here. Writes take a write lock (so the
 * owner stamps HLC and appends to the WAL in order); reads take a read lock and resolve a key to the
 * highest-HLC locator across the active memtable and the L0 SSTables, then stream the bytes from
 * Syrups. A single fenced owner serializing all writes makes the Box effectively per-key linearizable
 * on the owner (see DESIGN.md).
 *
 * <p>This engine is networkless (Phase 1); the server wraps it with transport/routing in Phase 2.
 * Multi-level compaction is available via {@link #applyCompaction} but is not scheduled here (Phase 3).
 */
public final class BoxEngine implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BoxEngine.class);
    private static final int IDEMPOTENCY_CACHE_SIZE = 1024;

    private final BoxName box;
    private final CandyboxConfig config;
    private final LedgerStore ledgerStore;
    private final HybridLogicalClock hlc;
    private final Clock clock;

    private final SyrupManager syrupManager;
    private final SSTableWriter sstableWriter;
    private final SyrupReader syrupReader;
    private final Manifest manifest;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile Memtable active = new Memtable();
    private WriteAheadLog wal;
    private final ConcurrentMap<Long, SSTableReader> readers = new ConcurrentHashMap<>();

    // SSTable ledgers dropped by a committed compaction, awaiting physical deletion by GC: id -> when.
    private final ConcurrentMap<Long, Long> obsoleteSSTables = new ConcurrentHashMap<>();

    // Bounded idempotency cache: token -> already-applied result, so a retried put is a no-op.
    private final Map<String, CandyMetadata> idempotencyCache = Collections.synchronizedMap(
            new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, CandyMetadata> eldest) {
                    return size() > IDEMPOTENCY_CACHE_SIZE;
                }
            });

    private BoxEngine(BoxName box, CandyboxConfig config, LedgerStore ledgerStore,
                      HybridLogicalClock hlc, Clock clock, Manifest manifest, WriteAheadLog wal) {
        this.box = box;
        this.config = config;
        this.ledgerStore = ledgerStore;
        this.hlc = hlc;
        this.clock = clock;
        this.manifest = manifest;
        this.wal = wal;
        this.syrupManager = new SyrupManager(ledgerStore, config, ledgerConfig(LedgerRole.SYRUP));
        this.sstableWriter = new SSTableWriter(ledgerStore, config.bloomBitsPerKey());
        this.syrupReader = new SyrupReader(ledgerStore);
    }

    /**
     * Boots a brand-new Box: fresh manifest and WAL, empty memtable.
     *
     * @param fencingToken this owner's lease fencing token, stamped into every manifest edit
     */
    public static BoxEngine createNew(BoxName box, CandyboxConfig config, LedgerStore ledgerStore,
                                      int nodeId, Clock clock, long fencingToken) {
        HybridLogicalClock hlc = new HybridLogicalClock(nodeId, clock, config.maxClockSkewMillis());
        Manifest manifest = Manifest.createNew(ledgerStore, roleConfig(config, ledgerStore, box,
                LedgerRole.MANIFEST), fencingToken);
        WriteAheadLog wal = WriteAheadLog.create(ledgerStore, roleConfig(config, ledgerStore, box,
                LedgerRole.WAL));
        // Record the initial WAL id so a future owner can always find and fence it.
        manifest.apply(ManifestEdit.builder().newWalLedgerId(wal.ledgerId()).build());
        return new BoxEngine(box, config, ledgerStore, hlc, clock, manifest, wal);
    }

    /**
     * Recovers ownership of a Box on handover: replays the prior owner's manifest and WAL (fencing
     * both), rebuilds the memtable, and — critically — advances the HLC past the maximum HLC durably
     * recorded so a regressed wall clock cannot stamp a newer write with a lower timestamp.
     *
     * @param priorManifestLedgerId the prior owner's manifest ledger id
     * @param fencingToken          this owner's lease fencing token; a stale (lower) token is rejected
     */
    public static BoxEngine recover(BoxName box, CandyboxConfig config, LedgerStore ledgerStore,
                                    int nodeId, Clock clock, long priorManifestLedgerId,
                                    long fencingToken) {
        HybridLogicalClock hlc = new HybridLogicalClock(nodeId, clock, config.maxClockSkewMillis());
        Manifest manifest = Manifest.recover(ledgerStore,
                roleConfig(config, ledgerStore, box, LedgerRole.MANIFEST), priorManifestLedgerId,
                fencingToken);
        ManifestState state = manifest.current();

        Memtable memtable = new Memtable();
        long priorWalId = state.walLedgerId();
        if (priorWalId >= 0) {
            // recover-open fences the prior WAL so a resurrected old owner cannot keep appending.
            ReadableLedger priorWal = ledgerStore.recoverOpen(priorWalId);
            WriteAheadLog.ReplayResult replay;
            try {
                replay = WriteAheadLog.replay(priorWal);
            } finally {
                priorWal.close();
            }
            for (Mutation m : replay.mutations()) {
                memtable.put(m);
            }
            // The current WAL always holds the most recent mutations (it is rotated on flush), so its
            // max HLC dominates the flushed SSTables — observing it suffices for LWW correctness.
            hlc.observe(replay.maxHlc());
        }

        WriteAheadLog newWal = WriteAheadLog.create(ledgerStore,
                roleConfig(config, ledgerStore, box, LedgerRole.WAL));
        manifest.apply(ManifestEdit.builder().newWalLedgerId(newWal.ledgerId()).build());

        BoxEngine engine = new BoxEngine(box, config, ledgerStore, hlc, clock, manifest, newWal);
        engine.active = memtable;
        engine.openReadersFor(state);
        return engine;
    }

    private void openReadersFor(ManifestState state) {
        for (SSTableMeta table : state.tables()) {
            readers.put(table.ledgerId(), new SSTableReader(ledgerStore, table.ledgerId()));
        }
    }

    public BoxName box() {
        return box;
    }

    /** The id of the manifest ledger this owner writes to (used to hand off to the next owner). */
    public long manifestLedgerId() {
        return manifest.ledgerId();
    }

    // ---- writes ----------------------------------------------------------------------------

    /**
     * Stores a Candy. The owner stamps the HLC, streams bytes into Syrups, appends the locator to the
     * WAL, and applies it to the memtable.
     *
     * @param idempotencyToken optional client token to dedupe retried writes (may be null)
     * @return metadata for the stored Candy
     */
    public CandyMetadata putCandy(CandyKey key, InputStream data, String contentType,
                                  Map<String, String> userMetadata, String idempotencyToken) {
        Validation.checkCandyKey(key, config.sizeLimits());
        Validation.checkUserMetadata(userMetadata, config.sizeLimits());
        if (idempotencyToken != null) {
            CandyMetadata cached = idempotencyCache.get(idempotencyToken);
            if (cached != null) {
                return cached;
            }
        }
        Map<String, String> metadata = userMetadata == null ? Map.of() : Map.copyOf(userMetadata);

        lock.writeLock().lock();
        try {
            rejectIfStalled();
            SyrupWriteResult written = syrupManager.writeCandy(data);
            Validation.checkCandySize(written.contentLength(), config.sizeLimits());

            Hlc stamp = hlc.tick();
            CandyLocator locator = new CandyLocator(stamp, LocatorType.PUT, written.contentLength(),
                    config.sizeLimits().chunkSizeBytes(), contentType, metadata, written.crc32c(),
                    clock.currentTimeMillis(), written.segments());
            Mutation mutation = new Mutation(key, locator);
            wal.append(mutation);
            active.put(mutation);
            maybeFlushLocked();

            CandyMetadata result = CandyMetadata.from(locator);
            if (idempotencyToken != null) {
                idempotencyCache.put(idempotencyToken, result);
            }
            return result;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Convenience byte[] put. */
    public CandyMetadata putCandy(CandyKey key, byte[] data, String contentType,
                                  Map<String, String> userMetadata, String idempotencyToken) {
        return putCandy(key, new ByteArrayInputStream(data), contentType, userMetadata, idempotencyToken);
    }

    /** Writes a DELETE tombstone for {@code key} under a fresh HLC. */
    public void deleteCandy(CandyKey key) {
        Validation.checkCandyKey(key, config.sizeLimits());
        lock.writeLock().lock();
        try {
            rejectIfStalled();
            Hlc stamp = hlc.tick();
            Mutation mutation = new Mutation(key, CandyLocator.tombstone(stamp, clock.currentTimeMillis()));
            wal.append(mutation);
            active.put(mutation);
            maybeFlushLocked();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ---- reads -----------------------------------------------------------------------------

    /** Returns metadata for a live Candy, or throws {@link CandyNotFoundException}. */
    public CandyMetadata headCandy(CandyKey key) {
        CandyLocator locator = resolveLive(key)
                .orElseThrow(() -> new CandyNotFoundException(box.value(), key.value()));
        return CandyMetadata.from(locator);
    }

    /**
     * Streams a Candy's bytes to {@code out}, validating the whole-object CRC, and returns its metadata.
     *
     * @throws CandyNotFoundException if there is no live Candy at {@code key}
     */
    public CandyMetadata getCandy(CandyKey key, OutputStream out) {
        CandyLocator locator = resolveLive(key)
                .orElseThrow(() -> new CandyNotFoundException(box.value(), key.value()));
        Crc32c.Accumulator wholeCrc = new Crc32c.Accumulator();
        OutputStream checking = new FilterOutputStream(out) {
            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);
                wholeCrc.update(b, off, len);
            }

            @Override
            public void write(int b) throws IOException {
                out.write(b);
                byte[] one = {(byte) b};
                wholeCrc.update(one, 0, 1);
            }
        };
        syrupReader.readTo(locator.segments(), checking);
        if (wholeCrc.value() != locator.crc32c()) {
            throw new StorageException("Whole-object CRC mismatch for box=" + box + " key=" + key);
        }
        return CandyMetadata.from(locator);
    }

    /** Convenience: fully read a (small) Candy into a byte array. */
    public byte[] getCandy(CandyKey key) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        getCandy(key, out);
        return out.toByteArray();
    }

    /**
     * Lists live Candies whose key starts with {@code prefix}, after {@code startAfter}, up to
     * {@code maxKeys}. Tombstones are suppressed; the result carries a continuation cursor.
     */
    public ListResult listCandies(String prefix, String startAfter, int maxKeys) {
        int limit = maxKeys > 0 ? maxKeys : 1000;
        String pfx = prefix == null ? "" : prefix;

        lock.readLock().lock();
        try {
            CandyKey startKey = null;
            if (startAfter != null) {
                startKey = CandyKey.of(startAfter);
            } else if (!pfx.isEmpty()) {
                startKey = CandyKey.of(pfx);
            }

            Iterator<Mutation> merged = mergedView(startKey, true);
            List<ListResult.ListEntry> entries = new ArrayList<>();
            String next = null;
            while (merged.hasNext()) {
                Mutation m = merged.next();
                String keyValue = m.key().value();
                if (startAfter != null && keyValue.compareTo(startAfter) <= 0) {
                    continue; // startAfter is exclusive
                }
                if (!pfx.isEmpty() && !keyValue.startsWith(pfx)) {
                    if (keyValue.compareTo(pfx) > 0) {
                        break; // sorted: past the prefix range
                    }
                    continue;
                }
                if (entries.size() == limit) {
                    next = entries.get(entries.size() - 1).key().value();
                    break;
                }
                CandyLocator loc = m.locator();
                entries.add(new ListResult.ListEntry(m.key(), loc.contentLength(), loc.createdAtMillis()));
            }
            return new ListResult(entries, next);
        } finally {
            lock.readLock().unlock();
        }
    }

    // ---- compaction (execution exposed; scheduling is Phase 3) ------------------------------

    /**
     * Commits a compaction's manifest edit (swap inputs for output) and refreshes the SSTable readers.
     * The removed input ledgers are recorded as obsolete (with the time they left the committed
     * manifest) for GC to delete after the grace period; physical deletion is GC's job.
     */
    public void applyCompaction(ManifestEdit edit) {
        lock.writeLock().lock();
        try {
            manifest.apply(edit); // fencing-gated: throws if this owner has been superseded
            for (SSTableMeta added : edit.addedTables()) {
                readers.computeIfAbsent(added.ledgerId(), id -> new SSTableReader(ledgerStore, id));
            }
            long now = clock.currentTimeMillis();
            for (Long removed : edit.removedTableLedgerIds()) {
                SSTableReader r = readers.remove(removed);
                if (r != null) {
                    r.close();
                }
                obsoleteSSTables.put(removed, now);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * SSTable ledger ids dropped by a committed compaction at or before {@code asOfMillis} and not yet
     * physically deleted — the GC reclaim set for this Box.
     */
    public java.util.List<Long> reclaimableSSTables(long asOfMillis) {
        java.util.List<Long> ids = new java.util.ArrayList<>();
        for (Map.Entry<Long, Long> e : obsoleteSSTables.entrySet()) {
            if (e.getValue() <= asOfMillis) {
                ids.add(e.getKey());
            }
        }
        return ids;
    }

    /** Drops a ledger id from the obsolete set once GC has physically deleted it. */
    public void forgetObsoleteSSTable(long ledgerId) {
        obsoleteSSTables.remove(ledgerId);
    }

    /** A consistent snapshot of the current LSM state (for compaction picking / inspection). */
    public ManifestState manifestState() {
        return manifest.current();
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            for (SSTableReader r : readers.values()) {
                r.close();
            }
            readers.clear();
            wal.close();
            manifest.close();
            syrupManager.close();
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ---- internals -------------------------------------------------------------------------

    private void rejectIfStalled() {
        int l0 = manifest.current().level0().size();
        if (l0 >= config.l0StallThreshold()) {
            throw new BusyException("Box " + box + " is write-stalled: " + l0 + " L0 SSTables");
        }
    }

    private Optional<CandyLocator> resolveLive(CandyKey key) {
        lock.readLock().lock();
        try {
            CandyLocator best = active.get(key).orElse(null);
            for (SSTableReader reader : readers.values()) {
                if (reader.minKey().compareTo(key) <= 0 && reader.maxKey().compareTo(key) >= 0) {
                    Optional<CandyLocator> candidate = reader.get(key);
                    if (candidate.isPresent()
                            && (best == null || candidate.get().hlc().isAfter(best.hlc()))) {
                        best = candidate.get();
                    }
                }
            }
            if (best == null || best.isTombstone()) {
                return Optional.empty();
            }
            return Optional.of(best);
        } finally {
            lock.readLock().unlock();
        }
    }

    private Iterator<Mutation> mergedView(CandyKey start, boolean dropTombstones) {
        List<Iterator<Mutation>> sources = new ArrayList<>();
        sources.add(start == null ? active.iterator() : active.iterator(start));
        for (SSTableReader reader : readers.values()) {
            sources.add(reader.scan(start));
        }
        return new me.predatorray.candybox.lsm.iterator.MergingIterator(sources, dropTombstones);
    }

    /** Flushes the active memtable to an L0 SSTable if it has grown past the threshold. */
    private void maybeFlushLocked() {
        if (active.approximateSizeBytes() >= config.memtableFlushThresholdBytes()) {
            flushLocked();
        }
    }

    /** Forces a flush of the active memtable (visible for tests/operations). */
    public void flush() {
        lock.writeLock().lock();
        try {
            flushLocked();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void flushLocked() {
        Memtable flushing = active;
        if (flushing.isEmpty()) {
            return;
        }
        // Collect the Syrups referenced by this memtable (for live-Syrup tracking / GC).
        Set<Long> syrups = new LinkedHashSet<>();
        Iterator<Mutation> scan = flushing.iterator();
        while (scan.hasNext()) {
            for (SegmentRef seg : scan.next().locator().segments()) {
                syrups.add(seg.syrupId());
            }
        }

        SSTableMeta table = sstableWriter.write(ledgerConfig(LedgerRole.SSTABLE), 0, flushing.iterator());

        WriteAheadLog newWal = WriteAheadLog.create(ledgerStore, ledgerConfig(LedgerRole.WAL));
        manifest.apply(ManifestEdit.flush(table, syrups, newWal.ledgerId()));
        wal.close();
        wal = newWal;
        active = new Memtable();

        readers.put(table.ledgerId(), new SSTableReader(ledgerStore, table.ledgerId()));
        LOG.debug("Flushed memtable of box {} to SSTable ledger {} ({} entries)", box,
                table.ledgerId(), table.entryCount());
    }

    private LedgerConfig ledgerConfig(LedgerRole role) {
        return roleConfig(config, ledgerStore, box, role);
    }

    private static LedgerConfig roleConfig(CandyboxConfig config, LedgerStore store, BoxName box,
                                           LedgerRole role) {
        Map<String, byte[]> metadata = new LinkedHashMap<>();
        metadata.put("candybox-role", role.name().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        metadata.put("candybox-box", box.value().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return new LedgerConfig(config.quorum(role), metadata);
    }
}
