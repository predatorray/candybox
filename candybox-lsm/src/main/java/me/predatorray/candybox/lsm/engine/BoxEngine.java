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
import java.util.concurrent.atomic.AtomicLong;
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
import me.predatorray.candybox.common.exception.ValidationException;
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

    // Syrup ledgers no longer referenced by any SSTable/memtable, awaiting GC: id -> first-seen-orphan.
    private final ConcurrentMap<Long, Long> pendingOrphanSyrups = new ConcurrentHashMap<>();

    // WAL ledgers rotated out at flush (data now durable in an SSTable), awaiting GC: id -> when.
    private final ConcurrentMap<Long, Long> obsoleteWals = new ConcurrentHashMap<>();

    // Lightweight operational counters (snapshotted via stats()).
    private final AtomicLong putCount = new AtomicLong();
    private final AtomicLong deleteCount = new AtomicLong();
    private final AtomicLong getCount = new AtomicLong();
    private final AtomicLong headCount = new AtomicLong();
    private final AtomicLong listCount = new AtomicLong();
    private final AtomicLong flushCount = new AtomicLong();
    private final AtomicLong compactionCount = new AtomicLong();
    private final AtomicLong stallRejectionCount = new AtomicLong();

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
        // Catch Syrups already orphaned before this handover (e.g. by a prior owner that crashed
        // pre-GC) so they are not leaked.
        engine.lock.writeLock().lock();
        try {
            engine.recomputeOrphanSyrupsLocked(clock.currentTimeMillis());
        } finally {
            engine.lock.writeLock().unlock();
        }
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
            putCount.incrementAndGet();
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
            deleteCount.incrementAndGet();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Server-side, zero-copy copy: writes a fresh PUT at {@code dst} that points at the <em>same</em>
     * Syrup segments as the live Candy at {@code src} — no Candy bytes are read or rewritten. Both keys
     * then resolve to identical content (and share the same whole-object CRC). Same-Box only.
     *
     * @throws CandyNotFoundException if there is no live Candy at {@code src}
     */
    public CandyMetadata copyCandy(CandyKey src, CandyKey dst, String idempotencyToken) {
        return copyOrRename(src, dst, idempotencyToken, false);
    }

    /**
     * Server-side, zero-copy rename/move: like {@link #copyCandy} but also writes a DELETE tombstone at
     * {@code src}, atomically (the single owner serializes both into the WAL and memtable under one
     * write lock). The Candy's bytes never move; only locator pointers are rewritten. Same-Box only.
     *
     * @throws CandyNotFoundException if there is no live Candy at {@code src}
     */
    public CandyMetadata renameCandy(CandyKey src, CandyKey dst, String idempotencyToken) {
        return copyOrRename(src, dst, idempotencyToken, true);
    }

    private CandyMetadata copyOrRename(CandyKey src, CandyKey dst, String idempotencyToken,
                                       boolean tombstoneSource) {
        Validation.checkCandyKey(src, config.sizeLimits());
        Validation.checkCandyKey(dst, config.sizeLimits());
        if (src.equals(dst)) {
            throw new ValidationException("source and destination keys must differ");
        }
        if (idempotencyToken != null) {
            CandyMetadata cached = idempotencyCache.get(idempotencyToken);
            if (cached != null) {
                return cached;
            }
        }
        lock.writeLock().lock();
        try {
            rejectIfStalled();
            CandyLocator source = resolveLiveLocked(src)
                    .orElseThrow(() -> new CandyNotFoundException(box.value(), src.value()));

            // The destination locator reuses the source's Syrup segments and CRC verbatim — zero copy.
            Hlc stamp = hlc.tick();
            CandyLocator dstLocator = new CandyLocator(stamp, LocatorType.PUT, source.contentLength(),
                    source.chunkSize(), source.contentType(), source.userMetadata(), source.crc32c(),
                    clock.currentTimeMillis(), source.segments());
            Mutation dstMutation = new Mutation(dst, dstLocator);
            wal.append(dstMutation);

            Mutation tombstone = null;
            if (tombstoneSource) {
                tombstone = new Mutation(src,
                        CandyLocator.tombstone(hlc.tick(), clock.currentTimeMillis()));
                wal.append(tombstone);
            }
            // Apply to the memtable only after both are durable in the WAL (atomic to readers).
            active.put(dstMutation);
            if (tombstone != null) {
                active.put(tombstone);
                deleteCount.incrementAndGet();
            }
            maybeFlushLocked();

            CandyMetadata result = CandyMetadata.from(dstLocator);
            if (idempotencyToken != null) {
                idempotencyCache.put(idempotencyToken, result);
            }
            putCount.incrementAndGet();
            return result;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ---- reads -----------------------------------------------------------------------------

    /** Returns metadata for a live Candy, or throws {@link CandyNotFoundException}. */
    public CandyMetadata headCandy(CandyKey key) {
        CandyLocator locator = resolveLive(key)
                .orElseThrow(() -> new CandyNotFoundException(box.value(), key.value()));
        headCount.incrementAndGet();
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
        getCount.incrementAndGet();
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
     * {@code maxKeys}. Tombstones are suppressed; the result carries a continuation cursor. A thin
     * forward wrapper over {@link #scanCandies(ScanQuery)}.
     */
    public ListResult listCandies(String prefix, String startAfter, int maxKeys) {
        CandyKey cursor = startAfter == null ? null : CandyKey.of(startAfter);
        return scanCandies(ScanQuery.forward(prefix, cursor, maxKeys));
    }

    /**
     * Lists live Candies over a {@link ScanQuery}: an optional {@code [start, end)} window, optionally
     * narrowed to a prefix, walked forward or in reverse, paged by the query's cursor and limit.
     * Tombstones are suppressed; the result's {@code nextStartAfter} is the continuation cursor for the
     * next page in the same direction (or {@code null} when exhausted).
     */
    public ListResult scanCandies(ScanQuery query) {
        int limit = query.effectiveMaxKeys();
        boolean forward = query.direction() == ScanDirection.FORWARD;

        lock.readLock().lock();
        try {
            // Normalize the prefix into [lower, upper) and intersect it with any explicit bounds.
            CandyKey lower = query.startInclusive();
            CandyKey upper = query.endExclusive();
            if (query.prefix() != null && !query.prefix().isEmpty()) {
                CandyKey prefixLower = CandyKey.of(query.prefix());
                byte[] succ = me.predatorray.candybox.common.util.Bytes
                        .prefixSuccessor(prefixLower.utf8Bytes());
                lower = maxKey(lower, prefixLower);
                upper = minKey(upper, succ == null ? null : CandyKey.ofUtf8(succ));
            }
            CandyKey cursor = query.cursorExclusive();

            Iterator<Mutation> merged = mergedView(lower, upper, cursor, query.direction());
            List<ListResult.ListEntry> entries = new ArrayList<>();
            String next = null;
            while (merged.hasNext()) {
                Mutation m = merged.next();
                CandyKey key = m.key();
                if (cursor != null) {
                    int c = key.compareTo(cursor);
                    if (forward ? c <= 0 : c >= 0) {
                        continue; // cursor is exclusive in the scan direction
                    }
                }
                if (forward) {
                    if (lower != null && key.compareTo(lower) < 0) {
                        continue;
                    }
                    if (upper != null && key.compareTo(upper) >= 0) {
                        break; // ascending: past the window's exclusive upper bound
                    }
                } else {
                    if (upper != null && key.compareTo(upper) >= 0) {
                        continue;
                    }
                    if (lower != null && key.compareTo(lower) < 0) {
                        break; // descending: below the window's inclusive lower bound
                    }
                }
                if (entries.size() == limit) {
                    next = entries.get(entries.size() - 1).key().value();
                    break;
                }
                CandyLocator loc = m.locator();
                entries.add(new ListResult.ListEntry(key, loc.contentLength(), loc.createdAtMillis()));
            }
            listCount.incrementAndGet();
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
            recomputeOrphanSyrupsLocked(now);
            compactionCount.incrementAndGet();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Recomputes which live Syrups are no longer referenced by any SSTable, the active memtable, or the
     * currently open write Syrup, recording newly-orphaned ones with the time first seen. Syrup
     * references only ever decrease (compaction drops superseded/tombstoned locators), so an orphan
     * stays an orphan. Called under the write lock after a manifest change.
     */
    private void recomputeOrphanSyrupsLocked(long now) {
        java.util.Set<Long> referenced = new java.util.HashSet<>(manifest.current().referencedSyrups());
        for (java.util.Iterator<Mutation> it = active.iterator(); it.hasNext(); ) {
            for (SegmentRef seg : it.next().locator().segments()) {
                referenced.add(seg.syrupId());
            }
        }
        long openSyrup = syrupManager.currentSyrupId();
        if (openSyrup >= 0) {
            referenced.add(openSyrup);
        }
        for (Long syrup : manifest.current().liveSyrups()) {
            if (!referenced.contains(syrup)) {
                pendingOrphanSyrups.putIfAbsent(syrup, now);
            }
        }
    }

    /** Orphaned Syrup ledger ids first seen at or before {@code asOfMillis} — the GC reclaim set. */
    public java.util.List<Long> reclaimableSyrups(long asOfMillis) {
        java.util.List<Long> ids = new java.util.ArrayList<>();
        for (Map.Entry<Long, Long> e : pendingOrphanSyrups.entrySet()) {
            if (e.getValue() <= asOfMillis) {
                ids.add(e.getKey());
            }
        }
        return ids;
    }

    /**
     * Drops the given orphaned Syrups from the live set via a fencing-gated manifest edit (so a fenced
     * owner cannot remove them) and stops tracking them. Call before physically deleting the ledgers.
     */
    public void dropSyrups(java.util.Collection<Long> syrupIds) {
        if (syrupIds.isEmpty()) {
            return;
        }
        lock.writeLock().lock();
        try {
            manifest.apply(ManifestEdit.builder()
                    .removedSyrups(new java.util.LinkedHashSet<>(syrupIds))
                    .build());
            syrupIds.forEach(pendingOrphanSyrups::remove);
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

    /** WAL ledger ids rotated out at or before {@code asOfMillis} and not yet deleted. */
    public java.util.List<Long> reclaimableWals(long asOfMillis) {
        java.util.List<Long> ids = new java.util.ArrayList<>();
        for (Map.Entry<Long, Long> e : obsoleteWals.entrySet()) {
            if (e.getValue() <= asOfMillis) {
                ids.add(e.getKey());
            }
        }
        return ids;
    }

    /** Drops a WAL ledger id from the obsolete set once GC has physically deleted it. */
    public void forgetObsoleteWal(long ledgerId) {
        obsoleteWals.remove(ledgerId);
    }

    /** A consistent snapshot of the current LSM state (for compaction picking / inspection). */
    public ManifestState manifestState() {
        return manifest.current();
    }

    /** A snapshot of this engine's cumulative operational counters. */
    public BoxEngineStats stats() {
        return new BoxEngineStats(putCount.get(), deleteCount.get(), getCount.get(), headCount.get(),
                listCount.get(), flushCount.get(), compactionCount.get(), stallRejectionCount.get());
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
            stallRejectionCount.incrementAndGet();
            throw new BusyException("Box " + box + " is write-stalled: " + l0 + " L0 SSTables");
        }
    }

    private Optional<CandyLocator> resolveLive(CandyKey key) {
        lock.readLock().lock();
        try {
            return resolveLiveLocked(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Resolves a key to its live locator; caller must hold the read or write lock. */
    private Optional<CandyLocator> resolveLiveLocked(CandyKey key) {
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
    }

    /**
     * Builds a tombstone-suppressed merged view over the window {@code [lower, upper)} in the given
     * direction, seeking each source to the appropriate bound and pruning SSTables that cannot overlap
     * the window. The caller still applies exact bound/cursor filtering on the emitted keys.
     */
    private Iterator<Mutation> mergedView(CandyKey lower, CandyKey upper, CandyKey cursor,
                                          ScanDirection direction) {
        List<Iterator<Mutation>> sources = new ArrayList<>();
        if (direction == ScanDirection.FORWARD) {
            CandyKey seek = maxKey(lower, cursor);
            sources.add(seek == null ? active.iterator() : active.iterator(seek));
            for (SSTableReader reader : readers.values()) {
                if (overlapsWindow(reader.minKey(), reader.maxKey(), lower, upper)) {
                    sources.add(reader.scan(seek));
                }
            }
        } else {
            CandyKey seekUpper = minKey(upper, cursor);
            sources.add(seekUpper == null ? active.descendingIterator()
                    : active.descendingIterator(seekUpper));
            for (SSTableReader reader : readers.values()) {
                if (overlapsWindow(reader.minKey(), reader.maxKey(), lower, upper)) {
                    sources.add(reader.scanReverse(seekUpper));
                }
            }
        }
        return new me.predatorray.candybox.lsm.iterator.MergingIterator(sources, true, direction);
    }

    /** Whether {@code [minKey, maxKey]} overlaps the half-open window {@code [lower, upper)}. */
    private static boolean overlapsWindow(CandyKey minKey, CandyKey maxKey, CandyKey lower,
                                          CandyKey upper) {
        if (lower != null && maxKey.compareTo(lower) < 0) {
            return false;
        }
        if (upper != null && minKey.compareTo(upper) >= 0) {
            return false;
        }
        return true;
    }

    /** The greater of two nullable keys, where {@code null} means "unbounded below" (loses). */
    private static CandyKey maxKey(CandyKey a, CandyKey b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.compareTo(b) >= 0 ? a : b;
    }

    /** The lesser of two nullable keys, where {@code null} means "unbounded above" (loses). */
    private static CandyKey minKey(CandyKey a, CandyKey b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.compareTo(b) <= 0 ? a : b;
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
        long obsoleteWalId = wal.ledgerId();
        manifest.apply(ManifestEdit.flush(table, syrups, newWal.ledgerId()));
        wal.close();
        wal = newWal;
        active = new Memtable();

        // The rotated WAL's mutations are now durable in the SSTable and the manifest points at the
        // new WAL, so the old one is no longer a recovery source and may be GC'd.
        obsoleteWals.put(obsoleteWalId, clock.currentTimeMillis());
        readers.put(table.ledgerId(), new SSTableReader(ledgerStore, table.ledgerId()));
        flushCount.incrementAndGet();
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
