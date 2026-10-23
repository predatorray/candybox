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
package me.predatorray.candybox.lsm.engine;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
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
import me.predatorray.candybox.common.auth.ObjectAcl;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.HybridLogicalClock;
import me.predatorray.candybox.common.LocatorType;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.Part;
import me.predatorray.candybox.common.RangeTombstone;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.Validation;
import me.predatorray.candybox.common.concurrent.BoundedLruCache;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.common.exception.BusyException;
import me.predatorray.candybox.common.exception.CandyNotFoundException;
import me.predatorray.candybox.common.exception.LimitExceededException;
import me.predatorray.candybox.common.exception.ValidationException;
import me.predatorray.candybox.lsm.iterator.MergingIterator;
import me.predatorray.candybox.lsm.iterator.ReopeningIterator;
import me.predatorray.candybox.lsm.manifest.Manifest;
import me.predatorray.candybox.lsm.manifest.ManifestEdit;
import me.predatorray.candybox.lsm.manifest.ManifestState;
import me.predatorray.candybox.lsm.manifest.MultipartUploadState;
import me.predatorray.candybox.lsm.manifest.RenameIntent;
import me.predatorray.candybox.lsm.memtable.Memtable;
import me.predatorray.candybox.lsm.sstable.SSTableMeta;
import me.predatorray.candybox.lsm.sstable.SSTableReader;
import me.predatorray.candybox.lsm.sstable.SSTableWriter;
import me.predatorray.candybox.lsm.syrup.SyrupManager;
import me.predatorray.candybox.lsm.syrup.SyrupReader;
import me.predatorray.candybox.lsm.syrup.SyrupWriteResult;
import me.predatorray.candybox.lsm.wal.WalEntry;
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

    // Syrup ledgers no longer referenced by any SSTable/memtable, awaiting GC: id -> first-seen-orphan.
    private final ConcurrentMap<Long, Long> pendingOrphanSyrups = new ConcurrentHashMap<>();

    // Syrups this owner created since the last flush, joined to the live set by the next flush even if
    // nothing then references them (the Candy was overwritten, or its write failed), so GC sees them.
    // Guarded by the write lock, which every Syrup write holds.
    private final Set<Long> unflushedSyrups = new LinkedHashSet<>();

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
    private final BoundedLruCache<String, CandyMetadata> idempotencyCache =
            new BoundedLruCache<>(IDEMPOTENCY_CACHE_SIZE);

    private BoxEngine(BoxName box, CandyboxConfig config, LedgerStore ledgerStore,
                      HybridLogicalClock hlc, Clock clock, Manifest manifest, WriteAheadLog wal) {
        this.box = box;
        this.config = config;
        this.ledgerStore = ledgerStore;
        this.hlc = hlc;
        this.clock = clock;
        this.manifest = manifest;
        this.wal = wal;
        this.syrupManager = new SyrupManager(ledgerStore, config, ledgerConfig(LedgerRole.SYRUP),
                unflushedSyrups::add);
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
     * both), flushes the replayed WAL to an L0 SSTable, and — critically — advances the HLC past
     * the maximum HLC durably recorded so a regressed wall clock cannot stamp a newer write with a
     * lower timestamp.
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
        WriteAheadLog newWal = null;
        BoxEngine engine = null;
        try {
            ManifestState state = manifest.current();
            Memtable memtable = new Memtable();
            // Every replayed write's Syrups, overwritten ones included: the prior owner tracked
            // those only in memory, so the recovery flush adds them to the live set for GC.
            Set<Long> replayedSyrups = new LinkedHashSet<>();
            Hlc highWater = state.maxHlc();
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
                for (WalEntry e : replay.entries()) {
                    if (e instanceof WalEntry.PointMutation pm) {
                        memtable.put(pm.mutation());
                        for (SegmentRef seg : pm.mutation().locator().segments()) {
                            replayedSyrups.add(seg.syrupId());
                        }
                    } else if (e instanceof WalEntry.RangeDelete rd) {
                        memtable.delete(rd.tombstone());
                    }
                }
                if (replay.maxHlc().isAfter(highWater)) {
                    highWater = replay.maxHlc();
                }
            }
            // The WAL alone is not enough: right after a flush it is empty and the newest HLCs live
            // only in SSTables, which the manifest's high-water mark covers.
            if (highWater.isAfter(Hlc.MIN)) {
                try {
                    hlc.observe(highWater);
                } catch (IllegalStateException e) {
                    throw new IllegalStateException("Cannot recover box " + box + ": recorded HLC "
                            + highWater + " (manifest high-water " + state.maxHlc() + ") leads the"
                            + " local clock by more than maxClockSkewMillis; fix the clock or raise"
                            + " the limit", e);
                }
            }

            newWal = WriteAheadLog.create(ledgerStore,
                    roleConfig(config, ledgerStore, box, LedgerRole.WAL));
            engine = new BoxEngine(box, config, ledgerStore, hlc, clock, manifest, newWal);
            engine.openReadersFor(state);
            // The prior manifest and WAL are superseded by this one: the rotation below leaves the
            // prior WAL unreachable, so a non-empty replay is flushed to L0 in the same edit, or the
            // next owner would lose it (#63). This edit only takes effect if the manifest pointer
            // moves here, and nothing reads a superseded manifest or WAL again.
            long now = clock.currentTimeMillis();
            Map<Long, Long> superseded = new LinkedHashMap<>();
            superseded.put(priorManifestLedgerId, now);
            if (priorWalId >= 0) {
                superseded.put(priorWalId, now);
            }
            if (memtable.isEmpty()) {
                manifest.apply(ManifestEdit.builder().newWalLedgerId(newWal.ledgerId())
                        .maxHlc(highWater).addedPendingDeletes(superseded).build());
            } else {
                L0Table table = engine.writeL0(memtable);
                Set<Long> syrups = new LinkedHashSet<>(table.syrups());
                syrups.addAll(replayedSyrups);
                manifest.apply(ManifestEdit.flush(table.meta(), syrups, newWal.ledgerId(), highWater)
                        .withAddedPendingDeletes(superseded));
                engine.readers.put(table.meta().ledgerId(),
                        new SSTableReader(ledgerStore, table.meta().ledgerId()));
                engine.flushCount.incrementAndGet();
                LOG.info("Recovery flushed {} replayed entries of box {} to SSTable ledger {}",
                        table.meta().entryCount(), box, table.meta().ledgerId());
            }
            // Catch Syrups already orphaned before this handover (e.g. by a prior owner that crashed
            // pre-GC) so they are not leaked.
            engine.lock.writeLock().lock();
            try {
                engine.recomputeOrphanSyrupsLocked(clock.currentTimeMillis());
            } finally {
                engine.lock.writeLock().unlock();
            }
            return engine;
        } catch (RuntimeException e) {
            // The pointer never moves here, so nothing references this owner's ledgers; they leak
            // (#105), but their handles must not.
            try {
                if (engine != null) {
                    engine.close();
                } else {
                    if (newWal != null) {
                        newWal.close();
                    }
                    manifest.close();
                }
            } catch (RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
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
        return putCandy(key, data, contentType, userMetadata, idempotencyToken, ObjectAcl.NONE);
    }

    /**
     * {@link #putCandy(CandyKey, InputStream, String, Map, String)} stamping the object's owner and
     * ACL grants into the locator (format v3). Pass {@link ObjectAcl#NONE} for unowned writes.
     */
    public CandyMetadata putCandy(CandyKey key, InputStream data, String contentType,
                                  Map<String, String> userMetadata, String idempotencyToken,
                                  ObjectAcl acl) {
        Validation.checkCandyKey(key, config.sizeLimits());
        Validation.checkUserMetadata(userMetadata, config.sizeLimits());
        CandyMetadata cached = idempotentResult(idempotencyToken);
        if (cached != null) {
            return cached;
        }
        Map<String, String> metadata = userMetadata == null ? Map.of() : Map.copyOf(userMetadata);

        lock.writeLock().lock();
        try {
            CandyMetadata replay = idempotentResult(idempotencyToken);
            if (replay != null) {
                return replay;
            }
            rejectIfStalled();
            SyrupWriteResult written = syrupManager.writeCandy(data);
            Validation.checkCandySize(written.contentLength(), config.sizeLimits());

            Hlc stamp = hlc.tick();
            CandyLocator locator = CandyLocator.singlePart(stamp, written.contentLength(),
                    config.sizeLimits().chunkSizeBytes(), contentType, metadata, written.crc32c(),
                    clock.currentTimeMillis(), written.segments(), acl);
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

    // ---- multipart upload -------------------------------------------------------------------

    /**
     * Begins a multipart upload targeting {@code key}. Records an in-flight upload in the manifest
     * (fenced) so it survives owner handover, and returns the {@code uploadId} the client uses for
     * subsequent {@link #uploadPart} / {@link #completeMultipartUpload} / {@link #abortMultipartUpload}
     * calls. The {@code uploadId} is a 128-bit random base32 string generated by the engine.
     */
    public String createMultipartUpload(CandyKey key, String contentType,
                                        Map<String, String> userMetadata) {
        Validation.checkCandyKey(key, config.sizeLimits());
        Validation.checkUserMetadata(userMetadata, config.sizeLimits());
        Map<String, String> metadata = userMetadata == null ? Map.of() : Map.copyOf(userMetadata);
        lock.writeLock().lock();
        try {
            int inFlight = manifest.current().multipartUploads().size();
            if (inFlight >= config.multipartMaxConcurrentUploadsPerBox()) {
                throw new ValidationException("Too many in-flight multipart uploads in box "
                        + box + " (cap=" + config.multipartMaxConcurrentUploadsPerBox() + ")");
            }
            String uploadId = generateUploadId();
            MultipartUploadState upload = new MultipartUploadState(uploadId, key.value(), contentType,
                    metadata, clock.currentTimeMillis(), java.util.Map.of());
            manifest.apply(ManifestEdit.builder().addUpload(upload).build());
            return uploadId;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Streams a part's bytes into Syrups, then records the resulting {@link Part} in the manifest
     * under {@code (uploadId, partNumber)}. A re-upload of the same {@code partNumber} supersedes the
     * prior part (last-write-wins); its Syrup segments are enqueued as pending orphans for GC.
     *
     * @return the per-part CRC32C and length, useful to the gateway for ETag construction
     */
    public PartUploadResult uploadPart(String uploadId, int partNumber, InputStream data) {
        if (uploadId == null || uploadId.isEmpty()) {
            throw new ValidationException("uploadId is required");
        }
        if (partNumber < 1 || partNumber > config.multipartMaxParts()) {
            throw new ValidationException("partNumber must be in [1, " + config.multipartMaxParts()
                    + "]");
        }
        lock.writeLock().lock();
        try {
            rejectIfStalled();
            MultipartUploadState upload = manifest.current().multipartUploads().get(uploadId);
            if (upload == null) {
                throw new CandyNotFoundException(box.value(), uploadId);
            }
            // The bytes go to Syrups via the normal write path, exactly like a single PUT.
            SyrupWriteResult written = syrupManager.writeCandy(data);
            Validation.checkCandySize(written.contentLength(), config.sizeLimits());
            Part newPart = new Part(written.contentLength(), config.sizeLimits().chunkSizeBytes(),
                    written.crc32c(), written.segments());
            // Apply the manifest edit; on success the old part (if any) becomes a pending orphan.
            Part previous = upload.parts().get(partNumber);
            manifest.apply(ManifestEdit.builder()
                    .addPartUpsert(uploadId, partNumber, newPart)
                    .addedSyrups(syrupIds(newPart))
                    .build());
            recomputeOrphanSyrupsLocked(clock.currentTimeMillis());
            // Defensive: if we just superseded a part, its segments are reachable only through the
            // pending-orphan path now.
            if (previous != null) {
                // referenced-syrup recompute already enqueued any newly-orphaned ledger.
                LOG.debug("Superseded part {} of upload {} ({} bytes)", partNumber, uploadId,
                        previous.partLength());
            }
            return new PartUploadResult(written.crc32c(), written.contentLength());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** A part's Syrups join the live set, so they become orphans once nothing references them. */
    private static Set<Long> syrupIds(Part part) {
        Set<Long> ids = new LinkedHashSet<>();
        for (SegmentRef seg : part.segments()) {
            ids.add(seg.syrupId());
        }
        return ids;
    }

    /** Convenience: byte[] form of {@link #uploadPart}. */
    public PartUploadResult uploadPart(String uploadId, int partNumber, byte[] data) {
        return uploadPart(uploadId, partNumber, new ByteArrayInputStream(data));
    }

    /**
     * Server-side copy of a byte range of a live Candy into a part slot of an in-flight upload —
     * the engine reads the source bytes through {@link SyrupReader#readRange} and writes them into
     * Syrups as a fresh part (i.e. not zero-copy on the byte stream itself; the optimization to share
     * Syrup segments when the range aligns to chunk boundaries is a future refinement). Same-Box
     * only, mirroring {@link #copyCandy}.
     */
    public PartUploadResult uploadPartCopy(String uploadId, int partNumber, CandyKey src,
                                           long firstByte, long lastByte) {
        Validation.checkCandyKey(src, config.sizeLimits());
        if (uploadId == null || uploadId.isEmpty()) {
            throw new ValidationException("uploadId is required");
        }
        if (partNumber < 1 || partNumber > config.multipartMaxParts()) {
            throw new ValidationException("partNumber must be in [1, " + config.multipartMaxParts()
                    + "]");
        }
        lock.writeLock().lock();
        try {
            rejectIfStalled();
            MultipartUploadState upload = manifest.current().multipartUploads().get(uploadId);
            if (upload == null) {
                throw new CandyNotFoundException(box.value(), uploadId);
            }
            CandyLocator source = resolveLiveLocked(src)
                    .orElseThrow(() -> new CandyNotFoundException(box.value(), src.value()));
            long total = source.contentLength();
            long resolvedFirst = firstByte < 0 ? 0 : firstByte;
            long resolvedLast = lastByte < 0 ? total - 1 : Math.min(lastByte, total - 1);
            if (total == 0 || resolvedFirst >= total || resolvedLast < resolvedFirst) {
                throw new ValidationException("InvalidRange: copy-source-range " + firstByte + "-"
                        + lastByte + " not satisfiable (object length " + total + ")");
            }
            // v1: buffer the slice in memory, then write through the normal Syrup chunker. The slice
            // size is bounded by the same per-part cap that already applies to UploadPart, so this is
            // no worse than a regular UploadPart of the same bytes. A true zero-copy that shares
            // Syrup segments when the source range aligns to chunk boundaries is a future refinement.
            java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream(
                    (int) Math.min(resolvedLast - resolvedFirst + 1, Integer.MAX_VALUE));
            syrupReader.readRange(source.parts(), resolvedFirst, resolvedLast, buffer);
            SyrupWriteResult written = syrupManager.writeCandy(
                    new java.io.ByteArrayInputStream(buffer.toByteArray()));
            Validation.checkCandySize(written.contentLength(), config.sizeLimits());
            Part newPart = new Part(written.contentLength(), config.sizeLimits().chunkSizeBytes(),
                    written.crc32c(), written.segments());
            manifest.apply(ManifestEdit.builder()
                    .addPartUpsert(uploadId, partNumber, newPart)
                    .addedSyrups(syrupIds(newPart))
                    .build());
            recomputeOrphanSyrupsLocked(clock.currentTimeMillis());
            return new PartUploadResult(written.crc32c(), written.contentLength());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Materializes a multipart upload as a single multi-part {@link CandyLocator} at the upload's
     * target key. {@code expectedParts} must enumerate every recorded part in ascending order,
     * matching the recorded CRC32C — this is the S3 {@code CompleteMultipartUpload} contract and the
     * server-side validation that the client saw what the server has. Every part except the last must
     * be at least {@link CandyboxConfig#multipartMinPartBytes()} bytes.
     *
     * <p>Atomically (under the write lock): builds the assembled locator, appends the mutation to the
     * WAL, drops the upload from the manifest, and applies to the memtable. Fencing-gated.
     *
     * @return metadata for the materialized Candy, identical to a single-PUT response shape
     */
    public CandyMetadata completeMultipartUpload(String uploadId, java.util.List<PartCompletion> expectedParts,
                                                 String idempotencyToken) {
        return completeMultipartUpload(uploadId, expectedParts, idempotencyToken, ObjectAcl.NONE);
    }

    /** {@link #completeMultipartUpload(String, java.util.List, String)} stamping owner/grants. */
    public CandyMetadata completeMultipartUpload(String uploadId, java.util.List<PartCompletion> expectedParts,
                                                 String idempotencyToken, ObjectAcl acl) {
        if (uploadId == null || uploadId.isEmpty()) {
            throw new ValidationException("uploadId is required");
        }
        if (expectedParts == null || expectedParts.isEmpty()) {
            throw new ValidationException("CompleteMultipartUpload requires at least one part");
        }
        CandyMetadata cached = idempotentResult(idempotencyToken);
        if (cached != null) {
            return cached;
        }
        lock.writeLock().lock();
        try {
            CandyMetadata replay = idempotentResult(idempotencyToken);
            if (replay != null) {
                return replay;
            }
            rejectIfStalled();
            MultipartUploadState upload = manifest.current().multipartUploads().get(uploadId);
            if (upload == null) {
                throw new CandyNotFoundException(box.value(), uploadId);
            }
            java.util.List<Part> ordered = new java.util.ArrayList<>(expectedParts.size());
            int lastPartNumber = 0;
            for (int i = 0; i < expectedParts.size(); i++) {
                PartCompletion completion = expectedParts.get(i);
                if (completion.partNumber() <= lastPartNumber) {
                    throw new ValidationException("Parts must be in strictly ascending partNumber order");
                }
                lastPartNumber = completion.partNumber();
                Part recorded = upload.parts().get(completion.partNumber());
                if (recorded == null) {
                    throw new ValidationException("Unknown part " + completion.partNumber()
                            + " in upload " + uploadId);
                }
                if (recorded.crc32c() != completion.crc32c()) {
                    throw new ValidationException("Part " + completion.partNumber()
                            + " CRC32C does not match the recorded part");
                }
                boolean isLast = i == expectedParts.size() - 1;
                if (!isLast && recorded.partLength() < config.multipartMinPartBytes()) {
                    throw new ValidationException("Part " + completion.partNumber()
                            + " is below the multipart minimum (" + config.multipartMinPartBytes()
                            + " bytes)");
                }
                ordered.add(recorded);
            }
            Hlc stamp = hlc.tick();
            CandyKey targetKey = CandyKey.of(upload.key());
            CandyLocator locator = new CandyLocator(stamp, LocatorType.PUT, upload.contentType(),
                    upload.userMetadata(), clock.currentTimeMillis(), ordered, acl);
            Mutation mutation = new Mutation(targetKey, locator);
            // Validate that the assembled locator stays within the configured cap before we write
            // anything irrevocable. (Re-serialized by SSTableWriter later anyway.)
            me.predatorray.candybox.common.serial.CandyLocatorSerializer.serialize(locator,
                    config.sizeLimits().maxLocatorBytes());

            wal.append(mutation);
            manifest.apply(ManifestEdit.builder().removedUploads(Set.of(uploadId)).build());
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

    /**
     * Aborts a multipart upload: drops the entry from the manifest (fenced). Each of the upload's
     * recorded parts' Syrup segments now becomes orphan-reclaimable through the usual path.
     *
     * <p>Idempotent: aborting a non-existent {@code uploadId} is a no-op (matches S3 behavior of
     * returning 204 even for a missing upload).
     */
    public void abortMultipartUpload(String uploadId) {
        if (uploadId == null || uploadId.isEmpty()) {
            throw new ValidationException("uploadId is required");
        }
        lock.writeLock().lock();
        try {
            if (!manifest.current().multipartUploads().containsKey(uploadId)) {
                return;
            }
            manifest.apply(ManifestEdit.builder().removedUploads(Set.of(uploadId)).build());
            recomputeOrphanSyrupsLocked(clock.currentTimeMillis());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Snapshot of an in-flight multipart upload, or {@code null} if none. Returns a defensive copy so
     * callers can iterate freely.
     */
    public MultipartUploadState multipartUpload(String uploadId) {
        return manifest.current().multipartUploads().get(uploadId);
    }

    /** Snapshot of every in-flight multipart upload in the Box, in insertion order. */
    public java.util.List<MultipartUploadState> listMultipartUploads() {
        return new java.util.ArrayList<>(manifest.current().multipartUploads().values());
    }

    /**
     * Per-part result of an {@link #uploadPart} call: the CRC32C the gateway echoes back as the part
     * ETag, and the exact byte count the server received.
     */
    public record PartUploadResult(int crc32c, long partLength) {
    }

    /**
     * One element of the client-supplied {@code CompleteMultipartUpload} part list — the part number
     * plus the CRC32C the client believes that part has (returned from the matching {@link #uploadPart}
     * call). The server checks both against its recorded {@link Part}.
     */
    public record PartCompletion(int partNumber, int crc32c) {
    }

    private static String generateUploadId() {
        byte[] raw = new byte[16];
        new java.security.SecureRandom().nextBytes(raw);
        // Base32 (Crockford-ish, no padding) without depending on java.util.Base64's URL-safety quirks.
        char[] alphabet = "0123456789abcdefghjkmnpqrstvwxyz".toCharArray();
        StringBuilder sb = new StringBuilder(26);
        long high = ((long) (raw[0] & 0xFF) << 56) | ((long) (raw[1] & 0xFF) << 48)
                | ((long) (raw[2] & 0xFF) << 40) | ((long) (raw[3] & 0xFF) << 32)
                | ((long) (raw[4] & 0xFF) << 24) | ((long) (raw[5] & 0xFF) << 16)
                | ((long) (raw[6] & 0xFF) << 8) | (raw[7] & 0xFF);
        long low = ((long) (raw[8] & 0xFF) << 56) | ((long) (raw[9] & 0xFF) << 48)
                | ((long) (raw[10] & 0xFF) << 40) | ((long) (raw[11] & 0xFF) << 32)
                | ((long) (raw[12] & 0xFF) << 24) | ((long) (raw[13] & 0xFF) << 16)
                | ((long) (raw[14] & 0xFF) << 8) | (raw[15] & 0xFF);
        for (int i = 0; i < 13; i++) {
            sb.append(alphabet[(int) ((high >>> (60 - i * 5)) & 0x1F)]);
        }
        for (int i = 0; i < 13; i++) {
            sb.append(alphabet[(int) ((low >>> (60 - i * 5)) & 0x1F)]);
        }
        return sb.toString();
    }

    // ---- single-key writes ------------------------------------------------------------------

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
        return copyCandy(src, dst, idempotencyToken, ObjectAcl.NONE);
    }

    /**
     * {@link #copyCandy(CandyKey, CandyKey, String)} stamping the destination's owner/grants. Per
     * S3 CopyObject semantics the destination does <em>not</em> inherit the source's ACL — the
     * requester owns the copy (the caller passes that identity here).
     */
    public CandyMetadata copyCandy(CandyKey src, CandyKey dst, String idempotencyToken,
                                   ObjectAcl dstAcl) {
        return copyOrRename(src, dst, idempotencyToken, false, dstAcl);
    }

    /**
     * Server-side, zero-copy rename/move: like {@link #copyCandy} but also writes a DELETE tombstone at
     * {@code src}, atomically (the single owner serializes both into the WAL and memtable under one
     * write lock). The Candy's bytes never move; only locator pointers are rewritten. Same-Box only.
     *
     * @throws CandyNotFoundException if there is no live Candy at {@code src}
     */
    public CandyMetadata renameCandy(CandyKey src, CandyKey dst, String idempotencyToken) {
        // A rename is a move: the object keeps its identity, so owner + grants travel with it.
        return copyOrRename(src, dst, idempotencyToken, true, null);
    }

    /** {@code dstAcl == null} ⇒ keep the source's owner/grants (rename); else stamp it (copy). */
    private CandyMetadata copyOrRename(CandyKey src, CandyKey dst, String idempotencyToken,
                                       boolean tombstoneSource, ObjectAcl dstAcl) {
        Validation.checkCandyKey(src, config.sizeLimits());
        Validation.checkCandyKey(dst, config.sizeLimits());
        if (src.equals(dst)) {
            throw new ValidationException("source and destination keys must differ");
        }
        CandyMetadata cached = idempotentResult(idempotencyToken);
        if (cached != null) {
            return cached;
        }
        lock.writeLock().lock();
        try {
            CandyMetadata replay = idempotentResult(idempotencyToken);
            if (replay != null) {
                return replay;
            }
            rejectIfStalled();
            CandyLocator source = resolveLiveLocked(src)
                    .orElseThrow(() -> new CandyNotFoundException(box.value(), src.value()));

            // The destination locator reuses the source's parts verbatim — zero copy of the bytes
            // *and* zero rebuild of the part list, so a multipart source becomes a multipart-shaped
            // copy with identical Syrup references.
            Hlc stamp = hlc.tick();
            CandyLocator dstLocator = new CandyLocator(stamp, LocatorType.PUT, source.contentType(),
                    source.userMetadata(), clock.currentTimeMillis(), source.parts(),
                    dstAcl == null ? source.acl() : dstAcl);
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

    // ---- cross-partition zero-copy (copy/rename across partitions) ---------------------------

    /**
     * Resolves a key to its live {@link CandyLocator}, for a cross-partition copy/rename: the client
     * relays the returned parts to the destination partition's owner (which has no way to read this
     * partition's manifest directly), where {@link #zeroCopyPut} reuses the segments verbatim.
     *
     * @throws CandyNotFoundException if there is no live Candy at {@code key}
     */
    public CandyLocator resolveLocator(CandyKey key) {
        Validation.checkCandyKey(key, config.sizeLimits());
        return resolveLive(key)
                .orElseThrow(() -> new CandyNotFoundException(box.value(), key.value()));
    }

    /**
     * Writes a fresh PUT at {@code dst} that reuses {@code parts} verbatim — the zero-copy trick, but
     * across partitions: the {@code parts} were resolved from a source Candy in <em>another</em>
     * partition (via {@link #resolveLocator} relayed by the client). No Candy bytes are read or
     * rewritten; the destination locator points at the very same Syrup segments, which stay alive
     * Box-globally because every partition publishes its referenced-Syrup set (see the server's
     * Box-global GC). Same-Box only.
     */
    public CandyMetadata zeroCopyPut(CandyKey dst, List<Part> parts, String contentType,
                                     Map<String, String> userMetadata, long createdAtMillis,
                                     ObjectAcl acl, String idempotencyToken) {
        Validation.checkCandyKey(dst, config.sizeLimits());
        Map<String, String> metadata = userMetadata == null ? Map.of() : Map.copyOf(userMetadata);
        CandyMetadata cached = idempotentResult(idempotencyToken);
        if (cached != null) {
            return cached;
        }
        lock.writeLock().lock();
        try {
            CandyMetadata replay = idempotentResult(idempotencyToken);
            if (replay != null) {
                return replay;
            }
            rejectIfStalled();
            Hlc stamp = hlc.tick();
            CandyLocator dstLocator = new CandyLocator(stamp, LocatorType.PUT, contentType, metadata,
                    createdAtMillis > 0 ? createdAtMillis : clock.currentTimeMillis(),
                    List.copyOf(parts), acl == null ? ObjectAcl.NONE : acl);
            Mutation mutation = new Mutation(dst, dstLocator);
            wal.append(mutation);
            active.put(mutation);
            // The parts may point at a Syrup this partition already counts as a pending orphan. Clear it
            // here so its grace period restarts: the next recompute may only run once this reference is
            // gone again and would keep the old stamp, and claim-time re-validation only guards safety.
            for (SegmentRef seg : dstLocator.segments()) {
                pendingOrphanSyrups.remove(seg.syrupId());
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

    /**
     * Conditionally tombstones {@code key} only if its live locator's HLC equals {@code expectedHlc}
     * — the LWW-safe source delete that finalizes a cross-partition rename. If the source was already
     * deleted, or legitimately re-{@code put} after the rename began (a strictly newer HLC), the
     * delete is a no-op so a delayed/duplicated finalize can never clobber a newer value.
     *
     * @return {@code true} if a tombstone was written, {@code false} if the condition did not hold
     */
    public boolean deleteCandyConditional(CandyKey key, Hlc expectedHlc) {
        Validation.checkCandyKey(key, config.sizeLimits());
        lock.writeLock().lock();
        try {
            rejectIfStalled();
            CandyLocator live = resolveLiveLocked(key).orElse(null);
            if (live == null || expectedHlc == null || !live.hlc().equals(expectedHlc)) {
                return false;
            }
            Hlc stamp = hlc.tick();
            Mutation mutation = new Mutation(key, CandyLocator.tombstone(stamp,
                    clock.currentTimeMillis()));
            wal.append(mutation);
            active.put(mutation);
            maybeFlushLocked();
            deleteCount.incrementAndGet();
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Records a cross-partition {@link RenameIntent} this (source) partition owes a delete for. */
    public void recordRenameIntent(RenameIntent intent) {
        lock.writeLock().lock();
        try {
            manifest.apply(ManifestEdit.builder().addRenameIntent(intent).build());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Clears a recorded rename intent once it has been finalized or abandoned. */
    public void clearRenameIntent(String token) {
        lock.writeLock().lock();
        try {
            manifest.apply(ManifestEdit.builder()
                    .removedRenameIntents(java.util.Set.of(token)).build());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** The cross-partition rename intents this partition still owes a source delete for. */
    public List<RenameIntent> listRenameIntents() {
        return new java.util.ArrayList<>(manifest.current().renameIntents().values());
    }

    /**
     * The set of Syrup ids this partition currently references — manifest SSTable refs, in-flight
     * multipart parts, the memtable, and the open write Syrup. Published to coordination so the
     * Box-global GC never reclaims a Syrup a sibling partition still points at (cross-partition
     * zero-copy copy/rename).
     */
    public java.util.Set<Long> referencedSyrups() {
        lock.readLock().lock();
        try {
            return referencedSyrupsLocked();
        } finally {
            lock.readLock().unlock();
        }
    }

    private java.util.Set<Long> referencedSyrupsLocked() {
        ManifestState current = manifest.current();
        java.util.Set<Long> referenced = new java.util.HashSet<>(current.referencedSyrups());
        // In-flight multipart uploads pin their parts' Syrups until Complete or Abort fires.
        referenced.addAll(current.multipartReferencedSyrups());
        for (java.util.Iterator<Mutation> it = active.iterator(); it.hasNext(); ) {
            for (SegmentRef seg : it.next().locator().segments()) {
                referenced.add(seg.syrupId());
            }
        }
        long openSyrup = syrupManager.currentSyrupId();
        if (openSyrup >= 0) {
            referenced.add(openSyrup);
        }
        return referenced;
    }

    /**
     * Deletes every live Candy whose key falls in {@code [startInclusive, endExclusive)} with a single
     * O(1) range tombstone — no per-key scan or write. Either bound may be null (null start = from the
     * beginning of the keyspace, null end = to the end). Keys written later (with a higher HLC) are not
     * affected. The shadowed Candies' bytes are reclaimed lazily at compaction (DESIGN §8,§9).
     */
    public void deleteRange(CandyKey startInclusive, CandyKey endExclusive) {
        if (startInclusive != null) {
            Validation.checkCandyKey(startInclusive, config.sizeLimits());
        }
        if (endExclusive != null) {
            Validation.checkCandyKey(endExclusive, config.sizeLimits());
        }
        lock.writeLock().lock();
        try {
            rejectIfStalled();
            RangeTombstone tombstone = new RangeTombstone(startInclusive, endExclusive, hlc.tick());
            wal.append(tombstone);
            active.delete(tombstone);
            maybeFlushLocked();
            deleteCount.incrementAndGet();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Deletes every live Candy whose key starts with {@code prefix} via a single range tombstone over
     * {@code [prefix, prefixSuccessor)}. An empty prefix deletes the whole Box.
     */
    public void deleteRangeByPrefix(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            deleteRange(null, null);
            return;
        }
        CandyKey start = CandyKey.of(prefix);
        byte[] successor = me.predatorray.candybox.common.util.Bytes.prefixSuccessor(start.utf8Bytes());
        deleteRange(start, successor == null ? null : CandyKey.ofUtf8(successor));
    }

    // ---- object ACLs -------------------------------------------------------------------------

    /**
     * The live Candy's owner + grants (possibly {@link ObjectAcl#NONE} for pre-auth objects).
     *
     * @throws CandyNotFoundException if there is no live Candy at {@code key}
     */
    public ObjectAcl getCandyAcl(CandyKey key) {
        CandyLocator locator = resolveLive(key)
                .orElseThrow(() -> new CandyNotFoundException(box.value(), key.value()));
        return locator.acl();
    }

    /**
     * Replaces the live Candy's owner/grants with a metadata-only locator rewrite: the new locator
     * reuses the parts verbatim (the zero-copy trick), gets a fresh HLC, and goes through the normal
     * WAL + memtable path — so it is fenced, durable, and replayed on handover like any write. The
     * shadowed locator shares the same segments, so GC's reference counting is undisturbed.
     *
     * @throws CandyNotFoundException if there is no live Candy at {@code key}
     */
    public CandyMetadata setCandyAcl(CandyKey key, ObjectAcl acl) {
        Validation.checkCandyKey(key, config.sizeLimits());
        lock.writeLock().lock();
        try {
            rejectIfStalled();
            CandyLocator current = resolveLiveLocked(key)
                    .orElseThrow(() -> new CandyNotFoundException(box.value(), key.value()));
            CandyLocator updated = current.withAcl(hlc.tick(), acl);
            Mutation mutation = new Mutation(key, updated);
            wal.append(mutation);
            active.put(mutation);
            maybeFlushLocked();
            putCount.incrementAndGet();
            return CandyMetadata.from(updated);
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
     * Streams a Candy's bytes to {@code out}, validating each part's end-to-end CRC32C, and returns
     * its metadata. A single-PUT Candy is one Part so this reduces to a single end-to-end check;
     * multipart Candies validate per part as they stream.
     *
     * @throws CandyNotFoundException if there is no live Candy at {@code key}
     */
    public CandyMetadata getCandy(CandyKey key, OutputStream out) {
        return getCandy(key, out, Long.MAX_VALUE);
    }

    /**
     * {@link #getCandy(CandyKey, OutputStream)}, refusing up front (before any bytes are read) a
     * Candy longer than {@code maxBytes}.
     *
     * @throws LimitExceededException if the Candy is longer than {@code maxBytes}
     */
    public CandyMetadata getCandy(CandyKey key, OutputStream out, long maxBytes) {
        CandyLocator locator = resolveLive(key)
                .orElseThrow(() -> new CandyNotFoundException(box.value(), key.value()));
        checkReadSize(locator.contentLength(), maxBytes);
        syrupReader.readParts(locator.parts(), out);
        getCount.incrementAndGet();
        return CandyMetadata.from(locator);
    }

    /**
     * Streams a byte window of a Candy to {@code out}, S3 / HTTP {@code Range: bytes=A-B} semantics
     * (both ends inclusive). {@code firstByte} and {@code lastByte} are resolved against the
     * Candy's total length:
     *
     * <ul>
     *   <li>{@code firstByte >= 0, lastByte >= firstByte} → that explicit window;</li>
     *   <li>{@code lastByte < 0} → "from firstByte to end" ({@code bytes=A-});</li>
     *   <li>{@code firstByte < 0, lastByte >= 0} → suffix range ({@code bytes=-N}); the last
     *       {@code lastByte} bytes of the object.</li>
     * </ul>
     *
     * <p>A range that lies entirely beyond {@code contentLength} fails with
     * {@link IllegalArgumentException}; a range that overhangs is clamped at the end. Per-chunk
     * CRC32C still validates; the part-level CRC is not verified for a partial slice (see
     * {@link me.predatorray.candybox.lsm.syrup.SyrupReader#readRange}).
     *
     * @return a {@link RangeReadResult} carrying the resolved {@code firstByte}/{@code lastByte},
     *         the {@code totalLength}, and the Candy metadata
     * @throws CandyNotFoundException     if there is no live Candy at {@code key}
     * @throws IllegalArgumentException   if the resolved range is not satisfiable (S3
     *                                    {@code InvalidRange})
     */
    public RangeReadResult getCandyRange(CandyKey key, long firstByte, long lastByte,
                                         OutputStream out) {
        return getCandyRange(key, firstByte, lastByte, out, Long.MAX_VALUE);
    }

    /**
     * {@link #getCandyRange(CandyKey, long, long, OutputStream)}, refusing up front a resolved
     * window longer than {@code maxBytes}.
     *
     * @throws LimitExceededException if the resolved window is longer than {@code maxBytes}
     */
    public RangeReadResult getCandyRange(CandyKey key, long firstByte, long lastByte,
                                         OutputStream out, long maxBytes) {
        CandyLocator locator = resolveLive(key)
                .orElseThrow(() -> new CandyNotFoundException(box.value(), key.value()));
        long total = locator.contentLength();
        long resolvedFirst;
        long resolvedLast;
        if (firstByte < 0 && lastByte < 0) {
            throw new IllegalArgumentException("Invalid range: neither bound supplied");
        }
        if (firstByte < 0) {
            // Suffix range: the last `lastByte` bytes.
            long suffix = lastByte;
            if (suffix <= 0) {
                throw new IllegalArgumentException("Suffix range must be positive");
            }
            if (suffix >= total) {
                resolvedFirst = 0;
            } else {
                resolvedFirst = total - suffix;
            }
            resolvedLast = total - 1;
        } else if (lastByte < 0) {
            // "From firstByte to end".
            resolvedFirst = firstByte;
            resolvedLast = total - 1;
        } else {
            resolvedFirst = firstByte;
            resolvedLast = Math.min(lastByte, total - 1);
        }
        if (total == 0 || resolvedFirst >= total) {
            throw new IllegalArgumentException("Range " + firstByte + "-" + lastByte
                    + " not satisfiable (object length " + total + ")");
        }
        if (resolvedLast < resolvedFirst) {
            throw new IllegalArgumentException("Range " + firstByte + "-" + lastByte
                    + " is empty after resolution");
        }
        checkReadSize(resolvedLast - resolvedFirst + 1, maxBytes);
        syrupReader.readRange(locator.parts(), resolvedFirst, resolvedLast, out);
        getCount.incrementAndGet();
        return new RangeReadResult(resolvedFirst, resolvedLast, total, CandyMetadata.from(locator));
    }

    private static void checkReadSize(long length, long maxBytes) {
        if (length > maxBytes) {
            throw new LimitExceededException("Read of " + length
                    + " bytes exceeds the single-response maximum of " + maxBytes
                    + " bytes; use range reads");
        }
    }

    /**
     * Result of a {@link #getCandyRange} call. {@code firstByte} and {@code lastByte} are the
     * resolved bounds the server emitted (both inclusive), {@code totalLength} is the whole object's
     * length, and {@code metadata} carries the Candy's content-type / user-metadata / created-at.
     */
    public record RangeReadResult(long firstByte, long lastByte, long totalLength,
                                  CandyMetadata metadata) {
        public long contentLength() {
            return lastByte - firstByte + 1;
        }
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
     *
     * <p>With a delimiter, a key containing it after the prefix rolls up into a common prefix that
     * counts as one row, and the merged iterator seeks past the rest of that prefix in place: a
     * target inside an already loaded SSTable block costs no read. The
     * cursor may then be a common prefix (the last row of a previous page); the scan resumes past it.
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
            String delimiter = query.delimiter();
            String prefix = query.prefix() == null ? "" : query.prefix();
            if (delimiter != null && forward && cursor != null
                    && cursor.value().equals(commonPrefix(cursor.value(), prefix, delimiter))) {
                // Reverse needs no skip: every key under the cursor prefix sorts at or above it.
                CandyKey past = prefixSuccessor(cursor.value());
                if (past == null) {
                    return new ListResult(List.of(), null);
                }
                lower = maxKey(lower, past);
            }

            MergingIterator merged = mergedView(lower, upper, cursor, query.direction());
            List<RangeTombstone> rangeTombstones = gatherRangeTombstonesLocked();
            List<ListResult.ListEntry> entries = new ArrayList<>();
            List<String> commonPrefixes = new ArrayList<>();
            String last = null;
            String next = null;
            while (merged.hasNext()) {
                Mutation m = merged.next();
                CandyKey key = m.key();
                if (isShadowedByRange(rangeTombstones, key, m.hlc())) {
                    continue; // a newer range tombstone deletes this key
                }
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
                if (entries.size() + commonPrefixes.size() == limit) {
                    next = last;
                    break;
                }
                String rolled = delimiter == null ? null
                        : commonPrefix(key.value(), prefix, delimiter);
                if (rolled == null) {
                    CandyLocator loc = m.locator();
                    entries.add(new ListResult.ListEntry(key, loc.contentLength(),
                            loc.createdAtMillis()));
                    last = key.value();
                    continue;
                }
                commonPrefixes.add(rolled);
                last = rolled;
                // Skip the rest of this common prefix; the window checks above drop any straggler.
                if (forward) {
                    lower = prefixSuccessor(rolled);
                    if (lower == null) {
                        break;
                    }
                    merged.seek(lower);
                } else {
                    upper = CandyKey.of(rolled);
                    merged.seek(upper);
                }
            }
            listCount.incrementAndGet();
            return new ListResult(entries, next, commonPrefixes);
        } finally {
            lock.readLock().unlock();
        }
    }

    // ---- compaction (execution exposed; scheduling is Phase 3) ------------------------------

    /**
     * Commits a compaction's manifest edit (swap inputs for output) and refreshes the SSTable readers.
     * The removed input ledgers become pending deletes in the same edit (stamped with the time they
     * left the committed manifest) for GC to delete after the grace period.
     */
    public void applyCompaction(ManifestEdit edit) {
        lock.writeLock().lock();
        try {
            long now = clock.currentTimeMillis();
            Map<Long, Long> obsolete = new LinkedHashMap<>();
            for (Long removed : edit.removedTableLedgerIds()) {
                obsolete.put(removed, now);
            }
            // fencing-gated: throws if this owner has been superseded
            manifest.apply(edit.withAddedPendingDeletes(obsolete));
            for (SSTableMeta added : edit.addedTables()) {
                readers.computeIfAbsent(added.ledgerId(), id -> new SSTableReader(ledgerStore, id));
            }
            for (Long removed : edit.removedTableLedgerIds()) {
                SSTableReader r = readers.remove(removed);
                if (r != null) {
                    r.close();
                }
            }
            recomputeOrphanSyrupsLocked(now);
            compactionCount.incrementAndGet();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Recomputes which live Syrups are no longer referenced by any SSTable, in-flight multipart upload,
     * the active memtable, or the currently open write Syrup, recording newly-orphaned ones with the
     * time first seen. A cross-partition zero-copy put can re-reference an orphan, so pending entries
     * that are referenced again are dropped; {@link #zeroCopyPut} also clears them eagerly, since a
     * Syrup can be re-referenced and orphaned again between two recomputes. Called under the write
     * lock after a manifest change.
     */
    private void recomputeOrphanSyrupsLocked(long now) {
        java.util.Set<Long> referenced = referencedSyrupsLocked();
        pendingOrphanSyrups.keySet().removeIf(referenced::contains);
        for (Long syrup : manifest.current().liveSyrups()) {
            if (!referenced.contains(syrup)) {
                pendingOrphanSyrups.putIfAbsent(syrup, now);
            }
        }
    }

    /**
     * Claims the orphaned Syrups that have been pending since at or before {@code asOfMillis} and are
     * in neither this partition's current references nor {@code foreignReferencedSyrups}, moving
     * them from the live set to the pending deletes via a fencing-gated manifest edit (keeping their
     * orphan time, so they are due at once). References are re-validated under the write lock, so a
     * Syrup re-referenced since it was orphaned is never claimed.
     */
    public java.util.List<Long> claimReclaimableSyrups(long asOfMillis,
                                                       java.util.Set<Long> foreignReferencedSyrups) {
        lock.writeLock().lock();
        try {
            java.util.Set<Long> referenced = referencedSyrupsLocked();
            pendingOrphanSyrups.keySet().removeIf(referenced::contains);
            Map<Long, Long> claimed = new LinkedHashMap<>();
            for (Map.Entry<Long, Long> e : pendingOrphanSyrups.entrySet()) {
                if (e.getValue() <= asOfMillis && !foreignReferencedSyrups.contains(e.getKey())) {
                    claimed.put(e.getKey(), e.getValue());
                }
            }
            if (!claimed.isEmpty()) {
                manifest.apply(ManifestEdit.builder()
                        .removedSyrups(claimed.keySet())
                        .addedPendingDeletes(claimed)
                        .build());
                claimed.keySet().forEach(pendingOrphanSyrups::remove);
                unflushedSyrups.removeAll(claimed.keySet());
            }
            return new java.util.ArrayList<>(claimed.keySet());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Test hook: records {@code syrupId} as a pending orphan since {@code sinceMillis}. */
    void trackPendingOrphan(long syrupId, long sinceMillis) {
        lock.writeLock().lock();
        try {
            pendingOrphanSyrups.put(syrupId, sinceMillis);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Pending-delete ledger ids (compacted-away SSTables, rotated or superseded WALs, superseded
     * manifests, claimed Syrups) that left the committed state at or before {@code asOfMillis} — the
     * GC reclaim set for this partition. Defensively excludes any ledger the state still uses.
     */
    public java.util.List<Long> reclaimableLedgers(long asOfMillis) {
        lock.readLock().lock();
        try {
            ManifestState state = manifest.current();
            java.util.List<Long> ids = new java.util.ArrayList<>();
            for (Map.Entry<Long, Long> e : state.pendingDeletes().entrySet()) {
                if (e.getValue() <= asOfMillis) {
                    ids.add(e.getKey());
                }
            }
            if (ids.isEmpty()) {
                return ids;
            }
            Set<Long> inUse = referencedSyrupsLocked();
            inUse.addAll(state.liveSyrups());
            state.tables().forEach(t -> inUse.add(t.ledgerId()));
            inUse.add(state.walLedgerId());
            inUse.add(wal.ledgerId());
            inUse.add(manifest.ledgerId());
            for (Iterator<Long> it = ids.iterator(); it.hasNext(); ) {
                long id = it.next();
                if (inUse.contains(id)) {
                    // A bookkeeping bug: something was scheduled for deletion while still in use.
                    LOG.warn("Not deleting pending ledger {} of box {}: it is still in use", id, box);
                    it.remove();
                }
            }
            return ids;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Drops ledger ids from the pending deletes once GC has physically deleted them (fenced). */
    public void forgetDeletedLedgers(java.util.Collection<Long> ledgerIds) {
        if (ledgerIds.isEmpty()) {
            return;
        }
        lock.writeLock().lock();
        try {
            manifest.apply(ManifestEdit.builder()
                    .removedPendingDeletes(new LinkedHashSet<>(ledgerIds)).build());
        } finally {
            lock.writeLock().unlock();
        }
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

    /**
     * A previously stored idempotent result for {@code token}, or {@code null} if none (or no token).
     * Checked both on the unlocked fast path (to skip the write lock for an obvious retry) and again
     * under the write lock, so two concurrent retries that both miss the fast path cannot both apply
     * the mutation — the second sees the first's cached result and returns it.
     */
    private CandyMetadata idempotentResult(String token) {
        return token == null ? null : idempotencyCache.get(token);
    }

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
        // A range tombstone newer than the best point locator shadows the key (range delete).
        Hlc deleteFloor = maxRangeTombstoneCoveringLocked(key);
        if (deleteFloor != null && deleteFloor.isAfter(best.hlc())) {
            return Optional.empty();
        }
        return Optional.of(best);
    }

    /**
     * The highest HLC among range tombstones covering {@code key} across the memtable and every open
     * SSTable. Range tombstones can extend beyond a table's point-key range, so this consults all
     * readers (they are few; tombstones merge away at compaction). Caller holds the lock.
     */
    private Hlc maxRangeTombstoneCoveringLocked(CandyKey key) {
        Hlc max = active.maxRangeTombstoneCovering(key);
        for (SSTableReader reader : readers.values()) {
            Hlc h = reader.maxRangeTombstoneCovering(key);
            if (h != null && (max == null || h.isAfter(max))) {
                max = h;
            }
        }
        return max;
    }

    /** The union of range tombstones across the memtable and all open SSTables. Caller holds the lock. */
    private List<RangeTombstone> gatherRangeTombstonesLocked() {
        List<RangeTombstone> all = new ArrayList<>(active.rangeTombstones());
        for (SSTableReader reader : readers.values()) {
            all.addAll(reader.rangeTombstones());
        }
        return all;
    }

    /** Whether a range tombstone newer than {@code keyHlc} covers {@code key} (so it is deleted). */
    private static boolean isShadowedByRange(List<RangeTombstone> rangeTombstones, CandyKey key,
                                             Hlc keyHlc) {
        for (RangeTombstone rt : rangeTombstones) {
            if (rt.hlc().isAfter(keyHlc) && rt.covers(key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds a tombstone-suppressed merged view over the window {@code [lower, upper)} in the given
     * direction, seeking each source to the appropriate bound and pruning SSTables that cannot overlap
     * the window. The caller still applies exact bound/cursor filtering on the emitted keys.
     */
    private MergingIterator mergedView(CandyKey lower, CandyKey upper, CandyKey cursor,
                                       ScanDirection direction) {
        List<Iterator<Mutation>> sources = new ArrayList<>();
        Memtable mem = active;
        if (direction == ScanDirection.FORWARD) {
            CandyKey seek = maxKey(lower, cursor);
            sources.add(new ReopeningIterator(k -> k == null ? mem.iterator() : mem.iterator(k), seek,
                    direction));
            for (SSTableReader reader : readers.values()) {
                if (overlapsWindow(reader.minKey(), reader.maxKey(), lower, upper)) {
                    sources.add(reader.scan(seek));
                }
            }
        } else {
            CandyKey seekUpper = minKey(upper, cursor);
            sources.add(new ReopeningIterator(k -> k == null ? mem.descendingIterator()
                    : mem.descendingIterator(k), seekUpper, direction));
            for (SSTableReader reader : readers.values()) {
                if (overlapsWindow(reader.minKey(), reader.maxKey(), lower, upper)) {
                    sources.add(reader.scanReverse(seekUpper));
                }
            }
        }
        return new MergingIterator(sources, true, direction);
    }

    /**
     * The common prefix {@code key} rolls up into: {@code key} up to and including the first
     * {@code delimiter} after {@code prefix}, or {@code null} if it has none (or lies outside the prefix).
     */
    private static String commonPrefix(String key, String prefix, String delimiter) {
        if (!key.startsWith(prefix)) {
            return null;
        }
        int idx = key.indexOf(delimiter, prefix.length());
        return idx < 0 ? null : key.substring(0, idx + delimiter.length());
    }

    /** The smallest key above every key starting with {@code prefix}, or {@code null} if none. */
    private static CandyKey prefixSuccessor(String prefix) {
        byte[] succ = me.predatorray.candybox.common.util.Bytes
                .prefixSuccessor(prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return succ == null ? null : CandyKey.ofUtf8(succ);
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
        L0Table table = writeL0(flushing);

        WriteAheadLog newWal = WriteAheadLog.create(ledgerStore, ledgerConfig(LedgerRole.WAL));
        // The rotated WAL's mutations are durable in the SSTable once this edit commits, and the
        // manifest then points at the new WAL, so it is no longer a recovery source.
        long now = clock.currentTimeMillis();
        manifest.apply(ManifestEdit.flush(table.meta(), table.syrups(), newWal.ledgerId(),
                table.maxHlc()).withAddedPendingDeletes(Map.of(wal.ledgerId(), now)));
        unflushedSyrups.clear();
        wal.close();
        wal = newWal;
        active = new Memtable();
        // A Syrup whose Candies were all superseded before this flush is an orphan already.
        recomputeOrphanSyrupsLocked(now);

        readers.put(table.meta().ledgerId(), new SSTableReader(ledgerStore, table.meta().ledgerId()));
        flushCount.incrementAndGet();
        LOG.debug("Flushed memtable of box {} to SSTable ledger {} ({} entries)", box,
                table.meta().ledgerId(), table.meta().entryCount());
    }

    private record L0Table(SSTableMeta meta, Set<Long> syrups, Hlc maxHlc) {
    }

    /**
     * Writes {@code flushing} to an L0 SSTable, collecting the Syrups it references (plus those created
     * since the last flush, for live-Syrup tracking / GC) and its max HLC.
     */
    private L0Table writeL0(Memtable flushing) {
        Set<Long> syrups = new LinkedHashSet<>(unflushedSyrups);
        Hlc maxHlc = Hlc.MIN;
        Iterator<Mutation> scan = flushing.iterator();
        while (scan.hasNext()) {
            Mutation m = scan.next();
            for (SegmentRef seg : m.locator().segments()) {
                syrups.add(seg.syrupId());
            }
            if (m.hlc().isAfter(maxHlc)) {
                maxHlc = m.hlc();
            }
        }
        for (RangeTombstone rt : flushing.rangeTombstones()) {
            if (rt.hlc().isAfter(maxHlc)) {
                maxHlc = rt.hlc();
            }
        }

        SSTableMeta table = sstableWriter.write(ledgerConfig(LedgerRole.SSTABLE), 0,
                flushing.iterator(), flushing.rangeTombstones());
        return new L0Table(table, syrups, maxHlc);
    }

    /** The quorum and metadata this Box's SSTable ledgers are created with, for compaction outputs. */
    public LedgerConfig sstableLedgerConfig() {
        return ledgerConfig(LedgerRole.SSTABLE);
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
