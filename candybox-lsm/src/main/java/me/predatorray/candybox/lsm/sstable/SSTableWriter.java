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
package me.predatorray.candybox.lsm.sstable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Hlc;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.RangeTombstone;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.bloom.BloomFilter;
import me.predatorray.candybox.common.serial.BinaryWriter;
import me.predatorray.candybox.common.serial.MutationSerializer;
import me.predatorray.candybox.common.serial.RangeTombstoneSerializer;
import me.predatorray.candybox.lsm.iterator.PeekingIterator;

/**
 * Writes a sorted run of mutations into a new SSTable ledger in the {@link SSTableFormat} layout, then
 * seals it. The input iterator must yield mutations in ascending key order with each key appearing
 * once (the memtable and merge guarantee this); the writer does not de-duplicate.
 */
public final class SSTableWriter {

    private final LedgerStore ledgerStore;
    private final int bloomBitsPerKey;
    private final int dataBlockTargetBytes;

    public SSTableWriter(LedgerStore ledgerStore, int bloomBitsPerKey) {
        this(ledgerStore, bloomBitsPerKey, SSTableFormat.DEFAULT_DATA_BLOCK_TARGET_BYTES);
    }

    public SSTableWriter(LedgerStore ledgerStore, int bloomBitsPerKey, int dataBlockTargetBytes) {
        this.ledgerStore = ledgerStore;
        this.bloomBitsPerKey = bloomBitsPerKey;
        this.dataBlockTargetBytes = dataBlockTargetBytes;
    }

    /** Writes a run with no range tombstones (a plain point-only SSTable). */
    public SSTableMeta write(LedgerConfig config, int level, Iterator<Mutation> sorted) {
        return write(config, level, sorted, List.of());
    }

    /**
     * Writes all mutations from {@code sorted}, plus any {@code rangeTombstones}, into a fresh SSTable
     * ledger. The table may be range-only (an empty {@code sorted} with non-empty tombstones).
     *
     * @param config          quorum/metadata for the SSTable ledger
     * @param level           the LSM level this table belongs to
     * @param sorted          ascending-key, unique-key mutations
     * @param rangeTombstones range tombstones to persist alongside the points (may be empty)
     * @return metadata for the sealed table
     * @throws IllegalArgumentException if there are neither mutations nor range tombstones
     */
    public SSTableMeta write(LedgerConfig config, int level, Iterator<Mutation> sorted,
                             Collection<RangeTombstone> rangeTombstones) {
        return write(config, level, sorted, rangeTombstones, Long.MAX_VALUE, null, null);
    }

    /**
     * Writes the next table of a run split into key-disjoint tables. Like
     * {@link #write(LedgerConfig, int, Iterator, Collection)}, but stops pulling from {@code sorted} once
     * the table holds at least {@code targetDataBytes} of data blocks, or its bloom filter or index
     * would outgrow {@link SSTableFormat#MAX_META_BLOCK_BYTES}; mutations not consumed stay in
     * {@code sorted} for the next table. The table keeps only the parts of {@code rangeTombstones}
     * inside its slot of the key space: from {@code slotStart} ({@code null} = unbounded) up to the
     * next key left in {@code sorted} (unbounded if none). Successive slots tile the key space, so no
     * range tombstone coverage is lost.
     */
    public SSTableMeta write(LedgerConfig config, int level, PeekingIterator<Mutation> sorted,
                             Collection<RangeTombstone> rangeTombstones, CandyKey slotStart,
                             long targetDataBytes) {
        return write(config, level, sorted, rangeTombstones, targetDataBytes, sorted, slotStart);
    }

    private SSTableMeta write(LedgerConfig config, int level, Iterator<Mutation> sorted,
                              Collection<RangeTombstone> rangeTombstones, long targetDataBytes,
                              PeekingIterator<Mutation> splitRun, CandyKey slotStart) {
        boolean bounded = splitRun != null;
        WritableLedger ledger = ledgerStore.createLedger(config);
        try {
            List<byte[]> indexLastKeys = new ArrayList<>();
            List<Long> indexEntryIds = new ArrayList<>();
            BloomFilter.Builder bloom = new BloomFilter.Builder(bloomBitsPerKey);
            long indexBytes = 0;

            List<byte[]> blockMutations = new ArrayList<>();
            int blockBytes = 0;
            byte[] blockLastKey = null;

            byte[] minKey = null;
            byte[] maxKey = null;
            long numEntries = 0;
            long sizeBytes = 0;
            Set<Long> referencedSyrups = new LinkedHashSet<>();
            Hlc minPointHlc = null;

            while (sorted.hasNext()) {
                if (bounded && (sizeBytes + blockBytes >= targetDataBytes
                        || bloom.serializedSizeEstimate() >= SSTableFormat.MAX_META_BLOCK_BYTES
                        || indexBytes >= SSTableFormat.MAX_META_BLOCK_BYTES)) {
                    break;
                }
                Mutation m = sorted.next();
                byte[] keyBytes = m.key().utf8Bytes();
                for (SegmentRef seg : m.locator().segments()) {
                    referencedSyrups.add(seg.syrupId());
                }
                byte[] mb = MutationSerializer.serialize(m);

                if (blockBytes > 0 && blockBytes + mb.length + 5 > dataBlockTargetBytes) {
                    sizeBytes += flushBlock(ledger, blockMutations, blockLastKey, indexLastKeys, indexEntryIds);
                    indexBytes += blockLastKey.length + 10;
                    blockMutations = new ArrayList<>();
                    blockBytes = 0;
                }

                blockMutations.add(mb);
                blockBytes += mb.length + 5;
                blockLastKey = keyBytes;
                bloom.add(keyBytes);

                if (minPointHlc == null || minPointHlc.isAfter(m.hlc())) {
                    minPointHlc = m.hlc();
                }
                if (minKey == null) {
                    minKey = keyBytes;
                }
                maxKey = keyBytes;
                numEntries++;
            }

            if (bounded) {
                CandyKey slotEnd = splitRun.hasNext() ? splitRun.peek().key() : null;
                rangeTombstones = clip(rangeTombstones, slotStart, slotEnd);
            }
            if (numEntries == 0 && rangeTombstones.isEmpty()) {
                throw new IllegalArgumentException("Refusing to write an empty SSTable");
            }
            sizeBytes += flushBlock(ledger, blockMutations, blockLastKey, indexLastKeys, indexEntryIds);

            long bloomEntryId = ledger.append(bloom.build().serialize());

            long indexEntryId = ledger.append(serializeIndex(indexLastKeys, indexEntryIds));

            long rangeDelEntryId = -1;
            if (!rangeTombstones.isEmpty()) {
                byte[] block = serializeRangeTombstones(rangeTombstones);
                if (block.length > SSTableFormat.MAX_RANGE_DEL_BLOCK_BYTES) {
                    throw new IllegalStateException("Range-tombstone block of " + block.length
                            + " bytes exceeds " + SSTableFormat.MAX_RANGE_DEL_BLOCK_BYTES + " bytes ("
                            + rangeTombstones.size() + " range tombstones)");
                }
                rangeDelEntryId = ledger.append(block);
            }

            // A range-only table still needs concrete min/max keys for the manifest; they bound no point
            // data (the read path consults range tombstones across all tables, never via these keys).
            if (numEntries == 0) {
                byte[] rep = representativeKey(rangeTombstones);
                minKey = rep;
                maxKey = rep;
            }

            int numDataBlocks = indexEntryIds.size();
            ledger.append(serializeFooter(bloomEntryId, indexEntryId, rangeDelEntryId, numDataBlocks,
                    numEntries, minKey, maxKey));
            ledger.close();

            return new SSTableMeta(ledger.ledgerId(), level, CandyKey.ofUtf8(minKey),
                    CandyKey.ofUtf8(maxKey), numEntries, sizeBytes, referencedSyrups,
                    spanOf(rangeTombstones), minPointHlc);
        } catch (RuntimeException e) {
            try {
                ledger.close();
                ledgerStore.deleteLedger(ledger.ledgerId());
            } catch (RuntimeException cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
    }

    /** The parts of {@code tombstones} inside {@code [slotStart, slotEnd)} (null bounds = unbounded). */
    private static List<RangeTombstone> clip(Collection<RangeTombstone> tombstones, CandyKey slotStart,
                                             CandyKey slotEnd) {
        List<RangeTombstone> out = new ArrayList<>();
        for (RangeTombstone rt : tombstones) {
            CandyKey start = max(rt.startInclusive(), slotStart);
            CandyKey end = rt.endExclusive() == null ? slotEnd
                    : slotEnd == null || rt.endExclusive().compareTo(slotEnd) < 0 ? rt.endExclusive()
                    : slotEnd;
            if (start == null || end == null || start.compareTo(end) < 0) {
                out.add(new RangeTombstone(start, end, rt.hlc()));
            }
        }
        return out;
    }

    private static CandyKey max(CandyKey a, CandyKey b) {
        if (a == null) {
            return b;
        }
        return b == null || a.compareTo(b) >= 0 ? a : b;
    }

    private static KeySpan spanOf(Collection<RangeTombstone> tombstones) {
        KeySpan span = null;
        for (RangeTombstone rt : tombstones) {
            span = span == null ? KeySpan.of(rt) : span.union(KeySpan.of(rt));
        }
        return span;
    }

    private static byte[] serializeRangeTombstones(Collection<RangeTombstone> tombstones) {
        BinaryWriter w = new BinaryWriter(64);
        w.writeVarInt(tombstones.size());
        for (RangeTombstone rt : tombstones) {
            w.writeBytes(RangeTombstoneSerializer.serialize(rt));
        }
        return w.toByteArray();
    }

    /** A concrete key for a range-only table's metadata: any bound present, else a 0x00 sentinel. */
    private static byte[] representativeKey(Collection<RangeTombstone> tombstones) {
        for (RangeTombstone rt : tombstones) {
            if (rt.startInclusive() != null) {
                return rt.startInclusive().utf8Bytes();
            }
            if (rt.endExclusive() != null) {
                return rt.endExclusive().utf8Bytes();
            }
        }
        return new byte[] {0};
    }

    /** Appends the block as one ledger entry; returns the entry's byte length (0 if empty). */
    private static int flushBlock(WritableLedger ledger, List<byte[]> blockMutations,
                                  byte[] blockLastKey, List<byte[]> indexLastKeys,
                                  List<Long> indexEntryIds) {
        if (blockMutations.isEmpty()) {
            return 0;
        }
        BinaryWriter w = new BinaryWriter(256);
        w.writeVarInt(blockMutations.size());
        for (byte[] mb : blockMutations) {
            w.writeBytes(mb);
        }
        byte[] block = w.toByteArray();
        long entryId = ledger.append(block);
        indexLastKeys.add(blockLastKey);
        indexEntryIds.add(entryId);
        return block.length;
    }

    private static byte[] serializeIndex(List<byte[]> lastKeys, List<Long> entryIds) {
        BinaryWriter w = new BinaryWriter(64);
        w.writeVarInt(lastKeys.size());
        for (int i = 0; i < lastKeys.size(); i++) {
            w.writeBytes(lastKeys.get(i));
            w.writeVarLong(entryIds.get(i));
        }
        return w.toByteArray();
    }

    private static byte[] serializeFooter(long bloomEntryId, long indexEntryId, long rangeDelEntryId,
                                          int numDataBlocks, long numEntries, byte[] minKey,
                                          byte[] maxKey) {
        BinaryWriter w = new BinaryWriter(64)
                .writeInt(SSTableFormat.FOOTER_MAGIC)
                .writeByte(SSTableFormat.FORMAT_VERSION)
                .writeVarLong(bloomEntryId)
                .writeVarLong(indexEntryId)
                .writeVarInt(numDataBlocks)
                .writeVarLong(numEntries)
                .writeBytes(minKey)
                .writeBytes(maxKey);
        // v2 trailer: presence flag + range-del block entry id.
        if (rangeDelEntryId >= 0) {
            w.writeBoolean(true);
            w.writeVarLong(rangeDelEntryId);
        } else {
            w.writeBoolean(false);
        }
        return w.toByteArray();
    }
}
