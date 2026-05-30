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
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.RangeTombstone;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.bloom.BloomFilter;
import me.predatorray.candybox.common.serial.BinaryWriter;
import me.predatorray.candybox.common.serial.MutationSerializer;
import me.predatorray.candybox.common.serial.RangeTombstoneSerializer;

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
        WritableLedger ledger = ledgerStore.createLedger(config);

        List<byte[]> indexLastKeys = new ArrayList<>();
        List<Long> indexEntryIds = new ArrayList<>();
        List<byte[]> allKeys = new ArrayList<>();

        List<byte[]> blockMutations = new ArrayList<>();
        int blockBytes = 0;
        byte[] blockLastKey = null;

        byte[] minKey = null;
        byte[] maxKey = null;
        long numEntries = 0;
        long sizeBytes = 0;
        Set<Long> referencedSyrups = new LinkedHashSet<>();

        while (sorted.hasNext()) {
            Mutation m = sorted.next();
            byte[] keyBytes = m.key().utf8Bytes();
            for (SegmentRef seg : m.locator().segments()) {
                referencedSyrups.add(seg.syrupId());
            }
            byte[] mb = MutationSerializer.serialize(m);

            if (blockBytes > 0 && blockBytes + mb.length + 5 > dataBlockTargetBytes) {
                sizeBytes += flushBlock(ledger, blockMutations, blockLastKey, indexLastKeys, indexEntryIds);
                blockMutations = new ArrayList<>();
                blockBytes = 0;
            }

            blockMutations.add(mb);
            blockBytes += mb.length + 5;
            blockLastKey = keyBytes;
            allKeys.add(keyBytes);

            if (minKey == null) {
                minKey = keyBytes;
            }
            maxKey = keyBytes;
            numEntries++;
        }

        if (numEntries == 0 && rangeTombstones.isEmpty()) {
            ledger.close();
            ledgerStore.deleteLedger(ledger.ledgerId());
            throw new IllegalArgumentException("Refusing to write an empty SSTable");
        }
        sizeBytes += flushBlock(ledger, blockMutations, blockLastKey, indexLastKeys, indexEntryIds);

        BloomFilter bloom = BloomFilter.build(allKeys, bloomBitsPerKey);
        long bloomEntryId = ledger.append(bloom.serialize());

        long indexEntryId = ledger.append(serializeIndex(indexLastKeys, indexEntryIds));

        long rangeDelEntryId = -1;
        if (!rangeTombstones.isEmpty()) {
            rangeDelEntryId = ledger.append(serializeRangeTombstones(rangeTombstones));
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
                CandyKey.ofUtf8(maxKey), numEntries, sizeBytes, referencedSyrups);
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
