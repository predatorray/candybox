package me.predatorray.candybox.lsm.sstable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.bloom.BloomFilter;
import me.predatorray.candybox.common.serial.BinaryWriter;
import me.predatorray.candybox.common.serial.MutationSerializer;

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

    /**
     * Writes all mutations from {@code sorted} into a fresh SSTable ledger.
     *
     * @param config quorum/metadata for the SSTable ledger
     * @param level  the LSM level this table belongs to
     * @param sorted ascending-key, unique-key mutations
     * @return metadata for the sealed table
     * @throws IllegalArgumentException if the iterator is empty
     */
    public SSTableMeta write(LedgerConfig config, int level, Iterator<Mutation> sorted) {
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

        if (numEntries == 0) {
            ledger.close();
            ledgerStore.deleteLedger(ledger.ledgerId());
            throw new IllegalArgumentException("Refusing to write an empty SSTable");
        }
        sizeBytes += flushBlock(ledger, blockMutations, blockLastKey, indexLastKeys, indexEntryIds);

        BloomFilter bloom = BloomFilter.build(allKeys, bloomBitsPerKey);
        long bloomEntryId = ledger.append(bloom.serialize());

        long indexEntryId = ledger.append(serializeIndex(indexLastKeys, indexEntryIds));

        int numDataBlocks = indexEntryIds.size();
        ledger.append(serializeFooter(bloomEntryId, indexEntryId, numDataBlocks, numEntries, minKey,
                maxKey));
        ledger.close();

        return new SSTableMeta(ledger.ledgerId(), level, CandyKey.ofUtf8(minKey),
                CandyKey.ofUtf8(maxKey), numEntries, sizeBytes, referencedSyrups);
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

    private static byte[] serializeFooter(long bloomEntryId, long indexEntryId, int numDataBlocks,
                                          long numEntries, byte[] minKey, byte[] maxKey) {
        return new BinaryWriter(64)
                .writeInt(SSTableFormat.FOOTER_MAGIC)
                .writeByte(SSTableFormat.FORMAT_VERSION)
                .writeVarLong(bloomEntryId)
                .writeVarLong(indexEntryId)
                .writeVarInt(numDataBlocks)
                .writeVarLong(numEntries)
                .writeBytes(minKey)
                .writeBytes(maxKey)
                .toByteArray();
    }
}
