package me.predatorray.candybox.lsm.sstable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.common.CandyKey;
import me.predatorray.candybox.common.CandyLocator;
import me.predatorray.candybox.common.Mutation;
import me.predatorray.candybox.common.bloom.BloomFilter;
import me.predatorray.candybox.common.exception.SerializationException;
import me.predatorray.candybox.common.serial.BinaryReader;
import me.predatorray.candybox.common.serial.MutationSerializer;
import me.predatorray.candybox.common.util.Bytes;

/**
 * Reads an SSTable ledger written by {@link SSTableWriter}. The footer, index, and bloom filter are
 * loaded eagerly; data blocks are read on demand. Point lookups consult the bloom filter first; range
 * scans iterate blocks from the one containing the start key.
 *
 * <p>Holds an open read handle on the ledger for its lifetime; {@link #close()} releases it.
 */
public final class SSTableReader implements AutoCloseable {

    private final ReadableLedger ledger;
    private final BloomFilter bloom;
    private final byte[][] blockLastKeys;
    private final long[] blockEntryIds;
    private final CandyKey minKey;
    private final CandyKey maxKey;
    private final long entryCount;

    public SSTableReader(LedgerStore store, long ledgerId) {
        this.ledger = store.openLedger(ledgerId);
        long lac = ledger.lastAddConfirmed();
        if (lac < 0) {
            throw new SerializationException("SSTable ledger " + ledgerId + " is empty");
        }
        Footer footer = parseFooter(ledger.read(lac).data(), ledgerId);
        this.minKey = CandyKey.ofUtf8(footer.minKey);
        this.maxKey = CandyKey.ofUtf8(footer.maxKey);
        this.entryCount = footer.numEntries;
        this.bloom = BloomFilter.deserialize(ledger.read(footer.bloomEntryId).data());

        BinaryReader idx = new BinaryReader(ledger.read(footer.indexEntryId).data());
        int blocks = idx.readVarInt();
        this.blockLastKeys = new byte[blocks][];
        this.blockEntryIds = new long[blocks];
        for (int i = 0; i < blocks; i++) {
            blockLastKeys[i] = idx.readBytes();
            blockEntryIds[i] = idx.readVarLong();
        }
    }

    public CandyKey minKey() {
        return minKey;
    }

    public CandyKey maxKey() {
        return maxKey;
    }

    public long entryCount() {
        return entryCount;
    }

    /**
     * Point lookup. Returns the stored locator (possibly a tombstone) for {@code key}, or empty if the
     * table does not contain it.
     */
    public Optional<CandyLocator> get(CandyKey key) {
        if (!bloom.mightContain(key.utf8Bytes())) {
            return Optional.empty();
        }
        int block = findBlock(key.utf8Bytes());
        if (block < 0) {
            return Optional.empty();
        }
        for (Mutation m : readBlock(block)) {
            int cmp = m.key().compareTo(key);
            if (cmp == 0) {
                return Optional.of(m.locator());
            }
            if (cmp > 0) {
                break; // block is sorted; we've passed where the key would be
            }
        }
        return Optional.empty();
    }

    /**
     * Returns an iterator over mutations with key {@code >= start} (or all if {@code start} is null),
     * in ascending key order.
     */
    public Iterator<Mutation> scan(CandyKey start) {
        int startBlock = start == null ? 0 : findBlock(start.utf8Bytes());
        if (startBlock < 0) {
            return List.<Mutation>of().iterator();
        }
        return new ScanIterator(startBlock, start);
    }

    /**
     * Returns an iterator over mutations with key {@code <= start} (or all if {@code start} is null),
     * in descending key order.
     */
    public Iterator<Mutation> scanReverse(CandyKey start) {
        int startBlock = start == null ? blockLastKeys.length - 1 : findBlock(start.utf8Bytes());
        if (startBlock < 0) {
            // start is beyond the table's last key: begin at the final block.
            startBlock = blockLastKeys.length - 1;
        }
        if (startBlock < 0) {
            return List.<Mutation>of().iterator(); // empty table
        }
        return new ReverseScanIterator(startBlock, start);
    }

    @Override
    public void close() {
        ledger.close();
    }

    /** First block whose lastKey >= key, or -1 if key is beyond the table's last key. */
    private int findBlock(byte[] key) {
        int lo = 0;
        int hi = blockLastKeys.length; // exclusive
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (Bytes.compare(blockLastKeys[mid], key) < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo == blockLastKeys.length ? -1 : lo;
    }

    private List<Mutation> readBlock(int blockIndex) {
        byte[] data = ledger.read(blockEntryIds[blockIndex]).data();
        BinaryReader r = new BinaryReader(data);
        int count = r.readVarInt();
        List<Mutation> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(MutationSerializer.deserialize(r.readBytes()));
        }
        return out;
    }

    private final class ScanIterator implements Iterator<Mutation> {
        private int blockIndex;
        private List<Mutation> block;
        private int posInBlock;
        private final CandyKey start;
        private boolean startSkipped;

        ScanIterator(int startBlock, CandyKey start) {
            this.blockIndex = startBlock;
            this.start = start;
            loadBlock();
        }

        private void loadBlock() {
            block = blockIndex < blockEntryIds.length ? readBlock(blockIndex) : List.of();
            posInBlock = 0;
            if (!startSkipped && start != null) {
                while (posInBlock < block.size() && block.get(posInBlock).key().compareTo(start) < 0) {
                    posInBlock++;
                }
                startSkipped = true;
            }
        }

        @Override
        public boolean hasNext() {
            while (posInBlock >= block.size()) {
                blockIndex++;
                if (blockIndex >= blockEntryIds.length) {
                    return false;
                }
                loadBlock();
            }
            return true;
        }

        @Override
        public Mutation next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return block.get(posInBlock++);
        }
    }

    private final class ReverseScanIterator implements Iterator<Mutation> {
        private int blockIndex;
        private List<Mutation> block;
        private int posInBlock;
        private final CandyKey start;
        private boolean startPositioned;

        ReverseScanIterator(int startBlock, CandyKey start) {
            this.blockIndex = startBlock;
            this.start = start;
            loadBlock();
        }

        private void loadBlock() {
            block = blockIndex >= 0 ? readBlock(blockIndex) : List.of();
            posInBlock = block.size() - 1;
            if (!startPositioned && start != null) {
                while (posInBlock >= 0 && block.get(posInBlock).key().compareTo(start) > 0) {
                    posInBlock--;
                }
                startPositioned = true;
            }
        }

        @Override
        public boolean hasNext() {
            while (posInBlock < 0) {
                blockIndex--;
                if (blockIndex < 0) {
                    return false;
                }
                loadBlock();
            }
            return true;
        }

        @Override
        public Mutation next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return block.get(posInBlock--);
        }
    }

    private static Footer parseFooter(byte[] data, long ledgerId) {
        BinaryReader r = new BinaryReader(data);
        int magic = r.readInt();
        if (magic != SSTableFormat.FOOTER_MAGIC) {
            throw new SerializationException("Bad SSTable footer magic in ledger " + ledgerId);
        }
        int version = r.readByte();
        if (version != SSTableFormat.FORMAT_VERSION) {
            throw new SerializationException("Unsupported SSTable format version " + version);
        }
        Footer f = new Footer();
        f.bloomEntryId = r.readVarLong();
        f.indexEntryId = r.readVarLong();
        f.numDataBlocks = r.readVarInt();
        f.numEntries = r.readVarLong();
        f.minKey = r.readBytes();
        f.maxKey = r.readBytes();
        return f;
    }

    private static final class Footer {
        long bloomEntryId;
        long indexEntryId;
        int numDataBlocks;
        long numEntries;
        byte[] minKey;
        byte[] maxKey;
    }
}
