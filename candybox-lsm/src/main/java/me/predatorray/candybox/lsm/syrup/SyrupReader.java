package me.predatorray.candybox.lsm.syrup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import me.predatorray.candybox.bookkeeper.LedgerEntry;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.checksum.Crc32c;
import me.predatorray.candybox.common.exception.StorageException;

/**
 * Reassembles a Candy's bytes from its Syrup segments, validating each chunk's crc32c as it goes.
 * The whole-object CRC is checked by the caller (the engine) against the CandyLocator.
 */
public final class SyrupReader {

    private final LedgerStore ledgerStore;

    public SyrupReader(LedgerStore ledgerStore) {
        this.ledgerStore = ledgerStore;
    }

    /**
     * Streams the bytes for {@code segments} to {@code out}.
     *
     * @return the total number of payload bytes written
     * @throws StorageException if a chunk fails crc validation or a read fails
     */
    public long readTo(List<SegmentRef> segments, OutputStream out) {
        long total = 0;
        for (SegmentRef segment : segments) {
            ReadableLedger ledger = ledgerStore.openLedger(segment.syrupId());
            try {
                for (LedgerEntry entry : ledger.readRange(segment.firstEntryId(), segment.lastEntryId())) {
                    byte[] data = entry.data();
                    if (data.length < SyrupManager.CHUNK_HEADER_BYTES) {
                        throw new StorageException("Corrupt Syrup chunk: too short in ledger "
                                + segment.syrupId() + " entry " + entry.entryId());
                    }
                    int storedCrc = ((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16)
                            | ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
                    int payloadLen = data.length - SyrupManager.CHUNK_HEADER_BYTES;
                    int actualCrc = Crc32c.of(data, SyrupManager.CHUNK_HEADER_BYTES, payloadLen);
                    if (storedCrc != actualCrc) {
                        throw new StorageException("Chunk CRC mismatch in Syrup " + segment.syrupId()
                                + " entry " + entry.entryId());
                    }
                    out.write(data, SyrupManager.CHUNK_HEADER_BYTES, payloadLen);
                    total += payloadLen;
                }
            } catch (IOException e) {
                throw new StorageException("Failed writing reassembled Candy bytes", e);
            } finally {
                ledger.close();
            }
        }
        return total;
    }

    /** Convenience: reassemble small Candies fully into a byte array. */
    public byte[] readAll(List<SegmentRef> segments, long expectedLength) {
        int cap = expectedLength > 0 && expectedLength < Integer.MAX_VALUE ? (int) expectedLength : 32;
        ByteArrayOutputStream out = new ByteArrayOutputStream(cap);
        readTo(segments, out);
        return out.toByteArray();
    }
}
