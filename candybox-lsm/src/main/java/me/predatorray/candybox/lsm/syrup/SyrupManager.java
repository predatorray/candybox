package me.predatorray.candybox.lsm.syrup;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.checksum.Crc32c;
import me.predatorray.candybox.common.config.CandyboxConfig;
import me.predatorray.candybox.common.exception.StorageException;

/**
 * Writes Candy bytes into Syrups (data ledgers), chunked into fixed-size entries. Each chunk entry is
 * prefixed with its own crc32c for partial validation on streaming reads/retries.
 *
 * <p>One Syrup stays open across Candies and fills with their bytes contiguously; it rolls to a fresh
 * Syrup once it reaches the configured size cap. A Candy that straddles a rollover is described by
 * more than one {@link SegmentRef}. This shared-Syrup layout (rather than one Syrup per Candy) keeps
 * the ledger count down; the space-amplification tradeoff under deletes is documented in DESIGN.md.
 *
 * <p>Not thread-safe on its own; the owning {@code BoxEngine} serializes writes for a Box.
 */
public final class SyrupManager implements AutoCloseable {

    /** Per-chunk entry overhead: a 4-byte big-endian crc32c prefix. */
    static final int CHUNK_HEADER_BYTES = 4;

    private final LedgerStore ledgerStore;
    private final int chunkSize;
    private final long rolloverBytes;
    private final LedgerConfig syrupConfig;

    private WritableLedger currentSyrup;
    private long bytesInCurrentSyrup;

    public SyrupManager(LedgerStore ledgerStore, CandyboxConfig config, LedgerConfig syrupConfig) {
        this.ledgerStore = ledgerStore;
        this.chunkSize = config.sizeLimits().chunkSizeBytes();
        this.rolloverBytes = config.syrupRolloverBytes();
        this.syrupConfig = syrupConfig;
    }

    /**
     * Streams a Candy's bytes into Syrups.
     *
     * @param in the Candy content; read to EOF
     * @return the segments written, total length, and whole-object CRC
     */
    public synchronized SyrupWriteResult writeCandy(InputStream in) {
        Crc32c.Accumulator whole = new Crc32c.Accumulator();
        long total = 0;
        List<SegmentRef> segments = new ArrayList<>();

        long segSyrupId = -1;
        long segFirst = -1;
        long segLast = -1;
        byte[] buf = new byte[chunkSize];

        try {
            int n;
            while ((n = readChunk(in, buf)) > 0) {
                int entryLength = n + CHUNK_HEADER_BYTES;
                rollIfNeeded(entryLength);

                long syrupId = currentSyrup.ledgerId();
                if (syrupId != segSyrupId) {
                    if (segSyrupId != -1) {
                        segments.add(new SegmentRef(segSyrupId, segFirst, segLast));
                    }
                    segSyrupId = syrupId;
                    segFirst = -1;
                }

                int crc = Crc32c.of(buf, 0, n);
                byte[] entry = new byte[entryLength];
                entry[0] = (byte) (crc >>> 24);
                entry[1] = (byte) (crc >>> 16);
                entry[2] = (byte) (crc >>> 8);
                entry[3] = (byte) crc;
                System.arraycopy(buf, 0, entry, CHUNK_HEADER_BYTES, n);

                long entryId = appendOrAbandon(entry);
                if (segFirst == -1) {
                    segFirst = entryId;
                }
                segLast = entryId;
                bytesInCurrentSyrup += entryLength;

                whole.update(buf, 0, n);
                total += n;
            }
        } catch (IOException e) {
            throw new StorageException("Failed reading Candy content stream", e);
        }

        if (segSyrupId != -1) {
            segments.add(new SegmentRef(segSyrupId, segFirst, segLast));
        }
        return new SyrupWriteResult(segments, total, whole.value());
    }

    /**
     * Appends a chunk to the current Syrup. If the append fails — e.g. BookKeeper sealed/fenced the
     * ledger after bookie loss — the current Syrup is abandoned (for writes) so the next write rolls to
     * a fresh ledger instead of being permanently stuck on a dead one. The exception is rethrown so the
     * in-progress put fails; its partial chunks are left as orphans for GC. Already-written Candies in
     * the abandoned Syrup remain readable and referenced.
     */
    private long appendOrAbandon(byte[] entry) {
        try {
            return currentSyrup.append(entry);
        } catch (RuntimeException e) {
            currentSyrup = null;
            bytesInCurrentSyrup = 0;
            throw e;
        }
    }

    private void rollIfNeeded(int entryLength) {
        if (currentSyrup == null) {
            currentSyrup = ledgerStore.createLedger(syrupConfig);
            bytesInCurrentSyrup = 0;
        } else if (bytesInCurrentSyrup > 0 && bytesInCurrentSyrup + entryLength > rolloverBytes) {
            currentSyrup.close();
            currentSyrup = ledgerStore.createLedger(syrupConfig);
            bytesInCurrentSyrup = 0;
        }
    }

    /** Reads up to {@code buf.length} bytes, returning the count (0 only at immediate EOF). */
    private static int readChunk(InputStream in, byte[] buf) throws IOException {
        int off = 0;
        while (off < buf.length) {
            int r = in.read(buf, off, buf.length - off);
            if (r < 0) {
                break;
            }
            off += r;
        }
        return off;
    }

    /** The id of the Syrup currently open for writing, or -1 if none has been created yet. */
    public synchronized long currentSyrupId() {
        return currentSyrup == null ? -1 : currentSyrup.ledgerId();
    }

    @Override
    public synchronized void close() {
        if (currentSyrup != null) {
            currentSyrup.close();
            currentSyrup = null;
        }
    }
}
