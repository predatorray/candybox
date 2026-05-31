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
package me.predatorray.candybox.lsm.syrup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import me.predatorray.candybox.bookkeeper.LedgerEntry;
import me.predatorray.candybox.bookkeeper.LedgerStore;
import me.predatorray.candybox.bookkeeper.ReadableLedger;
import me.predatorray.candybox.common.Part;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.checksum.Crc32c;
import me.predatorray.candybox.common.exception.StorageException;

/**
 * Reassembles a Candy's bytes from its Syrup segments, validating each chunk's crc32c as it goes.
 *
 * <p>Three modes:
 * <ul>
 *   <li>{@link #readTo(List, OutputStream)} — a flat-segment read; the caller validates any
 *       higher-level CRC. Used by the v1-style code path.</li>
 *   <li>{@link #readParts(List, OutputStream)} — a parts-aware read that validates each part's
 *       end-to-end CRC32C as it streams. The normal full-object GET path.</li>
 *   <li>{@link #readRange(List, long, long, OutputStream)} — a Range GET: emits bytes
 *       {@code [firstByte, lastByte]} (inclusive on both ends, S3-style). Per-chunk CRCs still
 *       validate; the part-level CRC cannot be verified on a partial slice and is skipped.</li>
 * </ul>
 */
public final class SyrupReader {

    private final LedgerStore ledgerStore;

    public SyrupReader(LedgerStore ledgerStore) {
        this.ledgerStore = ledgerStore;
    }

    /**
     * Streams the bytes for {@code segments} to {@code out}, validating each chunk's per-chunk CRC.
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
                    int payloadLen = validatedPayloadLength(data, segment.syrupId(), entry.entryId());
                    try {
                        out.write(data, SyrupManager.CHUNK_HEADER_BYTES, payloadLen);
                    } catch (IOException e) {
                        throw new StorageException("Failed writing reassembled Candy bytes", e);
                    }
                    total += payloadLen;
                }
            } finally {
                ledger.close();
            }
        }
        return total;
    }

    /**
     * Streams every part's bytes to {@code out}, validating per-chunk CRC32C continuously and
     * verifying each part's end-to-end CRC32C as the part finishes. A multi-part locator is the sum
     * of its parts in order; a single-part locator reduces to a single end-to-end validation.
     */
    public long readParts(List<Part> parts, OutputStream out) {
        long total = 0;
        for (Part part : parts) {
            Crc32c.Accumulator partCrc = new Crc32c.Accumulator();
            long partWritten = 0;
            for (SegmentRef segment : part.segments()) {
                ReadableLedger ledger = ledgerStore.openLedger(segment.syrupId());
                try {
                    for (LedgerEntry entry : ledger.readRange(segment.firstEntryId(),
                            segment.lastEntryId())) {
                        byte[] data = entry.data();
                        int payloadLen = validatedPayloadLength(data, segment.syrupId(),
                                entry.entryId());
                        try {
                            out.write(data, SyrupManager.CHUNK_HEADER_BYTES, payloadLen);
                        } catch (IOException e) {
                            throw new StorageException("Failed writing reassembled Candy bytes", e);
                        }
                        partCrc.update(data, SyrupManager.CHUNK_HEADER_BYTES, payloadLen);
                        partWritten += payloadLen;
                        total += payloadLen;
                    }
                } finally {
                    ledger.close();
                }
            }
            if (partWritten != part.partLength()) {
                throw new StorageException("Part length mismatch: expected " + part.partLength()
                        + " bytes, read " + partWritten);
            }
            if (partCrc.value() != part.crc32c()) {
                throw new StorageException("Part CRC mismatch (expected " + part.crc32c()
                        + ", got " + partCrc.value() + ")");
            }
        }
        return total;
    }

    /**
     * Streams the byte window {@code [firstByte, lastByte]} (inclusive on both ends, S3-style) of a
     * multi-part Candy to {@code out}. Bounds must satisfy {@code 0 <= firstByte <= lastByte <
     * sum(partLength)}.
     *
     * <p>Chunks at the slice boundary are read whole — per-chunk CRC still validates — and trimmed
     * before emission. Part-level CRC is <em>not</em> verified (the slice doesn't carry enough bytes
     * to recompute it); per-chunk CRCs remain the per-byte integrity floor.
     */
    public long readRange(List<Part> parts, long firstByte, long lastByte, OutputStream out) {
        if (firstByte < 0 || lastByte < firstByte) {
            throw new IllegalArgumentException("Invalid byte range [" + firstByte + ", " + lastByte
                    + "]");
        }
        long want = lastByte - firstByte + 1;
        long emitted = 0;
        long partStart = 0;
        for (Part part : parts) {
            long partEnd = partStart + part.partLength(); // exclusive
            if (partEnd <= firstByte) {
                partStart = partEnd;
                continue;
            }
            if (partStart > lastByte) {
                break;
            }
            // Bytes wanted inside this part, in part-local coordinates:
            long inPartFirst = Math.max(firstByte - partStart, 0);
            long inPartLast = Math.min(lastByte - partStart, part.partLength() - 1);
            emitted += readWithinPart(part, inPartFirst, inPartLast, out);
            partStart = partEnd;
        }
        if (emitted != want) {
            throw new StorageException("Range read produced " + emitted + " bytes; expected " + want);
        }
        return emitted;
    }

    /**
     * Reads bytes {@code [inPartFirst, inPartLast]} (inclusive) of one Part to {@code out}. Within a
     * Part chunks are uniform-sized (modulo the final chunk in the part), so chunk indices are
     * arithmetic; we walk the part's segments and skip / take whole chunks accordingly.
     */
    private long readWithinPart(Part part, long inPartFirst, long inPartLast, OutputStream out) {
        long chunkSize = part.chunkSize();
        long firstChunkIdx = inPartFirst / chunkSize;
        long lastChunkIdx = inPartLast / chunkSize;
        long emitted = 0;
        long chunkIdx = 0; // running global index within this part
        for (SegmentRef segment : part.segments()) {
            long segEntryCount = segment.entryCount();
            long segLastChunkIdx = chunkIdx + segEntryCount - 1;
            if (segLastChunkIdx < firstChunkIdx) {
                chunkIdx += segEntryCount;
                continue;
            }
            if (chunkIdx > lastChunkIdx) {
                break;
            }
            long fromIdx = Math.max(firstChunkIdx, chunkIdx);
            long toIdx = Math.min(lastChunkIdx, segLastChunkIdx);
            long fromEntry = segment.firstEntryId() + (fromIdx - chunkIdx);
            long toEntry = segment.firstEntryId() + (toIdx - chunkIdx);
            ReadableLedger ledger = ledgerStore.openLedger(segment.syrupId());
            try {
                long currentChunkIdx = fromIdx;
                for (LedgerEntry entry : ledger.readRange(fromEntry, toEntry)) {
                    byte[] data = entry.data();
                    int payloadLen = validatedPayloadLength(data, segment.syrupId(), entry.entryId());
                    long chunkPayloadStart = currentChunkIdx * chunkSize;
                    long chunkPayloadEnd = chunkPayloadStart + payloadLen; // exclusive
                    long sliceStart = Math.max(inPartFirst, chunkPayloadStart);
                    long sliceEnd = Math.min(inPartLast + 1, chunkPayloadEnd); // exclusive
                    int offWithinChunk = (int) (sliceStart - chunkPayloadStart);
                    int len = (int) (sliceEnd - sliceStart);
                    if (len > 0) {
                        try {
                            out.write(data, SyrupManager.CHUNK_HEADER_BYTES + offWithinChunk, len);
                        } catch (IOException e) {
                            throw new StorageException("Failed writing reassembled Candy bytes", e);
                        }
                        emitted += len;
                    }
                    currentChunkIdx++;
                }
            } finally {
                ledger.close();
            }
            chunkIdx += segEntryCount;
        }
        return emitted;
    }

    /** Validates a chunk's CRC header and returns its payload length. */
    private static int validatedPayloadLength(byte[] data, long syrupId, long entryId) {
        if (data.length < SyrupManager.CHUNK_HEADER_BYTES) {
            throw new StorageException("Corrupt Syrup chunk: too short in ledger " + syrupId
                    + " entry " + entryId);
        }
        int storedCrc = ((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16)
                | ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
        int payloadLen = data.length - SyrupManager.CHUNK_HEADER_BYTES;
        int actualCrc = Crc32c.of(data, SyrupManager.CHUNK_HEADER_BYTES, payloadLen);
        if (storedCrc != actualCrc) {
            throw new StorageException("Chunk CRC mismatch in Syrup " + syrupId + " entry " + entryId);
        }
        return payloadLen;
    }

    /** Convenience: reassemble small Candies fully into a byte array (legacy single-part path). */
    public byte[] readAll(List<SegmentRef> segments, long expectedLength) {
        int cap = expectedLength > 0 && expectedLength < Integer.MAX_VALUE ? (int) expectedLength : 32;
        ByteArrayOutputStream out = new ByteArrayOutputStream(cap);
        readTo(segments, out);
        return out.toByteArray();
    }
}
