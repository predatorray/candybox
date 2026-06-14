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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import me.predatorray.candybox.bookkeeper.LedgerConfig;
import me.predatorray.candybox.bookkeeper.WritableLedger;
import me.predatorray.candybox.bookkeeper.fake.InMemoryLedgerStore;
import me.predatorray.candybox.common.Part;
import me.predatorray.candybox.common.SegmentRef;
import me.predatorray.candybox.common.checksum.Crc32c;
import me.predatorray.candybox.common.config.LedgerRole;
import me.predatorray.candybox.common.exception.StorageException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Failure-mode coverage for {@link SyrupReader}: the integrity checks (chunk too short, per-part length
 * and CRC mismatch), invalid range bounds, and the downstream-write {@link IOException} paths. The
 * happy paths live in {@link SyrupTest}; this class deliberately hand-builds malformed inputs and a
 * failing {@link OutputStream} to drive the error branches.
 */
class SyrupReaderEdgeTest {

    private final InMemoryLedgerStore store = new InMemoryLedgerStore();
    private final SyrupReader reader = new SyrupReader(store);

    @AfterEach
    void tearDown() {
        store.close();
    }

    /** Appends one well-formed chunk ({@code [4-byte CRC32C][payload]}) and returns its segment. */
    private SegmentRef writeChunk(byte[] payload) {
        WritableLedger syrup = store.createLedger(LedgerConfig.forRole(LedgerRole.SYRUP));
        int crc = Crc32c.of(payload);
        byte[] entry = new byte[SyrupManager.CHUNK_HEADER_BYTES + payload.length];
        entry[0] = (byte) (crc >>> 24);
        entry[1] = (byte) (crc >>> 16);
        entry[2] = (byte) (crc >>> 8);
        entry[3] = (byte) crc;
        System.arraycopy(payload, 0, entry, SyrupManager.CHUNK_HEADER_BYTES, payload.length);
        long id = syrup.append(entry);
        syrup.close();
        return new SegmentRef(syrup.ledgerId(), id, id);
    }

    /** An OutputStream that throws on the first write — models a broken client connection. */
    private static OutputStream failingStream() {
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("broken pipe");
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                throw new IOException("broken pipe");
            }
        };
    }

    @Test
    void readRangeRejectsInvalidBounds() {
        List<Part> parts = List.of(new Part(3, 16, Crc32c.of("abc".getBytes()),
                List.of(writeChunk("abc".getBytes()))));
        assertThatThrownBy(() -> reader.readRange(parts, -1, 2, new ByteArrayOutputStream()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> reader.readRange(parts, 5, 3, new ByteArrayOutputStream()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readToWrapsDownstreamIoErrorAsStorageException() {
        List<SegmentRef> segments = List.of(writeChunk("abc".getBytes()));
        assertThatThrownBy(() -> reader.readTo(segments, failingStream()))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("Failed writing");
    }

    @Test
    void readPartsWrapsDownstreamIoErrorAsStorageException() {
        byte[] payload = "abc".getBytes();
        List<Part> parts = List.of(new Part(payload.length, 16, Crc32c.of(payload),
                List.of(writeChunk(payload))));
        assertThatThrownBy(() -> reader.readParts(parts, failingStream()))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("Failed writing");
    }

    @Test
    void readRangeWrapsDownstreamIoErrorAsStorageException() {
        byte[] payload = "abcdef".getBytes();
        List<Part> parts = List.of(new Part(payload.length, 16, Crc32c.of(payload),
                List.of(writeChunk(payload))));
        assertThatThrownBy(() -> reader.readRange(parts, 0, 5, failingStream()))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("Failed writing");
    }

    @Test
    void readPartsDetectsPartLengthMismatch() {
        byte[] payload = "abc".getBytes();
        // Declared length (10) overstates the bytes actually present (3).
        List<Part> parts = List.of(new Part(10, 16, Crc32c.of(payload),
                List.of(writeChunk(payload))));
        assertThatThrownBy(() -> reader.readParts(parts, new ByteArrayOutputStream()))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("Part length mismatch");
    }

    @Test
    void readPartsDetectsPartCrcMismatch() {
        byte[] payload = "abc".getBytes();
        // Length matches, but the declared part CRC does not.
        List<Part> parts = List.of(new Part(payload.length, 16, Crc32c.of(payload) ^ 0x1,
                List.of(writeChunk(payload))));
        assertThatThrownBy(() -> reader.readParts(parts, new ByteArrayOutputStream()))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("Part CRC mismatch");
    }

    @Test
    void readRangeDetectsUnderProducingPart() {
        byte[] payload = "abc".getBytes();
        // Part claims 10 bytes but only 3 exist, so the slice under-produces vs. the requested window.
        List<Part> parts = List.of(new Part(10, 16, Crc32c.of(payload),
                List.of(writeChunk(payload))));
        assertThatThrownBy(() -> reader.readRange(parts, 0, 9, new ByteArrayOutputStream()))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("Range read produced");
    }

    @Test
    void detectsChunkShorterThanHeader() {
        // A chunk whose bytes are fewer than the 4-byte CRC header is structurally corrupt.
        WritableLedger syrup = store.createLedger(LedgerConfig.forRole(LedgerRole.SYRUP));
        long id = syrup.append(new byte[]{1, 2}); // 2 < CHUNK_HEADER_BYTES
        syrup.close();
        List<SegmentRef> segments = List.of(new SegmentRef(syrup.ledgerId(), id, id));
        assertThatThrownBy(() -> reader.readTo(segments, new ByteArrayOutputStream()))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("too short");
    }

    @Test
    void readAllSizesBufferForUnknownLength() {
        // expectedLength <= 0 exercises the default-capacity branch of readAll.
        SegmentRef seg = writeChunk("hello".getBytes());
        assertThat(reader.readAll(List.of(seg), 0)).isEqualTo("hello".getBytes());
    }
}
