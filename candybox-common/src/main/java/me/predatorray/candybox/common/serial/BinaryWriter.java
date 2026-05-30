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
package me.predatorray.candybox.common.serial;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * A small, allocation-friendly big-endian binary encoder used by every Candybox record codec.
 *
 * <p>Length prefixes and small counts use unsigned LEB128 varints; scalar fields use fixed width.
 * Not thread-safe (single-writer, transient).
 */
public final class BinaryWriter {

    private final ByteArrayOutputStream out;

    public BinaryWriter() {
        this(64);
    }

    public BinaryWriter(int initialCapacity) {
        this.out = new ByteArrayOutputStream(initialCapacity);
    }

    public BinaryWriter writeByte(int b) {
        out.write(b & 0xFF);
        return this;
    }

    public BinaryWriter writeBoolean(boolean v) {
        return writeByte(v ? 1 : 0);
    }

    public BinaryWriter writeShort(int v) {
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
        return this;
    }

    public BinaryWriter writeInt(int v) {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
        return this;
    }

    public BinaryWriter writeLong(long v) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.write((int) ((v >>> shift) & 0xFF));
        }
        return this;
    }

    /** Writes a non-negative int as an unsigned LEB128 varint. */
    public BinaryWriter writeVarInt(int v) {
        if (v < 0) {
            throw new IllegalArgumentException("varint must be non-negative: " + v);
        }
        int value = v;
        while ((value & ~0x7F) != 0) {
            out.write((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.write(value);
        return this;
    }

    /** Writes a non-negative long as an unsigned LEB128 varint. */
    public BinaryWriter writeVarLong(long v) {
        if (v < 0) {
            throw new IllegalArgumentException("varlong must be non-negative: " + v);
        }
        long value = v;
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
        return this;
    }

    /** Writes a varint length prefix followed by the raw bytes. */
    public BinaryWriter writeBytes(byte[] bytes) {
        writeVarInt(bytes.length);
        out.writeBytes(bytes);
        return this;
    }

    /** Writes a UTF-8 string as a length-prefixed byte run. */
    public BinaryWriter writeString(String s) {
        return writeBytes(s.getBytes(StandardCharsets.UTF_8));
    }

    /** Appends raw bytes with no length prefix (caller manages framing). */
    public BinaryWriter writeRaw(byte[] bytes) {
        out.writeBytes(bytes);
        return this;
    }

    /** The number of bytes written so far. */
    public int size() {
        return out.size();
    }

    public byte[] toByteArray() {
        return out.toByteArray();
    }
}
