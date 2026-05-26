package me.predatorray.candybox.common.serial;

import java.nio.charset.StandardCharsets;
import me.predatorray.candybox.common.exception.SerializationException;

/**
 * The decoding counterpart of {@link BinaryWriter}. Every read is bounds-checked and raises
 * {@link SerializationException} on truncation or a malformed varint, so a corrupt buffer can never
 * over-read or trigger an unbounded allocation. Not thread-safe.
 */
public final class BinaryReader {

    private final byte[] buf;
    private final int limit;
    private int pos;

    public BinaryReader(byte[] buf) {
        this(buf, 0, buf.length);
    }

    public BinaryReader(byte[] buf, int offset, int length) {
        this.buf = buf;
        this.pos = offset;
        this.limit = offset + length;
    }

    private void require(int n) {
        if (n < 0 || pos + n > limit) {
            throw new SerializationException("Buffer underflow: need " + n + " byte(s) at pos " + pos
                    + " with limit " + limit);
        }
    }

    public int readByte() {
        require(1);
        return buf[pos++] & 0xFF;
    }

    public boolean readBoolean() {
        return readByte() != 0;
    }

    public int readShort() {
        require(2);
        return ((buf[pos++] & 0xFF) << 8) | (buf[pos++] & 0xFF);
    }

    public int readInt() {
        require(4);
        return ((buf[pos++] & 0xFF) << 24)
                | ((buf[pos++] & 0xFF) << 16)
                | ((buf[pos++] & 0xFF) << 8)
                | (buf[pos++] & 0xFF);
    }

    public long readLong() {
        require(8);
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (buf[pos++] & 0xFF);
        }
        return v;
    }

    public int readVarInt() {
        int result = 0;
        int shift = 0;
        while (shift < 35) {
            require(1);
            int b = buf[pos++] & 0xFF;
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        throw new SerializationException("Malformed varint (too long)");
    }

    public long readVarLong() {
        long result = 0;
        int shift = 0;
        while (shift < 70) {
            require(1);
            int b = buf[pos++] & 0xFF;
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        throw new SerializationException("Malformed varlong (too long)");
    }

    public byte[] readBytes() {
        int len = readVarInt();
        require(len);
        byte[] out = new byte[len];
        System.arraycopy(buf, pos, out, 0, len);
        pos += len;
        return out;
    }

    public String readString() {
        return new String(readBytes(), StandardCharsets.UTF_8);
    }

    /** Number of unread bytes remaining. */
    public int remaining() {
        return limit - pos;
    }

    public boolean hasRemaining() {
        return pos < limit;
    }
}
