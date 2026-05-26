package me.predatorray.candybox.protocol;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The framed binary codec: {@code magic(2) | version(1) | opcode(1) | length(4) | payload}, all
 * big-endian.
 *
 * <p>It enforces a configurable maximum payload length (default 16 MiB) and validates the magic,
 * version, and length prefix <em>before allocating</em> the payload buffer, so a hostile or corrupt
 * frame cannot drive a node to OOM. A length that is negative or exceeds the cap is rejected outright.
 */
public final class FrameCodec {

    public static final int MAGIC = 0xCB0F;
    public static final byte VERSION = 1;
    public static final int HEADER_BYTES = 8;
    public static final int DEFAULT_MAX_FRAME_BYTES = 16 << 20;

    private final int maxFrameBytes;

    public FrameCodec() {
        this(DEFAULT_MAX_FRAME_BYTES);
    }

    public FrameCodec(int maxFrameBytes) {
        if (maxFrameBytes < 0) {
            throw new IllegalArgumentException("maxFrameBytes must be non-negative");
        }
        this.maxFrameBytes = maxFrameBytes;
    }

    public int maxFrameBytes() {
        return maxFrameBytes;
    }

    /** Encodes a frame to a byte array. */
    public byte[] encode(Frame frame) {
        byte[] payload = frame.payload();
        if (payload.length > maxFrameBytes) {
            throw new ProtocolException("Frame payload " + payload.length + " exceeds max "
                    + maxFrameBytes);
        }
        byte[] out = new byte[HEADER_BYTES + payload.length];
        out[0] = (byte) (MAGIC >>> 8);
        out[1] = (byte) MAGIC;
        out[2] = VERSION;
        out[3] = (byte) frame.opcode().code();
        out[4] = (byte) (payload.length >>> 24);
        out[5] = (byte) (payload.length >>> 16);
        out[6] = (byte) (payload.length >>> 8);
        out[7] = (byte) payload.length;
        System.arraycopy(payload, 0, out, HEADER_BYTES, payload.length);
        return out;
    }

    /** Writes an encoded frame directly to a stream. */
    public void write(OutputStream out, Frame frame) throws IOException {
        out.write(encode(frame));
        out.flush();
    }

    /** Decodes a single frame from a byte array. */
    public Frame decode(byte[] bytes) {
        try {
            return read(new DataInputStream(new java.io.ByteArrayInputStream(bytes)));
        } catch (IOException e) {
            throw new ProtocolException("Failed to decode frame", e);
        }
    }

    /**
     * Reads one frame from a stream, validating the header and length cap before allocating.
     *
     * @throws ProtocolException on bad magic/version or an out-of-range length
     * @throws EOFException      if the stream ends mid-frame
     */
    public Frame read(DataInputStream in) throws IOException {
        int magic = in.readUnsignedShort();
        if (magic != MAGIC) {
            throw new ProtocolException("Bad frame magic: 0x" + Integer.toHexString(magic));
        }
        int version = in.readUnsignedByte();
        if (version != VERSION) {
            throw new ProtocolException("Unsupported protocol version: " + version);
        }
        int opcode = in.readUnsignedByte();
        int length = in.readInt();
        if (length < 0 || length > maxFrameBytes) {
            // Reject before allocating: a bad length must never trigger a huge allocation.
            throw new ProtocolException("Illegal frame length " + length + " (max " + maxFrameBytes + ")");
        }
        byte[] payload = new byte[length];
        in.readFully(payload);
        return new Frame(Opcode.fromCode(opcode), payload);
    }

    /** Reads one frame from a plain {@link InputStream}. */
    public Frame read(InputStream in) throws IOException {
        return read(in instanceof DataInputStream dis ? dis : new DataInputStream(in));
    }
}
