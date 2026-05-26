package me.predatorray.candybox.protocol;

/**
 * The wire unit: an {@link Opcode} plus an opaque payload. Higher-level {@link Message}s serialize
 * themselves into a frame's payload; {@link FrameCodec} handles the on-wire framing.
 *
 * @param opcode  the opcode
 * @param payload the payload bytes (treat as read-only)
 */
public record Frame(Opcode opcode, byte[] payload) {

    public Frame {
        if (opcode == null) {
            throw new IllegalArgumentException("opcode is required");
        }
        if (payload == null) {
            payload = new byte[0];
        }
    }
}
