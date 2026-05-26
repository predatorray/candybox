package me.predatorray.candybox.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import org.junit.jupiter.api.Test;

class FrameCodecTest {

    private final FrameCodec codec = new FrameCodec();

    @Test
    void encodeDecodeRoundTrip() {
        Frame frame = new Frame(Opcode.PUT_CANDY, "hello".getBytes());
        Frame decoded = codec.decode(codec.encode(frame));
        assertThat(decoded.opcode()).isEqualTo(Opcode.PUT_CANDY);
        assertThat(new String(decoded.payload())).isEqualTo("hello");
    }

    @Test
    void emptyPayloadRoundTrips() {
        Frame decoded = codec.decode(codec.encode(new Frame(Opcode.RESPONSE_OK, new byte[0])));
        assertThat(decoded.opcode()).isEqualTo(Opcode.RESPONSE_OK);
        assertThat(decoded.payload()).isEmpty();
    }

    @Test
    void rejectsOversizedLengthBeforeAllocating() {
        FrameCodec capped = new FrameCodec(16);
        // Hand-craft a header claiming a 2 GiB payload; must be rejected on the length check.
        byte[] header = new byte[FrameCodec.HEADER_BYTES];
        header[0] = (byte) (FrameCodec.MAGIC >>> 8);
        header[1] = (byte) FrameCodec.MAGIC;
        header[2] = FrameCodec.VERSION;
        header[3] = (byte) Opcode.PUT_CANDY.code();
        header[4] = 0x7F;
        header[5] = (byte) 0xFF;
        header[6] = (byte) 0xFF;
        header[7] = (byte) 0xFF;
        assertThatThrownBy(() -> capped.read(new DataInputStream(new ByteArrayInputStream(header))))
                .isInstanceOf(ProtocolException.class)
                .hasMessageContaining("Illegal frame length");
    }

    @Test
    void encodingPayloadOverCapIsRejected() {
        FrameCodec capped = new FrameCodec(8);
        assertThatThrownBy(() -> capped.encode(new Frame(Opcode.PUT_CANDY, new byte[9])))
                .isInstanceOf(ProtocolException.class)
                .hasMessageContaining("exceeds max");
    }

    @Test
    void badMagicIsRejected() {
        byte[] bytes = codec.encode(new Frame(Opcode.RESPONSE_OK, new byte[0]));
        bytes[0] = 0x00; // corrupt magic
        assertThatThrownBy(() -> codec.decode(bytes))
                .isInstanceOf(ProtocolException.class)
                .hasMessageContaining("magic");
    }
}
