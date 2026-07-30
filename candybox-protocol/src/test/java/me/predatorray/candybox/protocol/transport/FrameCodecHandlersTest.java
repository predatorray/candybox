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
package me.predatorray.candybox.protocol.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Arrays;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.Opcode;
import me.predatorray.candybox.protocol.ProtocolException;
import org.junit.jupiter.api.Test;

/**
 * The Netty framing handlers against the blocking {@link FrameCodec} they replace on the wire: the
 * two must agree byte for byte, and the decoder must keep the codec's "validate the header before
 * allocating the payload" guarantee while tolerating frames split across reads.
 */
class FrameCodecHandlersTest {

    private static final FrameCodec CODEC = new FrameCodec();

    private static EmbeddedChannel decoder() {
        return new EmbeddedChannel(new FrameDecoder(CODEC.maxFrameBytes()));
    }

    private static EmbeddedChannel encoder() {
        return new EmbeddedChannel(new FrameEncoder(CODEC.maxFrameBytes()));
    }

    @Test
    void encodesExactlyWhatTheBlockingCodecWouldWrite() {
        Frame frame = new Frame(Opcode.RESPONSE_CANDY_DATA, "candy".getBytes());
        EmbeddedChannel channel = encoder();

        assertThat(channel.writeOutbound(frame)).isTrue();

        ByteBuf written = channel.readOutbound();
        byte[] actual = new byte[written.readableBytes()];
        written.readBytes(actual);
        written.release();
        assertThat(actual).isEqualTo(CODEC.encode(frame));
        assertThat(channel.finish()).isFalse();
    }

    @Test
    void decodesAFrameWrittenByTheBlockingCodec() {
        Frame frame = new Frame(Opcode.PUT_CANDY, "hello".getBytes());
        EmbeddedChannel channel = decoder();

        assertThat(channel.writeInbound(Unpooled.wrappedBuffer(CODEC.encode(frame)))).isTrue();

        Frame decoded = channel.readInbound();
        assertThat(decoded.opcode()).isEqualTo(Opcode.PUT_CANDY);
        assertThat(new String(decoded.payload())).isEqualTo("hello");
        assertThat(channel.finish()).isFalse();
    }

    @Test
    void buffersAFrameSplitAcrossReadsUntilItIsComplete() {
        byte[] encoded = CODEC.encode(new Frame(Opcode.PUT_CANDY, "split me".getBytes()));
        EmbeddedChannel channel = decoder();

        // A partial header, then the rest of the header, then the payload one byte at a time:
        // nothing is emitted until the very last byte lands.
        assertThat(channel.writeInbound(Unpooled.wrappedBuffer(encoded, 0, 3))).isFalse();
        assertThat(channel.writeInbound(Unpooled.wrappedBuffer(encoded, 3,
                FrameCodec.HEADER_BYTES - 3))).isFalse();
        for (int i = FrameCodec.HEADER_BYTES; i < encoded.length - 1; i++) {
            assertThat(channel.writeInbound(Unpooled.wrappedBuffer(encoded, i, 1))).isFalse();
        }
        assertThat(channel.writeInbound(
                Unpooled.wrappedBuffer(encoded, encoded.length - 1, 1))).isTrue();

        Frame decoded = channel.readInbound();
        assertThat(new String(decoded.payload())).isEqualTo("split me");
        assertThat(channel.finish()).isFalse();
    }

    @Test
    void decodesEveryFrameArrivingInOneRead() {
        byte[] first = CODEC.encode(new Frame(Opcode.GET_CANDY, "one".getBytes()));
        byte[] second = CODEC.encode(new Frame(Opcode.GET_CANDY, "two".getBytes()));
        byte[] both = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, both, first.length, second.length);
        EmbeddedChannel channel = decoder();

        assertThat(channel.writeInbound(Unpooled.wrappedBuffer(both))).isTrue();

        assertThat(new String(((Frame) channel.readInbound()).payload())).isEqualTo("one");
        assertThat(new String(((Frame) channel.readInbound()).payload())).isEqualTo("two");
        assertThat((Frame) channel.readInbound()).isNull();
        assertThat(channel.finish()).isFalse();
    }

    @Test
    void rejectsABadMagic() {
        byte[] encoded = CODEC.encode(new Frame(Opcode.GET_CANDY, new byte[0]));
        encoded[0] = 0x00;
        EmbeddedChannel channel = decoder();

        assertThatThrownBy(() -> channel.writeInbound(Unpooled.wrappedBuffer(encoded)))
                .rootCause()
                .isInstanceOf(ProtocolException.class)
                .hasMessageContaining("Bad frame magic");
    }

    @Test
    void rejectsAnUnsupportedVersion() {
        byte[] encoded = CODEC.encode(new Frame(Opcode.GET_CANDY, new byte[0]));
        encoded[2] = 9;
        EmbeddedChannel channel = decoder();

        assertThatThrownBy(() -> channel.writeInbound(Unpooled.wrappedBuffer(encoded)))
                .rootCause()
                .isInstanceOf(ProtocolException.class)
                .hasMessageContaining("Unsupported protocol version: 9");
    }

    /**
     * The reason the length is checked from the header in place: an 8-byte header claiming 2 GiB
     * must be rejected without the decoder ever waiting for — let alone allocating — that payload.
     */
    @Test
    void rejectsAnOversizedLengthOnTheHeaderAlone() {
        EmbeddedChannel channel = decoder();
        ByteBuf header = Unpooled.buffer(FrameCodec.HEADER_BYTES)
                .writeShort(FrameCodec.MAGIC)
                .writeByte(FrameCodec.VERSION)
                .writeByte(Opcode.PUT_CANDY.code())
                .writeInt(Integer.MAX_VALUE);

        assertThatThrownBy(() -> channel.writeInbound(header))
                .rootCause()
                .isInstanceOf(ProtocolException.class)
                .hasMessageContaining("Illegal frame length");
    }

    @Test
    void rejectsANegativeLength() {
        EmbeddedChannel channel = decoder();
        ByteBuf header = Unpooled.buffer(FrameCodec.HEADER_BYTES)
                .writeShort(FrameCodec.MAGIC)
                .writeByte(FrameCodec.VERSION)
                .writeByte(Opcode.PUT_CANDY.code())
                .writeInt(-1);

        assertThatThrownBy(() -> channel.writeInbound(header))
                .rootCause()
                .isInstanceOf(ProtocolException.class)
                .hasMessageContaining("Illegal frame length -1");
    }

    /**
     * The encoder's reason for existing: a response carrying a whole Candy lands in a buffer sized
     * for exactly that frame, so a 16 MiB payload is neither copied into a growing buffer nor
     * copied twice via {@link FrameCodec#encode}'s intermediate array.
     */
    @Test
    void allocatesABufferSizedForExactlyOneFrame() {
        FrameEncoder encoder = new FrameEncoder(CODEC.maxFrameBytes());
        EmbeddedChannel channel = new EmbeddedChannel(encoder);
        ChannelHandlerContext ctx = channel.pipeline().context(encoder);
        Frame frame = new Frame(Opcode.RESPONSE_CANDY_DATA, new byte[4096]);
        int expected = FrameCodec.HEADER_BYTES + 4096;

        ByteBuf direct = encoder.allocateBuffer(ctx, frame, true);
        ByteBuf heap = encoder.allocateBuffer(ctx, frame, false);

        assertThat(direct.capacity()).isEqualTo(expected);
        assertThat(heap.capacity()).isEqualTo(expected);
        assertThat(heap.hasArray()).isTrue();
        direct.release();
        heap.release();
        assertThat(channel.finish()).isFalse();
    }

    @Test
    void refusesToEncodeAFrameOverTheCap() {
        EmbeddedChannel channel = new EmbeddedChannel(new FrameEncoder(16));

        assertThatThrownBy(() -> channel.writeOutbound(
                new Frame(Opcode.RESPONSE_CANDY_DATA, new byte[17])))
                .rootCause()
                .isInstanceOf(ProtocolException.class)
                .hasMessageContaining("exceeds max 16");
    }
}
