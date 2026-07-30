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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.Opcode;
import me.predatorray.candybox.protocol.ProtocolException;

/**
 * The non-blocking inbound half of {@link FrameCodec}'s wire format:
 * {@code magic(2) | version(1) | opcode(1) | length(4) | payload}, all big-endian.
 *
 * <p>It validates the magic, version and length prefix from the header <em>in place</em>, before
 * allocating the payload array, so a hostile or corrupt length can never drive a node to OOM —
 * the same guarantee the blocking {@link FrameCodec#read} makes, with the same messages. Where the
 * blocking codec parks a thread mid-frame, this leaves the partial frame in the channel's
 * accumulation buffer and returns; a rejected header throws, and the transport closes the
 * connection (the stream cannot be resynchronised without a request id on the wire).
 *
 * <p>One read may yield several frames: the decoder is invoked until the buffer no longer holds a
 * complete one, which is what lets a pipelining client keep more than one request in flight.
 */
final class FrameDecoder extends ByteToMessageDecoder {

    private final int maxFrameBytes;

    FrameDecoder(int maxFrameBytes) {
        this.maxFrameBytes = maxFrameBytes;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < FrameCodec.HEADER_BYTES) {
            return; // not even a header yet
        }
        int header = in.readerIndex();
        int magic = in.getUnsignedShort(header);
        if (magic != FrameCodec.MAGIC) {
            throw new ProtocolException("Bad frame magic: 0x" + Integer.toHexString(magic));
        }
        int version = in.getUnsignedByte(header + 2);
        if (version != FrameCodec.VERSION) {
            throw new ProtocolException("Unsupported protocol version: " + version);
        }
        int length = in.getInt(header + 4);
        if (length < 0 || length > maxFrameBytes) {
            // Reject before allocating: a bad length must never trigger a huge allocation.
            throw new ProtocolException("Illegal frame length " + length + " (max " + maxFrameBytes + ")");
        }
        if (in.readableBytes() < FrameCodec.HEADER_BYTES + length) {
            return; // header is good; wait for the payload
        }
        int opcode = in.getUnsignedByte(header + 3);
        in.skipBytes(FrameCodec.HEADER_BYTES);
        byte[] payload = new byte[length];
        in.readBytes(payload);
        out.add(new Frame(Opcode.fromCode(opcode), payload));
    }
}
