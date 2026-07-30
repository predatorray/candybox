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
import io.netty.handler.codec.MessageToByteEncoder;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.FrameCodec;
import me.predatorray.candybox.protocol.ProtocolException;

/**
 * The non-blocking outbound half of {@link FrameCodec}'s wire format. It writes the header and the
 * payload straight into the channel's buffer, sized exactly, rather than through the intermediate
 * array {@link FrameCodec#encode} returns — a response carrying a whole Candy is copied once
 * instead of twice.
 */
final class FrameEncoder extends MessageToByteEncoder<Frame> {

    private final int maxFrameBytes;

    FrameEncoder(int maxFrameBytes) {
        this.maxFrameBytes = maxFrameBytes;
    }

    @Override
    protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, Frame frame, boolean preferDirect) {
        int size = FrameCodec.HEADER_BYTES + frame.payload().length;
        return preferDirect ? ctx.alloc().ioBuffer(size) : ctx.alloc().heapBuffer(size);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Frame frame, ByteBuf out) {
        byte[] payload = frame.payload();
        if (payload.length > maxFrameBytes) {
            throw new ProtocolException("Frame payload " + payload.length + " exceeds max " + maxFrameBytes);
        }
        out.writeShort(FrameCodec.MAGIC)
                .writeByte(FrameCodec.VERSION)
                .writeByte(frame.opcode().code())
                .writeInt(payload.length)
                .writeBytes(payload);
    }
}
