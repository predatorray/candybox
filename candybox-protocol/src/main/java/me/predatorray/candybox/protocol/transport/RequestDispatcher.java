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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.CodecException;
import java.io.IOException;
import javax.net.ssl.SSLException;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.ProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs one connection's requests through the {@link RequestHandler}, on the handler executor that
 * {@link TcpTransportServer} pinned to this channel. One instance per connection: it owns that
 * connection's {@link ConnectionContext}, which is what makes the SASL exchange — and the principal
 * it authenticates — per-connection state rather than per-request.
 */
final class RequestDispatcher extends SimpleChannelInboundHandler<Frame> {

    private static final Logger LOG = LoggerFactory.getLogger(RequestDispatcher.class);

    private final RequestHandler handler;
    private final ReadGate gate;
    private final ConnectionContext context = new ConnectionContext();

    RequestDispatcher(RequestHandler handler, ReadGate gate) {
        this.handler = handler;
        this.gate = gate;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Frame request) {
        Frame response;
        try {
            response = handler.handle(context, request);
        } catch (RuntimeException e) {
            // Nothing on the wire identifies a request, so a handler that fails outright cannot be
            // reported without desynchronising the stream: drop the connection instead. The gate is
            // released first, or a connection could die holding its reads shut.
            LOG.warn("Closing connection {} after a handler failure on {}",
                    ctx.channel().remoteAddress(), request.opcode(), e);
            gate.completed();
            ctx.close();
            return;
        }
        // The request leaves the handler queue here; what is still unflushed is accounted for by
        // the gate's writability check instead.
        gate.completed();
        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable throwable) {
        // The framing handlers wrap what they throw, so classify on the original.
        Throwable cause = throwable instanceof CodecException && throwable.getCause() != null
                ? throwable.getCause()
                : throwable;
        if (cause instanceof ProtocolException || cause instanceof SSLException
                || cause instanceof IOException) {
            // Malformed framing, a TLS error, or the peer vanishing: all end the connection.
            LOG.debug("Connection {} ended: {}", ctx.channel().remoteAddress(), cause.toString());
        } else {
            LOG.warn("Unexpected error on connection {}", ctx.channel().remoteAddress(), cause);
        }
        ctx.close();
    }
}
