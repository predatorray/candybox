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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-connection backpressure for {@link TcpTransportServer}. Reads are switched off once too many
 * requests are queued for the handler, or while the peer is not draining the responses already
 * written, and back on as those clear. Without it an event loop would happily decode a pipelining
 * (or hostile) client's whole backlog into the handler group's queue, and keep buffering responses
 * for a peer that never reads them.
 *
 * <p>Every flip is evaluated <em>on the event loop</em>, against the count as it stands at that
 * moment, rather than on whichever thread changed the count. That ordering matters: a "stop
 * reading" decided on an event loop could otherwise land after the last in-flight request
 * completed on a handler thread, leaving an idle connection with its reads switched off for good.
 */
final class ReadGate extends ChannelInboundHandlerAdapter {

    private final int maxInFlight;
    private final AtomicInteger inFlight = new AtomicInteger();
    private volatile Channel channel;

    ReadGate(int maxInFlight) {
        this.maxInFlight = maxInFlight;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.channel = ctx.channel();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        inFlight.incrementAndGet();
        reevaluate();
        ctx.fireChannelRead(msg);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        reevaluate();
        ctx.fireChannelWritabilityChanged();
    }

    /** Called from the handler executor once a request has been handled. */
    void completed() {
        inFlight.decrementAndGet();
        reevaluate();
    }

    /** Requests decoded but not yet handled. */
    int inFlight() {
        return inFlight.get();
    }

    private void reevaluate() {
        Channel ch = channel;
        if (ch == null) {
            return;
        }
        if (ch.eventLoop().inEventLoop()) {
            ch.config().setAutoRead(shouldRead(ch));
            return;
        }
        try {
            ch.eventLoop().execute(() -> ch.config().setAutoRead(shouldRead(ch)));
        } catch (RejectedExecutionException shuttingDown) {
            // The event loop is gone, so this connection is finished; nothing left to gate.
        }
    }

    private boolean shouldRead(Channel ch) {
        return inFlight.get() < maxInFlight && ch.isWritable();
    }
}
