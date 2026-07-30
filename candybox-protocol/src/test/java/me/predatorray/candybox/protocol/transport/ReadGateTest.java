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

import io.netty.buffer.Unpooled;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The per-connection read gate: when it suspends reads, and that it always lets them resume. */
class ReadGateTest {

    private static EmbeddedChannel channelWith(ReadGate gate) {
        EmbeddedChannel channel = new EmbeddedChannel(gate);
        channel.config().setAutoRead(true);
        return channel;
    }

    @Test
    void suspendsReadsOnceTheInFlightLimitIsReached() {
        ReadGate gate = new ReadGate(2);
        EmbeddedChannel channel = channelWith(gate);

        channel.writeInbound("first");
        assertThat(channel.config().isAutoRead()).isTrue();

        channel.writeInbound("second");
        assertThat(channel.config().isAutoRead()).isFalse();
        assertThat(gate.inFlight()).isEqualTo(2);
    }

    @Test
    void resumesReadsAsRequestsAreAnswered() {
        ReadGate gate = new ReadGate(2);
        EmbeddedChannel channel = channelWith(gate);
        channel.writeInbound("first");
        channel.writeInbound("second");
        assertThat(channel.config().isAutoRead()).isFalse();

        gate.completed();

        assertThat(channel.config().isAutoRead()).isTrue();
        assertThat(gate.inFlight()).isEqualTo(1);
    }

    /**
     * A peer that stops draining responses must stop us reading its requests, or the node buffers
     * output on its behalf without bound. Unflushed writes past the high water mark are what make
     * the channel report itself unwritable.
     */
    @Test
    void suspendsReadsWhileThePeerIsNotDrainingResponses() {
        ReadGate gate = new ReadGate(8);
        EmbeddedChannel channel = channelWith(gate);
        channel.config().setWriteBufferWaterMark(new WriteBufferWaterMark(8, 16));

        channel.write(Unpooled.wrappedBuffer(new byte[64]));

        assertThat(channel.isWritable()).isFalse();
        assertThat(channel.config().isAutoRead()).isFalse();

        channel.flush();

        assertThat(channel.isWritable()).isTrue();
        assertThat(channel.config().isAutoRead()).isTrue();
        channel.finishAndReleaseAll();
    }

    /** A gate with nothing outstanding must never be left with reads switched off. */
    @Test
    void leavesReadsOnWhenEveryRequestHasCompleted() {
        ReadGate gate = new ReadGate(1);
        EmbeddedChannel channel = channelWith(gate);

        for (int i = 0; i < 5; i++) {
            channel.writeInbound("request");
            assertThat(channel.config().isAutoRead()).isFalse();
            gate.completed();
            assertThat(channel.config().isAutoRead()).isTrue();
        }
        assertThat(gate.inFlight()).isZero();
    }

    /** Before the gate is wired into a pipeline there is no channel to gate; it must not throw. */
    @Test
    void toleratesCompletionBeforeItHasAChannel() {
        ReadGate gate = new ReadGate(1);

        gate.completed();

        assertThat(gate.inFlight()).isEqualTo(-1);
    }

    /** The response's write listener still fires after the peer is gone; it must not throw. */
    @Test
    void toleratesCompletionAfterTheConnectionIsGone() {
        ReadGate gate = new ReadGate(1);
        EmbeddedChannel channel = channelWith(gate);
        channel.writeInbound("request");
        channel.close().syncUninterruptibly();

        gate.completed();

        assertThat(gate.inFlight()).isZero();
    }

    /**
     * A request completing as the node shuts down: the gate defers its decision to the event loop,
     * which by then refuses new work. Netty answers that with {@link RejectedExecutionException},
     * and a shutdown must not turn into a stack trace on the handler thread.
     */
    @Test
    void toleratesCompletionAfterTheEventLoopIsGone() {
        NioEventLoopGroup group = new NioEventLoopGroup(1);
        ReadGate gate = new ReadGate(1);
        NioSocketChannel channel = new NioSocketChannel();
        channel.pipeline().addLast(gate);
        group.register(channel).syncUninterruptibly();
        channel.close().syncUninterruptibly();
        group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).awaitUninterruptibly(10_000);
        assertThat(group.isTerminated()).isTrue();

        gate.completed(); // must be swallowed, not thrown at the caller

        assertThat(gate.inFlight()).isEqualTo(-1);
    }
}
