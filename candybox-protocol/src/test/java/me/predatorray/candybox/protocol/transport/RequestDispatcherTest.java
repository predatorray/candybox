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

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.net.ssl.SSLException;
import me.predatorray.candybox.common.auth.Principal;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Opcode;
import me.predatorray.candybox.protocol.ProtocolException;
import org.junit.jupiter.api.Test;

/**
 * The per-connection dispatcher: that it answers on the happy path, that a handler which fails
 * outright takes only its connection down (and releases the read gate on the way), that every
 * request on a connection sees the same {@link ConnectionContext}, and that a failure arriving
 * through the pipeline always ends the connection.
 */
class RequestDispatcherTest {

    private static final RequestHandler ECHO =
            request -> new Frame(Opcode.RESPONSE_OK, request.payload());

    private static EmbeddedChannel channelWith(RequestHandler handler, ReadGate gate) {
        return new EmbeddedChannel(gate, new RequestDispatcher(handler, gate));
    }

    /**
     * A handler that implements only the context-carrying overload, so a lambda can assert on the
     * context. {@link RequestHandler}'s own functional method is the context-free one, whose use by
     * the transport would be a bug: a connection's SASL state would be dropped.
     */
    @FunctionalInterface
    private interface ContextualHandler extends RequestHandler {
        @Override
        Frame handle(ConnectionContext context, Frame request);

        @Override
        default Frame handle(Frame request) {
            throw new AssertionError("the transport must call the context-carrying overload");
        }
    }

    @Test
    void answersARequestAndReleasesTheGate() {
        ReadGate gate = new ReadGate(4);
        EmbeddedChannel channel = channelWith(ECHO, gate);

        channel.writeInbound(new Frame(Opcode.GET_CANDY, "ping".getBytes()));

        Frame response = channel.readOutbound();
        assertThat(response.opcode()).isEqualTo(Opcode.RESPONSE_OK);
        assertThat(new String(response.payload())).isEqualTo("ping");
        assertThat(gate.inFlight()).isZero();
        assertThat(channel.isOpen()).isTrue();
    }

    /**
     * A handler that throws cannot be answered — the response would be positional, and there is no
     * request id to disown it with — so the connection goes. The gate must be released even so, or
     * the failure would be preceded by a connection wedged with its reads shut.
     */
    @Test
    void dropsTheConnectionWhenTheHandlerThrows() {
        ReadGate gate = new ReadGate(4);
        EmbeddedChannel channel = channelWith(request -> {
            throw new IllegalStateException("handler blew up");
        }, gate);

        channel.writeInbound(new Frame(Opcode.PUT_CANDY, "boom".getBytes()));

        assertThat((Frame) channel.readOutbound()).isNull();
        assertThat(channel.isOpen()).isFalse();
        assertThat(gate.inFlight()).isZero();
    }

    /**
     * The SASL gate keeps its progress and its authenticated principal on the ConnectionContext, so
     * a connection's requests must all be handed the same instance.
     */
    @Test
    void handsEveryRequestOnAConnectionTheSameContext() {
        List<ConnectionContext> seen = new ArrayList<>();
        ContextualHandler capturing = (context, request) -> {
            seen.add(context);
            return new Frame(Opcode.RESPONSE_OK, request.payload());
        };
        EmbeddedChannel channel = channelWith(capturing, new ReadGate(4));

        channel.writeInbound(new Frame(Opcode.GET_CANDY, "one".getBytes()));
        channel.writeInbound(new Frame(Opcode.GET_CANDY, "two".getBytes()));

        assertThat(seen).hasSize(2);
        assertThat(seen.get(0)).isSameAs(seen.get(1));

        // A second connection gets its own, so one caller's identity never leaks into another's.
        channelWith(capturing, new ReadGate(4))
                .writeInbound(new Frame(Opcode.GET_CANDY, "three".getBytes()));
        assertThat(seen.get(2)).isNotSameAs(seen.get(0));
    }

    @Test
    void authenticationStampedOnOneConnectionStaysOnIt() {
        List<ConnectionContext> seen = new ArrayList<>();
        ContextualHandler authenticating = (context, request) -> {
            seen.add(context);
            context.authenticated(Principal.user("alice"));
            return new Frame(Opcode.RESPONSE_OK, request.payload());
        };
        EmbeddedChannel first = channelWith(authenticating, new ReadGate(4));
        EmbeddedChannel second = channelWith(authenticating, new ReadGate(4));

        first.writeInbound(new Frame(Opcode.GET_CANDY, new byte[0]));
        second.writeInbound(new Frame(Opcode.GET_CANDY, new byte[0]));

        assertThat(seen.get(0).principal()).isEqualTo(Principal.user("alice"));
        assertThat(seen.get(1)).isNotSameAs(seen.get(0));
    }

    /** Framing, TLS and peer-disconnect failures are expected traffic, and end the connection. */
    @Test
    void closesTheConnectionOnAnExpectedFailure() {
        for (Throwable cause : List.of(
                new DecoderException(new ProtocolException("Bad frame magic: 0x0")),
                new ProtocolException("Illegal frame length -1"),
                new SSLException("handshake failed"),
                new IOException("connection reset"))) {
            EmbeddedChannel channel = channelWith(ECHO, new ReadGate(4));

            channel.pipeline().fireExceptionCaught(cause);

            assertThat(channel.isOpen()).as("closed after %s", cause).isFalse();
        }
    }

    /** Anything unexpected is logged louder, but still ends the connection rather than lingering. */
    @Test
    void closesTheConnectionOnAnUnexpectedFailure() {
        EmbeddedChannel channel = channelWith(ECHO, new ReadGate(4));

        channel.pipeline().fireExceptionCaught(new IllegalStateException("something else"));

        assertThat(channel.isOpen()).isFalse();
    }

    /** A CodecException with no cause must classify on itself rather than dereferencing null. */
    @Test
    void toleratesACodecExceptionWithoutACause() {
        EmbeddedChannel channel = channelWith(ECHO, new ReadGate(4));

        channel.pipeline().fireExceptionCaught(new DecoderException("no cause"));

        assertThat(channel.isOpen()).isFalse();
    }
}
