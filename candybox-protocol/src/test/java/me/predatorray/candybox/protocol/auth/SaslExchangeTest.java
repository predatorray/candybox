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
package me.predatorray.candybox.protocol.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import me.predatorray.candybox.common.auth.AuthenticationProviders;
import me.predatorray.candybox.common.auth.InMemoryCredentialStore;
import me.predatorray.candybox.common.auth.Principal;
import me.predatorray.candybox.common.exception.AuthenticationException;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.transport.Connection;
import me.predatorray.candybox.protocol.transport.ConnectionContext;
import me.predatorray.candybox.protocol.transport.LoopbackTransport;
import me.predatorray.candybox.protocol.transport.RequestHandler;
import org.junit.jupiter.api.Test;

/**
 * Drives the server-side SASL gate ({@link AuthenticatingRequestHandler}) and the client-side
 * {@link AuthenticatingTransport} against each other over the in-JVM loopback transport, so the
 * whole exchange — handshake, token round trips, the gate on regular opcodes — runs through the
 * real wire encoding for both mechanisms.
 */
class SaslExchangeTest {

    private static final MessageCodec CODEC = new MessageCodec();

    private final InMemoryCredentialStore store = new InMemoryCredentialStore()
            .addUser("alice", "wonderland")
            .addUser("node-1", "node-pw", new Principal(Principal.TYPE_NODE, "1"));

    /** Echoes the authenticated principal back, so tests can see who the delegate saw. */
    private static final class PrincipalEchoHandler implements RequestHandler {
        @Override
        public Frame handle(Frame request) {
            return handle(new ConnectionContext(), request);
        }

        @Override
        public Frame handle(ConnectionContext context, Frame request) {
            return CODEC.encode(new Message.ErrorResponse("principal",
                    context.principalOrAnonymous().toString()));
        }
    }

    private AuthenticatingRequestHandler gate(boolean required, String... mechanisms) {
        return new AuthenticatingRequestHandler(new PrincipalEchoHandler(),
                AuthenticationProviders.forMechanisms(List.of(mechanisms)), store, required);
    }

    private static Message call(Connection connection, Message request) {
        return CODEC.decode(connection.call(CODEC.encode(request)));
    }

    @Test
    void plainAuthenticatesAndUnlocksTheConnection() {
        LoopbackTransport loopback = new LoopbackTransport(gate(true, "PLAIN", "SCRAM-SHA-256"));
        try (AuthenticatingTransport transport =
                     new AuthenticatingTransport(loopback, "PLAIN", "alice", "wonderland");
             Connection connection = transport.connect("ignored", 0)) {
            Message response = call(connection, new Message.ListBoxesRequest());
            assertEquals("User:alice", ((Message.ErrorResponse) response).message());
        }
    }

    @Test
    void scramAuthenticatesAndMapsThePrincipal() {
        LoopbackTransport loopback = new LoopbackTransport(gate(true, "PLAIN", "SCRAM-SHA-256"));
        try (AuthenticatingTransport transport = new AuthenticatingTransport(loopback,
                "SCRAM-SHA-256", "node-1", "node-pw");
             Connection connection = transport.connect("ignored", 0)) {
            Message response = call(connection, new Message.ListBoxesRequest());
            assertEquals("Node:1", ((Message.ErrorResponse) response).message());
        }
    }

    @Test
    void unauthenticatedRequestIsRejectedWhenRequired() {
        LoopbackTransport loopback = new LoopbackTransport(gate(true, "PLAIN"));
        try (Connection connection = loopback.connect("ignored", 0)) {
            Message response = call(connection, new Message.ListBoxesRequest());
            assertInstanceOf(Message.AuthFailedResponse.class, response);
            assertTrue(((Message.AuthFailedResponse) response).message().contains("PLAIN"));
        }
    }

    @Test
    void unauthenticatedRequestPassesAsAnonymousWhenNotRequired() {
        LoopbackTransport loopback = new LoopbackTransport(gate(false, "PLAIN"));
        try (Connection connection = loopback.connect("ignored", 0)) {
            Message response = call(connection, new Message.ListBoxesRequest());
            assertEquals("User:ANONYMOUS", ((Message.ErrorResponse) response).message());
        }
    }

    @Test
    void optionalModeStillAuthenticatesWhenTheClientWantsTo() {
        LoopbackTransport loopback = new LoopbackTransport(gate(false, "PLAIN"));
        try (AuthenticatingTransport transport =
                     new AuthenticatingTransport(loopback, "PLAIN", "alice", "wonderland");
             Connection connection = transport.connect("ignored", 0)) {
            Message response = call(connection, new Message.ListBoxesRequest());
            assertEquals("User:alice", ((Message.ErrorResponse) response).message());
        }
    }

    @Test
    void wrongPasswordFailsTheConnect() {
        LoopbackTransport loopback = new LoopbackTransport(gate(true, "PLAIN"));
        AuthenticatingTransport transport =
                new AuthenticatingTransport(loopback, "PLAIN", "alice", "wrong");
        assertThrows(AuthenticationException.class, () -> transport.connect("ignored", 0));
    }

    @Test
    void wrongScramPasswordFailsTheConnect() {
        LoopbackTransport loopback = new LoopbackTransport(gate(true, "SCRAM-SHA-256"));
        AuthenticatingTransport transport =
                new AuthenticatingTransport(loopback, "SCRAM-SHA-256", "alice", "wrong");
        assertThrows(AuthenticationException.class, () -> transport.connect("ignored", 0));
    }

    @Test
    void disabledMechanismIsRejectedAtHandshakeListingTheEnabledOnes() {
        LoopbackTransport loopback = new LoopbackTransport(gate(true, "SCRAM-SHA-256"));
        AuthenticatingTransport transport =
                new AuthenticatingTransport(loopback, "PLAIN", "alice", "wonderland");
        AuthenticationException e = assertThrows(AuthenticationException.class,
                () -> transport.connect("ignored", 0));
        assertTrue(e.getMessage().contains("SCRAM-SHA-256"));
    }

    @Test
    void authenticateWithoutHandshakeIsRejected() {
        LoopbackTransport loopback = new LoopbackTransport(gate(true, "PLAIN"));
        try (Connection connection = loopback.connect("ignored", 0)) {
            Message response =
                    call(connection, new Message.SaslAuthenticateRequest(new byte[] {1}));
            assertInstanceOf(Message.AuthFailedResponse.class, response);
        }
    }

    @Test
    void failedExchangeNeedsAFreshHandshakeAndCanThenSucceed() {
        LoopbackTransport loopback = new LoopbackTransport(gate(true, "PLAIN"));
        try (Connection connection = loopback.connect("ignored", 0)) {
            call(connection, new Message.SaslHandshakeRequest("PLAIN"));
            Message failed = call(connection, new Message.SaslAuthenticateRequest(
                    "\0alice\0wrong".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            assertInstanceOf(Message.AuthFailedResponse.class, failed);
            // The dead exchange must not accept further tokens without a new handshake ...
            Message withoutHandshake = call(connection, new Message.SaslAuthenticateRequest(
                    "\0alice\0wonderland".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            assertInstanceOf(Message.AuthFailedResponse.class, withoutHandshake);
            // ... but a fresh handshake on the same connection works.
            call(connection, new Message.SaslHandshakeRequest("PLAIN"));
            Message ok = call(connection, new Message.SaslAuthenticateRequest(
                    "\0alice\0wonderland".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            assertInstanceOf(Message.SaslAuthenticateResponse.class, ok);
            assertTrue(((Message.SaslAuthenticateResponse) ok).complete());
        }
    }

    @Test
    void secondAuthenticationOnTheSameConnectionIsRejected() {
        LoopbackTransport loopback = new LoopbackTransport(gate(true, "PLAIN"));
        try (AuthenticatingTransport transport =
                     new AuthenticatingTransport(loopback, "PLAIN", "alice", "wonderland");
             Connection connection = transport.connect("ignored", 0)) {
            Message again = call(connection, new Message.SaslHandshakeRequest("PLAIN"));
            assertInstanceOf(Message.AuthFailedResponse.class, again);
        }
    }
}
