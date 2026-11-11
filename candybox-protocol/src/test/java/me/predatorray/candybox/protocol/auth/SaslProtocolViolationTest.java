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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import me.predatorray.candybox.common.auth.AuthenticationProviders;
import me.predatorray.candybox.common.auth.InMemoryCredentialStore;
import me.predatorray.candybox.common.auth.PlainAuthenticationProvider;
import me.predatorray.candybox.common.auth.Principal;
import me.predatorray.candybox.common.exception.AuthenticationException;
import me.predatorray.candybox.protocol.Frame;
import me.predatorray.candybox.protocol.Message;
import me.predatorray.candybox.protocol.MessageCodec;
import me.predatorray.candybox.protocol.Opcode;
import me.predatorray.candybox.protocol.transport.ConnectionContext;
import me.predatorray.candybox.protocol.transport.LoopbackTransport;
import org.junit.jupiter.api.Test;

class SaslProtocolViolationTest {
    private static final MessageCodec CODEC = new MessageCodec();
    private final AuthenticatingRequestHandler gate = new AuthenticatingRequestHandler(
            request -> { throw new AssertionError("Unauthenticated request reached the delegate"); },
            AuthenticationProviders.forMechanisms(List.of("PLAIN")),
            new InMemoryCredentialStore().addUser("alice", "wonderland"), true);

    @Test
    void malformedHandshakeReturnsAuthFailure() {
        ConnectionContext context = new ConnectionContext();
        assertFailed(gate.handle(context, new Frame(Opcode.SASL_HANDSHAKE, new byte[0])),
                "Malformed SASL handshake");
        assertThat(context.isAuthenticated()).isFalse();
        assertThat(context.saslState()).isNull();
    }

    @Test
    void malformedAuthenticatePayloadReturnsAuthFailure() {
        ConnectionContext context = new ConnectionContext();
        handshake(context);
        // Truncated wire payload makes MessageCodec throw before a mechanism sees a token.
        assertFailed(gate.handle(context, new Frame(Opcode.SASL_AUTHENTICATE, new byte[0])),
                "Malformed SASL token");
        assertThat(context.isAuthenticated()).isFalse();
        assertFailed(gate.handle(context, CODEC.encode(new Message.ListBoxesRequest())), "Not authenticated");
    }

    @Test
    void handshakeAfterSuccessIsRejectedAndPreservesPrincipal() {
        ConnectionContext context = new ConnectionContext();
        handshake(context);
        byte[] token = new PlainAuthenticationProvider()
                .newClientAuthenticator("alice", "wonderland").initialResponse();
        Message response = CODEC.decode(gate.handle(context,
                CODEC.encode(new Message.SaslAuthenticateRequest(token))));
        assertThat(response).isInstanceOfSatisfying(Message.SaslAuthenticateResponse.class,
                auth -> assertThat(auth.complete()).isTrue());
        assertThat(context.principal()).isEqualTo(Principal.user("alice"));
        assertFailed(gate.handle(context, CODEC.encode(new Message.SaslHandshakeRequest("PLAIN"))),
                "Connection is already authenticated");
        assertThat(context.principal()).isEqualTo(Principal.user("alice"));
    }

    @Test
    void handshakeAuthFailureIsSurfacedToClient() {
        AtomicInteger calls = new AtomicInteger();
        LoopbackTransport loopback = new LoopbackTransport(request -> {
            assertThat(CODEC.decode(request)).isInstanceOf(Message.SaslHandshakeRequest.class);
            calls.incrementAndGet();
            return CODEC.encode(new Message.AuthFailedResponse("Handshake denied"));
        });
        try (AuthenticatingTransport transport =
                     new AuthenticatingTransport(loopback, "PLAIN", "alice", "wonderland")) {
            assertThatThrownBy(() -> transport.connect("ignored", 0))
                    .isInstanceOf(AuthenticationException.class).hasMessageContaining("Handshake denied");
        }
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void rogueServerCannotCompleteScramBeforeMutualAuthentication() {
        AtomicInteger calls = new AtomicInteger();
        LoopbackTransport loopback = new LoopbackTransport(request -> {
            int call = calls.incrementAndGet();
            if (call == 1) {
                assertThat(CODEC.decode(request)).isEqualTo(new Message.SaslHandshakeRequest("SCRAM-SHA-256"));
                return CODEC.encode(new Message.SaslHandshakeResponse(true, List.of("SCRAM-SHA-256")));
            }
            assertThat(call).isEqualTo(2);
            assertThat(CODEC.decode(request)).isInstanceOf(Message.SaslAuthenticateRequest.class);
            return CODEC.encode(new Message.SaslAuthenticateResponse(true, new byte[0]));
        });
        try (AuthenticatingTransport transport =
                     new AuthenticatingTransport(loopback, "SCRAM-SHA-256", "alice", "wonderland")) {
            // A failed connect must never hand an authenticated connection to the caller.
            assertThatThrownBy(() -> transport.connect("ignored", 0))
                    .isInstanceOf(AuthenticationException.class)
                    .hasMessageContaining("Server completed the SASL exchange before the client did");
        }
        assertThat(calls.get()).isEqualTo(2);
    }

    private void handshake(ConnectionContext context) {
        assertThat(CODEC.decode(gate.handle(context,
                CODEC.encode(new Message.SaslHandshakeRequest("PLAIN")))))
                .isEqualTo(new Message.SaslHandshakeResponse(true, List.of("PLAIN")));
    }

    private static void assertFailed(Frame response, String message) {
        assertThat(CODEC.decode(response)).isInstanceOfSatisfying(Message.AuthFailedResponse.class,
                failed -> assertThat(failed.message()).contains(message));
    }
}
